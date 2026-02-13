#[cfg(not(unix))]
fn main() {
    eprintln!("lumen-shell is only supported on unix targets.");
    std::process::exit(1);
}

#[cfg(unix)]
mod unix_impl {
    use serde::{Deserialize, Serialize};
    use std::ffi::CString;
    use std::fs::{self, File};
    use std::io::{self, BufRead, BufReader, Read, Write};
    use std::os::fd::{AsRawFd, FromRawFd, RawFd};
    use std::os::unix::net::{UnixListener, UnixStream};
    use std::path::{Path, PathBuf};
    use std::process;
    use std::sync::{
        atomic::{AtomicBool, Ordering},
        Arc, Mutex,
    };
    use std::thread;
    use std::time::Duration;

    #[derive(Debug)]
    struct Config {
        socket_path: PathBuf,
        shell_path: String,
        print_socket: bool,
    }

    #[derive(Debug, Deserialize)]
    #[serde(tag = "type", rename_all = "snake_case")]
    enum ShellControlRequest {
        Cd { path: String },
        Exec { command: String },
        Ping,
    }

    #[derive(Debug, Serialize)]
    struct ShellControlResponse {
        ok: bool,
        #[serde(skip_serializing_if = "Option::is_none")]
        message: Option<String>,
    }

    #[derive(Clone)]
    struct PtyWriter {
        file: Arc<Mutex<File>>,
        master_fd: RawFd,
    }

    impl PtyWriter {
        fn new(file: File) -> Self {
            let master_fd = file.as_raw_fd();
            Self {
                file: Arc::new(Mutex::new(file)),
                master_fd,
            }
        }

        fn raw_fd(&self) -> RawFd {
            self.master_fd
        }

        fn clone_reader(&self) -> io::Result<File> {
            self.file
                .lock()
                .map_err(|_| io::Error::new(io::ErrorKind::Other, "pty writer lock poisoned"))?
                .try_clone()
        }

        fn write_all(&self, bytes: &[u8]) -> io::Result<()> {
            self.file
                .lock()
                .map_err(|_| io::Error::new(io::ErrorKind::Other, "pty writer lock poisoned"))?
                .write_all(bytes)
        }

        fn write_line(&self, line: &str) -> io::Result<()> {
            self.write_all(line.as_bytes())?;
            self.write_all(b"\n")
        }
    }

    struct RawTerminal {
        fd: RawFd,
        original: libc::termios,
    }

    impl RawTerminal {
        fn new(fd: RawFd) -> io::Result<Option<Self>> {
            if unsafe { libc::isatty(fd) } != 1 {
                return Ok(None);
            }

            let mut termios = unsafe { std::mem::zeroed::<libc::termios>() };
            if unsafe { libc::tcgetattr(fd, &mut termios) } != 0 {
                return Err(io::Error::last_os_error());
            }
            let original = termios;
            unsafe { libc::cfmakeraw(&mut termios) };
            if unsafe { libc::tcsetattr(fd, libc::TCSANOW, &termios) } != 0 {
                return Err(io::Error::last_os_error());
            }

            Ok(Some(Self { fd, original }))
        }
    }

    impl Drop for RawTerminal {
        fn drop(&mut self) {
            unsafe {
                libc::tcsetattr(self.fd, libc::TCSANOW, &self.original);
            }
        }
    }

    struct SocketGuard {
        path: PathBuf,
    }

    impl Drop for SocketGuard {
        fn drop(&mut self) {
            let _ = fs::remove_file(&self.path);
        }
    }

    fn print_help() {
        println!(
            "Usage: lumen-shell [--socket <path>] [--shell <path>] [--no-print-socket]\n\
             \n\
             Starts an interactive shell that Lumen can control via a unix socket.\n\
             \n\
             Options:\n\
               --socket <path>      Control socket path (default: $TMPDIR/lumen-shell-<pid>.sock)\n\
               --shell <path>       Shell executable (default: $SHELL or /bin/zsh)\n\
               --no-print-socket    Do not print the socket path banner\n\
               -h, --help           Show this help message\n"
        );
    }

    fn parse_args() -> Result<Config, String> {
        let mut args = std::env::args().skip(1);
        let mut socket_path: Option<PathBuf> = None;
        let mut shell_path: Option<String> = None;
        let mut print_socket = true;

        while let Some(arg) = args.next() {
            match arg.as_str() {
                "--socket" => {
                    let value = args
                        .next()
                        .ok_or_else(|| "missing value for --socket".to_string())?;
                    let trimmed = value.trim();
                    if trimmed.is_empty() {
                        return Err("socket path cannot be empty".to_string());
                    }
                    socket_path = Some(PathBuf::from(trimmed));
                }
                "--shell" => {
                    let value = args
                        .next()
                        .ok_or_else(|| "missing value for --shell".to_string())?;
                    let trimmed = value.trim();
                    if trimmed.is_empty() {
                        return Err("shell path cannot be empty".to_string());
                    }
                    shell_path = Some(trimmed.to_string());
                }
                "--no-print-socket" => {
                    print_socket = false;
                }
                "-h" | "--help" => {
                    print_help();
                    process::exit(0);
                }
                _ => {
                    return Err(format!("unknown argument: {arg}"));
                }
            }
        }

        let shell = shell_path
            .or_else(|| std::env::var("SHELL").ok())
            .unwrap_or_else(|| "/bin/zsh".to_string());
        let socket = socket_path.unwrap_or_else(default_socket_path);

        Ok(Config {
            socket_path: socket,
            shell_path: shell,
            print_socket,
        })
    }

    fn default_socket_path() -> PathBuf {
        std::env::temp_dir().join(format!("lumen-shell-{}.sock", process::id()))
    }

    fn shell_escape_single_quotes(value: &str) -> String {
        value.replace('\'', "'\"'\"'")
    }

    fn send_response(mut stream: UnixStream, response: ShellControlResponse) {
        let payload = serde_json::to_string(&response).unwrap_or_else(|error| {
            format!(
                r#"{{"ok":false,"message":"failed to serialize response: {}"}}"#,
                error
            )
        });
        let _ = stream.write_all(payload.as_bytes());
        let _ = stream.write_all(b"\n");
    }

    fn handle_control_client(stream: UnixStream, writer: &PtyWriter) {
        let mut reader = BufReader::new(stream);
        let mut line = String::new();
        if reader.read_line(&mut line).is_err() {
            return;
        }

        let request = serde_json::from_str::<ShellControlRequest>(line.trim());
        let response = match request {
            Ok(ShellControlRequest::Cd { path }) => {
                let trimmed = path.trim();
                if trimmed.is_empty() {
                    ShellControlResponse {
                        ok: false,
                        message: Some("path cannot be empty".to_string()),
                    }
                } else if trimmed.contains('\n') || trimmed.contains('\r') {
                    ShellControlResponse {
                        ok: false,
                        message: Some("path cannot contain newlines".to_string()),
                    }
                } else {
                    let escaped_path = shell_escape_single_quotes(trimmed);
                    let banner = format!("👭 > cd {trimmed}");
                    let escaped_banner = shell_escape_single_quotes(&banner);
                    let command =
                        format!("printf '%s\\n' '{escaped_banner}'; cd -- '{escaped_path}'");
                    match writer.write_line(&command) {
                        Ok(_) => ShellControlResponse {
                            ok: true,
                            message: None,
                        },
                        Err(error) => ShellControlResponse {
                            ok: false,
                            message: Some(format!("failed to send cd command: {error}")),
                        },
                    }
                }
            }
            Ok(ShellControlRequest::Exec { command }) => {
                let trimmed = command.trim();
                if trimmed.is_empty() {
                    ShellControlResponse {
                        ok: false,
                        message: Some("command cannot be empty".to_string()),
                    }
                } else {
                    match writer.write_line(trimmed) {
                        Ok(_) => ShellControlResponse {
                            ok: true,
                            message: None,
                        },
                        Err(error) => ShellControlResponse {
                            ok: false,
                            message: Some(format!("failed to send command: {error}")),
                        },
                    }
                }
            }
            Ok(ShellControlRequest::Ping) => ShellControlResponse {
                ok: true,
                message: None,
            },
            Err(error) => ShellControlResponse {
                ok: false,
                message: Some(format!("invalid request: {error}")),
            },
        };

        send_response(reader.into_inner(), response);
    }

    fn start_control_server(
        socket_path: &Path,
        writer: PtyWriter,
        running: Arc<AtomicBool>,
    ) -> io::Result<(SocketGuard, thread::JoinHandle<()>)> {
        if socket_path.exists() {
            fs::remove_file(socket_path)?;
        }
        let listener = UnixListener::bind(socket_path)?;
        listener.set_nonblocking(true)?;
        let socket_guard = SocketGuard {
            path: socket_path.to_path_buf(),
        };

        let handle = thread::spawn(move || {
            while running.load(Ordering::SeqCst) {
                match listener.accept() {
                    Ok((stream, _)) => handle_control_client(stream, &writer),
                    Err(error) if error.kind() == io::ErrorKind::WouldBlock => {
                        thread::sleep(Duration::from_millis(35));
                    }
                    Err(error) => {
                        eprintln!("lumen-shell: control listener error: {error}");
                        break;
                    }
                }
            }
        });

        Ok((socket_guard, handle))
    }

    fn current_winsize(fd: RawFd) -> Option<libc::winsize> {
        let mut size = libc::winsize {
            ws_row: 0,
            ws_col: 0,
            ws_xpixel: 0,
            ws_ypixel: 0,
        };
        let result = unsafe { libc::ioctl(fd, libc::TIOCGWINSZ, &mut size) };
        if result == 0 {
            Some(size)
        } else {
            None
        }
    }

    fn sync_child_winsize(master_fd: RawFd, stdin_fd: RawFd) {
        if let Some(size) = current_winsize(stdin_fd) {
            unsafe {
                libc::ioctl(master_fd, libc::TIOCSWINSZ, &size);
            }
        }
    }

    fn start_winsize_sync_thread(
        master_fd: RawFd,
        stdin_fd: RawFd,
        running: Arc<AtomicBool>,
    ) -> thread::JoinHandle<()> {
        let winsize_changed = |next: &libc::winsize, previous: &libc::winsize| {
            next.ws_row != previous.ws_row
                || next.ws_col != previous.ws_col
                || next.ws_xpixel != previous.ws_xpixel
                || next.ws_ypixel != previous.ws_ypixel
        };
        thread::spawn(move || {
            let mut previous = current_winsize(stdin_fd);
            while running.load(Ordering::SeqCst) {
                let next = current_winsize(stdin_fd);
                let changed = match (next.as_ref(), previous.as_ref()) {
                    (Some(next_size), Some(previous_size)) => winsize_changed(next_size, previous_size),
                    (Some(_), None) => true,
                    _ => false,
                };
                if changed {
                    if let Some(size) = next {
                        unsafe {
                            libc::ioctl(master_fd, libc::TIOCSWINSZ, &size);
                        }
                        previous = Some(size);
                    }
                }
                thread::sleep(Duration::from_millis(120));
            }
        })
    }

    fn start_output_thread(reader: File, running: Arc<AtomicBool>) -> thread::JoinHandle<()> {
        thread::spawn(move || {
            let mut reader = reader;
            let mut stdout = io::stdout().lock();
            let mut buffer = [0_u8; 8192];
            loop {
                match reader.read(&mut buffer) {
                    Ok(0) => break,
                    Ok(n) => {
                        if stdout.write_all(&buffer[..n]).is_err() {
                            break;
                        }
                        if stdout.flush().is_err() {
                            break;
                        }
                    }
                    Err(error) if error.kind() == io::ErrorKind::Interrupted => continue,
                    Err(_) => break,
                }
            }
            running.store(false, Ordering::SeqCst);
        })
    }

    fn start_input_thread(writer: PtyWriter, running: Arc<AtomicBool>) -> thread::JoinHandle<()> {
        thread::spawn(move || {
            let mut stdin = io::stdin().lock();
            let mut buffer = [0_u8; 8192];
            while running.load(Ordering::SeqCst) {
                match stdin.read(&mut buffer) {
                    Ok(0) => break,
                    Ok(n) => {
                        if writer.write_all(&buffer[..n]).is_err() {
                            break;
                        }
                    }
                    Err(error) if error.kind() == io::ErrorKind::Interrupted => continue,
                    Err(_) => break,
                }
            }
        })
    }

    fn wait_for_child(pid: libc::pid_t) -> io::Result<()> {
        loop {
            let mut status = 0;
            let result = unsafe { libc::waitpid(pid, &mut status, 0) };
            if result == -1 {
                let error = io::Error::last_os_error();
                if error.kind() == io::ErrorKind::Interrupted {
                    continue;
                }
                return Err(error);
            }
            return Ok(());
        }
    }

    fn open_pty() -> io::Result<(RawFd, RawFd)> {
        let mut master_fd = -1;
        let mut slave_fd = -1;

        let mut termios = unsafe { std::mem::zeroed::<libc::termios>() };
        let termios_ptr = if unsafe { libc::tcgetattr(libc::STDIN_FILENO, &mut termios) } == 0 {
            &mut termios as *mut libc::termios
        } else {
            std::ptr::null_mut()
        };

        let mut winsize = libc::winsize {
            ws_row: 0,
            ws_col: 0,
            ws_xpixel: 0,
            ws_ypixel: 0,
        };
        let winsize_ptr = if unsafe { libc::ioctl(libc::STDIN_FILENO, libc::TIOCGWINSZ, &mut winsize) } == 0 {
            &mut winsize as *mut libc::winsize
        } else {
            std::ptr::null_mut()
        };

        let result = unsafe {
            libc::openpty(
                &mut master_fd,
                &mut slave_fd,
                std::ptr::null_mut(),
                termios_ptr,
                winsize_ptr,
            )
        };
        if result != 0 {
            return Err(io::Error::last_os_error());
        }
        Ok((master_fd, slave_fd))
    }

    fn spawn_shell(shell_path: &str) -> io::Result<(File, libc::pid_t)> {
        let (master_fd, slave_fd) = open_pty()?;
        let pid = unsafe { libc::fork() };
        if pid < 0 {
            unsafe {
                libc::close(master_fd);
                libc::close(slave_fd);
            }
            return Err(io::Error::last_os_error());
        }

        if pid == 0 {
            unsafe {
                libc::close(master_fd);
            }
            if unsafe { libc::login_tty(slave_fd) } != 0 {
                unsafe { libc::_exit(1) };
            }

            let shell_cstring = CString::new(shell_path.as_bytes()).unwrap_or_else(|_| {
                CString::new("/bin/sh").expect("fallback shell path should be valid")
            });
            let interactive = CString::new("-i").expect("static arg should be valid");
            let args = [shell_cstring.as_ptr(), interactive.as_ptr(), std::ptr::null()];

            unsafe {
                libc::execvp(shell_cstring.as_ptr(), args.as_ptr());
                libc::_exit(127);
            }
        }

        unsafe {
            libc::close(slave_fd);
        }
        let file = unsafe { File::from_raw_fd(master_fd) };
        Ok((file, pid))
    }

    pub fn run() -> Result<(), String> {
        let config = parse_args()?;
        let running = Arc::new(AtomicBool::new(true));

        let (pty_file, child_pid) = match spawn_shell(&config.shell_path) {
            Ok(value) => value,
            Err(error) => return Err(format!("failed to spawn shell: {error}")),
        };
        let cleanup_child = |message: String| -> Result<(), String> {
            running.store(false, Ordering::SeqCst);
            unsafe {
                libc::kill(child_pid, libc::SIGHUP);
            }
            let _ = wait_for_child(child_pid);
            Err(message)
        };

        let writer = PtyWriter::new(pty_file);
        let pty_reader = match writer.clone_reader() {
            Ok(value) => value,
            Err(error) => return cleanup_child(format!("failed to clone pty reader: {error}")),
        };

        let (socket_guard, control_thread) = match start_control_server(
            &config.socket_path,
            writer.clone(),
            running.clone(),
        ) {
            Ok(value) => value,
            Err(error) => {
                return cleanup_child(format!("failed to start control socket: {error}"));
            }
        };

        let stdin_fd = io::stdin().as_raw_fd();
        let _raw_terminal = match RawTerminal::new(stdin_fd) {
            Ok(value) => value,
            Err(error) => {
                return cleanup_child(format!(
                    "failed to configure terminal raw mode: {error}"
                ))
            }
        };

        sync_child_winsize(writer.raw_fd(), stdin_fd);

        let _output_thread = start_output_thread(pty_reader, running.clone());
        let _input_thread = start_input_thread(writer.clone(), running.clone());
        let _winsize_thread = start_winsize_sync_thread(writer.raw_fd(), stdin_fd, running.clone());
        let _socket_guard = socket_guard;
        let _control_thread = control_thread;

        if config.print_socket {
            eprintln!(
                "Lumen shell control socket: {}",
                config.socket_path.to_string_lossy()
            );
        }

        let wait_result = wait_for_child(child_pid);
        running.store(false, Ordering::SeqCst);
        wait_result.map_err(|error| format!("shell process wait failed: {error}"))
    }
}

#[cfg(unix)]
fn main() {
    if let Err(error) = unix_impl::run() {
        eprintln!("lumen-shell: {error}");
        std::process::exit(1);
    }
}
