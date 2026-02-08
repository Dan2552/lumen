use include_dir::{include_dir, Dir};
use std::sync::LazyLock;
use tera::Tera;

static TEMPLATE_DIR: Dir = include_dir!("$CARGO_MANIFEST_DIR/templates");

pub static TERA: LazyLock<Tera> = LazyLock::new(|| {
    let mut tera = Tera::default();

    let glob = "**/*.html";
    for entry in TEMPLATE_DIR.find(glob).unwrap() {
        let path = entry.path().to_string_lossy();
        let contents = entry
            .as_file()
            .expect("Expected file entry")
            .contents_utf8()
            .expect("Invalid UTF-8 in template");
        tera.add_raw_template(&path, contents)
            .expect("Failed to add template");
    }

    tera
});

#[macro_export]
macro_rules! render {
    ($template:expr, $context:expr) => {{
        let template = format!("{}.html", $template);
        let rendered = $crate::templates::TERA
            .render(&template, $context)
            .expect("Error rendering template");
        // dbg!(&rendered);
        rendered
    }};
    ($template:expr) => {{
        let context = ::tera::Context::new();
        let template = format!("{}.html", $template);
        let rendered = $crate::templates::TERA
            .render(&template, &context)
            .expect("Error rendering template");
        // dbg!(&rendered);
        rendered
    }};
}
