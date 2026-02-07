const {invoke} = window.__TAURI__.core;

const COMMAND_PREFIX = "command/";

const patchedSend = async function(params) {
  // Make readonly properties writable
  Object.defineProperty(this, "readyState", {writable: true})
  Object.defineProperty(this, "status", {writable: true})
  Object.defineProperty(this, "statusText", {writable: true})
  Object.defineProperty(this, "response", {writable: true})

  this.response = await invoke(this.command, params);
  this.readyState = XMLHttpRequest.DONE;
  this.status = 200;
  this.statusText = "OK";

  // We only need load event to trigger a XHR response
  this.dispatchEvent(new ProgressEvent("load"));
};

function parseFormParams(params) {
  const result = {};

  for (const fullKey in params) {
    const value = params[fullKey];
    const path = [];

    // Parse the key path like: user[address][city] or tags[]
    const regex = /([^\[\]]+)|\[(.*?)\]/g;
    let match;
    while ((match = regex.exec(fullKey)) !== null) {
      const keyPart = match[1] ?? match[2]; // head or bracket content
      path.push(keyPart);
    }

    let curr = result;
    for (let i = 0; i < path.length; i++) {
      const key = path[i];

      // Final key: assign value
      if (i === path.length - 1) {
        if (key === "") {
          // [] → push to array
          if (!Array.isArray(curr)) curr = [];
          curr.push(value);
        } else if (Array.isArray(curr[key])) {
          curr[key].push(value);
        } else if (key in curr) {
          // Convert existing value to array
          curr[key] = [curr[key], value];
        } else {
          curr[key] = value;
        }
        break;
      }

      // Intermediate key: decide if object or array
      if (key === "") {
        // Unnamed index — treat as array
        if (!Array.isArray(curr)) curr = [];
        if (!curr.length || typeof curr[curr.length - 1] !== "object") {
          curr.push({});
        }
        curr = curr[curr.length - 1];
      } else {
        if (!(key in curr)) {
          // Peek ahead: is next key ""? then this should be an array
          const nextKey = path[i + 1];
          curr[key] = nextKey === "" ? [] : {};
        }
        curr = curr[key];
      }
    }
  }

  return result;
}

document.body.addEventListener('htmx:beforeSend', (event) => {
    const path = event.detail.requestConfig.path;
    if (path.startsWith(COMMAND_PREFIX)) {
        // Extract URL search parameters
        const urlParams = new URLSearchParams(event.detail.requestConfig.parameters);
        const urlParamsObj = Object.fromEntries(urlParams);

        // Extract form data
        const formParamsObj = Object.fromEntries(event.detail.requestConfig.parameters)

        // Merge URL search parameters and form data
        let mergedParams = {...urlParamsObj, ...formParamsObj};

        console.log("htmx+tauri BEFORE TRANSFORM: ", mergedParams);

        mergedParams = parseFormParams(mergedParams);

        const command = path.slice(COMMAND_PREFIX.length);

        console.log("htmx+tauri: ", command, mergedParams);

        event.detail.xhr.command = path.slice(COMMAND_PREFIX.length);
        event.detail.xhr.send = async function() {
            console.log("request sent!");
            patchedSend.call(this, mergedParams);
        };
    }
});

document.addEventListener("htmx:confirm", (event) => {
  if (!event.detail.question) return
  event.preventDefault();

  let promise = window.confirm(event.detail.question);
  promise.then((result) => {
    if (result) {
      event.detail.confirmed = true;
      event.detail.issueRequest(true);
    } else {
      event.detail.confirmed = false;
    }
  });
});
