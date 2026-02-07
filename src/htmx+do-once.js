function performOnce(e) {
  const targets = e.target.parentElement.querySelectorAll("[hx-do-once]");
  targets.forEach(el => {
    const code = el.getAttribute("hx-do-once");
    try {
      // Like eval(), but Function is safer in this case
      new Function(code)();
    } catch (err) {
      console.error("Error in hx-do-once:", err);
    }

    // Optionally remove the attribute to make it run-once
    el.removeAttribute("hx-do-once");
  });
}

document.body.addEventListener("htmx:afterSwap", (e) => {
  console.log("htmx:afterSwap", e);
  performOnce(e);
});

document.body.addEventListener("htmx:afterSettle", (e) => {
  console.log("htmx:afterSettle", e);
  performOnce(e);
});
