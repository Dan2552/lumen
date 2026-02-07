function blockEdit() {
  // find class unblocked-top-bar-edit-block and replace with top-bar-edit-block
  const topBar = document.querySelector('.unblocked-top-bar-edit-block');
  if (topBar) {
    topBar.classList.remove('unblocked-top-bar-edit-block');
    topBar.classList.add('top-bar-edit-block');
  }

  // same for unblocked-edit-block
  const editBlock = document.querySelector('.unblocked-edit-block');
  if (editBlock) {
    editBlock.classList.remove('unblocked-edit-block');
    editBlock.classList.add('edit-block');
  }

  // add edit-block-is-enabled to body
  document.body.classList.add('edit-block-is-enabled');
}

function disableBlockEdit() {
  // find class top-bar-edit-block and replace with unblocked-top-bar-edit-block
  const topBar = document.querySelector('.top-bar-edit-block');
  if (topBar) {
    topBar.classList.remove('top-bar-edit-block');
    topBar.classList.add('unblocked-top-bar-edit-block');
  }

  // same for edit-block
  const editBlock = document.querySelector('.edit-block');
  if (editBlock) {
    editBlock.classList.remove('edit-block');
    editBlock.classList.add('unblocked-edit-block');
  }

  // remove edit-block-is-enabled from body
  document.body.classList.remove('edit-block-is-enabled');
}

function disableOutsideTab(container) {
  console.log('Disabling tabbing outside of the container');
  console.log(container);

  // Common focusable elements
  const focusableSelectors =
    'a[href], area[href], input:not([disabled]), select:not([disabled]), ' +
    'textarea:not([disabled]), button:not([disabled]), iframe, object, embed, ' +
    '[tabindex]:not([tabindex="-1"]), [contenteditable]';

  // Select all focusable elements on the page
  const allFocusable = document.querySelectorAll(focusableSelectors);

  allFocusable.forEach(el => {
    // If the element is NOT inside the container
    if (!container.contains(el)) {
      // Store original tabindex if it exists, so we can restore it later
      if (el.hasAttribute('tabindex')) {
        el.setAttribute('data-original-tabindex', el.getAttribute('tabindex'));
      }
      // Set tabindex to -1 to remove it from the natural tab order
      el.setAttribute('tabindex', '-1');
      // Add a class for easier selection later if needed
      el.classList.add('disabled-tab');
    }
  });
}

// Function to re-enable tabbing on elements that were disabled
function enableOutsideTab() {
  // Select all elements that have been disabled by our function
  const disabledElements = document.querySelectorAll('.disabled-tab');
  disabledElements.forEach(el => {
    // If an element originally had a tabindex, restore it; otherwise, remove the attribute
    if (el.hasAttribute('data-original-tabindex')) {
      el.setAttribute('tabindex', el.getAttribute('data-original-tabindex'));
      el.removeAttribute('data-original-tabindex');
    } else {
      el.removeAttribute('tabindex');
    }
    // Remove the marker class
    el.classList.remove('disabled-tab');
  });
}
