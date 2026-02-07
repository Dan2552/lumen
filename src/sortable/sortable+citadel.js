let sortables = [];

// Function to update the order numbers in the hidden input fields
function updateOrderNumbers() {
  const lists = document.querySelectorAll('.sortable-list');
  lists.forEach((list) => {
    const items = list.querySelectorAll('.sortable-item');
    items.forEach((item, index) => {
      const orderNumber = index; // Order starts from 0
      item.dataset.order = orderNumber; // Update the data-order attribute
      const hiddenInput = item.querySelector('.sortable-hidden-input');
      if (hiddenInput) {
        hiddenInput.value = orderNumber; // Update the hidden input value
      }
    });
  });
}

// Function to refresh sortables
function refreshSortables() {
  // Destroy existing sortable instances
  sortables.forEach((sortable) => {
    sortable.destroy();
  });

  // Clear the sortables array
  sortables = [];

  // Reinitialize Sortable.js for all sortable lists
  const lists = document.querySelectorAll('.sortable-list');
  lists.forEach((list) => {
    const sortable = new Sortable(list, {
      handle: '.sortable-handle', // Only allow dragging by the handle
      animation: 150, // Animation speed in ms
      // fallbackOnBody: true,
      forceFallback: true,
      onEnd: function () {
        updateOrderNumbers(); // Update order numbers after sorting
      }
    });

    sortables.push(sortable);
  });

  // Update order numbers after reinitialization
  updateOrderNumbers();
}


document.addEventListener('DOMContentLoaded', function () {
  refreshSortables();

  // event listener to refresh after htmx:after-swap
  document.addEventListener('htmx:afterSwap', function (event) {
    console.log("SORTABLE, htmx after swap", event)
    refreshSortables();
  });
});
