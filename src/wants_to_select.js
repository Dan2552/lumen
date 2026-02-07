function applySelectBorderLogic(context) {
  const selects = (context || document).querySelectorAll('.wants-to-select');
  selects.forEach(function(select) {
    // Prevent duplicate listeners
    if (select._borderLogicAttached) return;
    select._borderLogicAttached = true;

    function updateBorder() {
      if (select.selectedIndex > 0) {
        select.style.border = '2px dashed #4285f4';
      } else {
        select.style.border = '';
      }
      updateWantsToActions();
    }
    select.addEventListener('change', updateBorder);
    updateBorder();
  });
  // Also update actions in case selects are dynamically added/removed
  updateWantsToActions();
}

function updateWantsToActions() {
  const selects = document.querySelectorAll('.wants-to-select');
  let count = 0;
  selects.forEach(function(select) {
    if (select.selectedIndex > 0) {
      count += 1;
    }
  });
  const actions = document.querySelectorAll('.wants-to-action');
  actions.forEach(function(action) {
    action.disabled = count === 0;
  });
}

document.addEventListener('DOMContentLoaded', function() {
  applySelectBorderLogic(document);
});
document.body.addEventListener('htmx:load', function(evt) {
  applySelectBorderLogic(evt.target);
});
