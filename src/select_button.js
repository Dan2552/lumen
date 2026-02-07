function adjustSelectWidth() {
  var selects = document.querySelectorAll('select.fixed-auto-width');
  selects.forEach(function(select) {
    // Store all options
    var options = Array.from(select.options);

    // Find the selected option
    var selectedOption = select.options[select.selectedIndex];

    // Remove all options except the selected one
    select.innerHTML = '';
    select.add(selectedOption);

    // Measure the width of the select element with only the selected option
    select.style.width = 'auto'; // Ensure the width is not constrained
    var width = select.offsetWidth;

    // Set the width of the select element explicitly
    select.style.width = width + 'px';

    // Add the options back to the select element
    options.forEach(function(option) {
      if (option !== selectedOption) {
        select.add(option);
      }
    });

    // Set the default selected option
    select.value = selectedOption.value;
  });
}

document.addEventListener('DOMContentLoaded', adjustSelectWidth);
document.body.addEventListener('htmx:load', adjustSelectWidth);
