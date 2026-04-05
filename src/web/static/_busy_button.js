document.addEventListener('submit', function(e) {
    if (e.defaultPrevented) return;
    var form = e.target;
    if (!form.hasAttribute('data-busy')) return;
    var btn = e.submitter || form.querySelector('button[type="submit"]');
    if (!btn || btn.disabled) return;
    btn.disabled = true;
    var label = btn.getAttribute('data-busy-label') || btn.textContent.trim();
    btn.innerHTML = '<span class="spinner-border spinner-border-sm" role="status" aria-hidden="true"></span> ' + label;
});

window.setAsyncButtonBusy = function(button, busy, busyLabel) {
    if (!button) return;
    if (!button.dataset.defaultLabel) {
        button.dataset.defaultLabel = button.innerHTML;
    }
    if (busy) {
        button.disabled = true;
        button.innerHTML =
            '<span class="spinner-border spinner-border-sm me-1" role="status" aria-hidden="true"></span>' +
            (busyLabel || '\u0412\u044b\u043f\u043e\u043b\u043d\u044f\u0435\u0442\u0441\u044f...');
        return;
    }
    button.disabled = false;
    button.innerHTML = button.dataset.defaultLabel;
};
