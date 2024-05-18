document.getElementById('fecha-calendario').addEventListener('change', function() {
    var fechaSeleccionada = this.value;
    if (fechaSeleccionada) {
        window.location.href = '/?fecha=' + fechaSeleccionada;
    }
});