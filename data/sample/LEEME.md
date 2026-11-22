# Muestras SINTÉTICAS (datos inventados)

Estos CSV tienen las columnas exactas de `schemas/esquemas.md` (v3), pero los valores son inventados. Sirven solo para programar y probar los jobs mientras llegan los datos reales. No se usan para ningún resultado del proyecto.

- **Qué contienen:** 5 celdas (una de control) y 20 días, del 25-ene al 13-feb de 2025 (temporada 2025).
- **Cómo se generaron:** Con `python scripts/generar_muestras_sinteticas.py`.
- **Son coherentes entre sí:** `clima_diario` sale de `clima_horario`, y `resultado` y `evaluacion_regla` salen de `focos`, `clima_diario` y `celdas` según las reglas del contrato. Así cada uno puede comparar lo que produce su job con el CSV esperado.

| archivo | filas | lo usa |
|---|---|---|
| celdas.csv | 5 | Daniel, Alan |
| clima_horario.csv | 2.400 | Daniel (entrada de prueba para el resumen a diario) |
| clima_diario.csv | 100 | Alan (entrada), Daniel (salida esperada) |
| focos.csv | 83 | Alan (entrada), Matías (índice `focos`) |
| resultado.csv | 100 | Matías (tablas Hive e índice `resultado`), Alan (salida esperada) |
| evaluacion_regla.csv | 4 | Matías (tabla Hive), Alan (salida esperada) |
