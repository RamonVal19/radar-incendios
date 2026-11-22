"""Genera datos SINTETICOS (inventados) con las columnas de schemas/esquemas.md v3.

Sirven solo para programar y probar los jobs mientras llegan los datos reales.
No usar para resultados del proyecto.

Uso: python scripts/generar_muestras_sinteticas.py
Salida: data/sample/*.csv

Las tablas son coherentes entre si: clima_diario se calcula desde clima_horario,
y resultado y evaluacion_regla se calculan desde focos, clima_diario y celdas
con las reglas del contrato. Asi cada integrante puede comparar la salida de su
job con el CSV esperado.
"""
import csv
import math
import os
import random
from datetime import date, timedelta

random.seed(2025)
SALIDA = os.path.join(os.path.dirname(__file__), "..", "data", "sample")
os.makedirs(SALIDA, exist_ok=True)

INICIO = date(2025, 1, 25)
DIAS = 20
FECHAS = [INICIO + timedelta(days=i) for i in range(DIAS)]

# celda_id, lat_celda, lon_celda, es_control
CELDAS = [
    ("-35.375_-71.625", -35.375, -71.625, False),
    ("-36.875_-72.375", -36.875, -72.375, False),
    ("-37.125_-72.625", -37.125, -72.625, False),
    ("-38.375_-72.375", -38.375, -72.375, False),
    ("-33.125_-71.125", -33.125, -71.125, True),
]


def guardar(nombre, campos, filas):
    with open(os.path.join(SALIDA, nombre), "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=campos, lineterminator="\n")
        w.writeheader()
        for fila in filas:
            w.writerow({k: ("" if fila.get(k) is None else fila[k]) for k in campos})
    print(f"{nombre}: {len(filas)} filas")


def b(x):
    return "true" if x else "false"


# 1. celdas
celdas = [{"celda_id": c, "lat_celda": la, "lon_celda": lo, "zona": math.floor(la),
           "es_control": b(ctrl)} for c, la, lo, ctrl in CELDAS]
guardar("celdas.csv", ["celda_id", "lat_celda", "lon_celda", "zona", "es_control"], celdas)

# 2. clima_horario (dias con calor, viento y lluvia variables)
horario = []
perfil_dia = {}
for c, la, lo, ctrl in CELDAS:
    for f in FECHAS:
        calor = random.uniform(22, 36)          # temperatura maxima del dia
        seco = random.uniform(15, 45)           # humedad minima del dia
        viento_base = random.uniform(8, 22)
        ventoso = random.random() < 0.3         # algunos dias con viento fuerte
        llueve = random.random() < 0.15
        perfil_dia[(c, f)] = (calor, seco, viento_base, ventoso, llueve)
        for h in range(24):
            ciclo = math.cos((h - 16) / 24 * 2 * math.pi)      # maximo a las 16:00
            temp = calor - 9 * (1 - ciclo) / 2
            hr = seco + 40 * (1 - ciclo) / 2
            viento = viento_base * (0.6 + 0.4 * (ciclo + 1) / 2)
            if ventoso and 13 <= h <= 19:
                viento += random.uniform(12, 20)
            rafaga = viento * random.uniform(1.4, 1.9)
            lluvia = round(random.uniform(0.2, 3.0), 2) if llueve and 3 <= h <= 9 else 0.0
            if lluvia:
                hr = min(100, hr + 30)
            horario.append({"fecha": f.isoformat(), "hora": h, "celda_id": c,
                            "temp": round(temp, 1), "hr": round(min(hr, 100)),
                            "viento": round(viento, 1), "rafaga": round(rafaga, 1),
                            "lluvia_hora": lluvia})
guardar("clima_horario.csv", ["fecha", "hora", "celda_id", "temp", "hr", "viento", "rafaga", "lluvia_hora"], horario)

# 3. clima_diario (agregado desde clima_horario con las reglas del contrato)
diario = {}
for r in horario:
    k = (r["celda_id"], r["fecha"])
    d = diario.setdefault(k, {"fecha": r["fecha"], "celda_id": r["celda_id"], "tmax": -99, "hr_min": 999,
                              "viento_max": 0, "rafaga_max": 0, "lluvia_dia": 0.0,
                              "horas_regla": 0, "horas_regla_rafaga": 0})
    d["tmax"] = max(d["tmax"], r["temp"])
    d["hr_min"] = min(d["hr_min"], r["hr"])
    d["viento_max"] = max(d["viento_max"], r["viento"])
    d["rafaga_max"] = max(d["rafaga_max"], r["rafaga"])
    d["lluvia_dia"] = round(d["lluvia_dia"] + r["lluvia_hora"], 2)
    if r["temp"] > 30 and r["hr"] < 30 and r["viento"] > 30:
        d["horas_regla"] += 1
    if r["temp"] > 30 and r["hr"] < 30 and r["rafaga"] > 30:
        d["horas_regla_rafaga"] += 1
diario_filas = sorted(diario.values(), key=lambda x: (x["celda_id"], x["fecha"]))
guardar("clima_diario.csv", ["fecha", "celda_id", "tmax", "hr_min", "viento_max", "rafaga_max",
                             "lluvia_dia", "horas_regla", "horas_regla_rafaga"], diario_filas)

# 4. focos (mas focos en dias calurosos y secos, ninguno en la celda control)
focos = []
for c, la, lo, ctrl in CELDAS:
    if ctrl:
        continue
    for f in FECHAS:
        d = diario[(c, f.isoformat())]
        riesgo = max(0, d["tmax"] - 26) / 10 + max(0, 35 - d["hr_min"]) / 20
        n = 0 if d["lluvia_dia"] >= 1 else int(random.random() * riesgo * 6)
        for _ in range(n):
            hora_utc = random.choice(["0530", "0612", "1748", "1830"])
            focos.append({"fecha": f.isoformat(), "hora_utc": hora_utc,
                          "lat": round(la + random.uniform(-0.124, 0.124), 5),
                          "lon": round(lo + random.uniform(-0.124, 0.124), 5),
                          "celda_id": c, "frp": round(random.lognormvariate(1.2, 1.0), 2),
                          "confidence": random.choice(["n", "n", "h", "l"]),
                          "daynight": "D" if hora_utc.startswith("1") else "N"})
guardar("focos.csv", ["fecha", "hora_utc", "lat", "lon", "celda_id", "frp", "confidence", "daynight"], focos)

# 5. resultado (una fila por celda y dia)
info = {c["celda_id"]: c for c in celdas}
conteo = {}
for fo in focos:
    k = (fo["celda_id"], fo["fecha"])
    n, frp = conteo.get(k, (0, 0.0))
    conteo[k] = (n + 1, frp + fo["frp"])

resultado = []
for c, *_ in CELDAS:
    previo = None
    secos = 0
    for f in FECHAS:
        d = diario[(c, f.isoformat())]
        n, frp = conteo.get((c, f.isoformat()), (0, 0.0))
        secos = secos + 1 if d["lluvia_dia"] < 1 else 0
        regla_diaria = d["tmax"] > 30 and d["hr_min"] < 30 and d["viento_max"] > 30
        fila = {"fecha": f.isoformat(), "anio": f.year, "temporada": f.year + 1 if f.month >= 7 else f.year,
                "celda_id": c, "lat_celda": info[c]["lat_celda"], "lon_celda": info[c]["lon_celda"],
                "zona": info[c]["zona"], "es_control": info[c]["es_control"],
                "n_focos": n, "frp_total": round(frp, 2),
                "tmax": d["tmax"], "hr_min": d["hr_min"], "viento_max": d["viento_max"],
                "rafaga_max": d["rafaga_max"], "lluvia_dia": d["lluvia_dia"],
                "horas_regla": d["horas_regla"], "horas_regla_rafaga": d["horas_regla_rafaga"],
                "tmax_d1": previo["tmax"] if previo else None,
                "hr_min_d1": previo["hr_min"] if previo else None,
                "rafaga_max_d1": previo["rafaga_max"] if previo else None,
                "dias_sin_lluvia": secos,
                "regla_diaria": b(regla_diaria),
                "regla_simultanea": b(d["horas_regla"] >= 1),
                "regla_rafaga": b(d["horas_regla_rafaga"] >= 1),
                "regla_diaria_d1": b(previo["_regla"]) if previo else None,
                "dia_muchos_focos": b(n >= 3)}
        fila["es_validacion"] = b(fila["temporada"] == 2025)
        resultado.append(fila)
        previo = dict(d, _regla=regla_diaria)
campos_res = ["fecha", "anio", "temporada", "es_validacion", "celda_id", "lat_celda", "lon_celda", "zona",
              "es_control", "n_focos", "frp_total", "tmax", "hr_min", "viento_max", "rafaga_max", "lluvia_dia",
              "horas_regla", "horas_regla_rafaga", "tmax_d1", "hr_min_d1", "rafaga_max_d1", "dias_sin_lluvia",
              "regla_diaria", "regla_simultanea", "regla_rafaga", "regla_diaria_d1", "dia_muchos_focos"]
guardar("resultado.csv", campos_res, resultado)

# 6. evaluacion_regla (por version de la regla y temporada)
evaluacion = []
columnas = {"diaria": "regla_diaria", "simultanea": "regla_simultanea",
            "rafaga": "regla_rafaga", "diaria_d1": "regla_diaria_d1"}
temporadas = sorted({r["temporada"] for r in resultado})
for t in temporadas:
    filas = [r for r in resultado if r["temporada"] == t]
    focos_tot = sum(r["n_focos"] for r in filas)
    muchos = sum(r["dia_muchos_focos"] == "true" for r in filas)
    for version, col in columnas.items():
        alerta = [r for r in filas if r[col] == "true"]
        evaluacion.append({
            "version": version, "temporada": t, "dias_alerta": len(alerta),
            "aciertos": sum(r["dia_muchos_focos"] == "true" for r in alerta),
            "falsas_alarmas": sum(r["n_focos"] == 0 for r in alerta),
            "omisiones": sum(r["dia_muchos_focos"] == "true" and r[col] != "true" for r in filas),
            "focos_en_alerta": sum(r["n_focos"] for r in alerta),
            "focos_totales": focos_tot,
            "tasa_base": round(muchos / len(filas), 4)})
guardar("evaluacion_regla.csv", ["version", "temporada", "dias_alerta", "aciertos", "falsas_alarmas",
                                 "omisiones", "focos_en_alerta", "focos_totales", "tasa_base"], evaluacion)
