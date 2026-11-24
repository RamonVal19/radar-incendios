#!/usr/bin/env python3
"""Descarga el clima de Open-Meteo (API histórica) para las celdas de celdas.csv.

Responsable: Alan Caro (rol B, descarga). Contrato: schemas/esquemas.md, sección 3.

Qué hace:
  - Lee celdas.csv (columnas celda_id, lat_celda, lon_celda, ...).
  - Con --desde y --hasta elige un rango de filas de celdas.csv (sin contar el
    encabezado, desde 0, y --hasta no se incluye, igual que en Python). Así se reparte
    la descarga entre varias redes: 0-92, 92-184 y 184-276.
  - Guarda un archivo por celda, <celda_id>.csv, tal como lo entrega la API (con las
    3 líneas de metadatos), que es lo que lee spark/clima_diario.py.
  - Si el archivo ya existe, lo salta. Así la descarga se puede retomar si se corta.
  - Escribe primero <celda_id>.csv.tmp y lo renombra al terminar, para no dejar
    archivos incompletos.
  - Si Open-Meteo responde 429 (límite superado), espera unos minutos y reintenta.
    Si el límite superado es el diario, se detiene (se retoma al día siguiente).
  - Con --diario baja las variables diarias del plan B.
  - Anota cada descarga en <salida>/descargas_log.csv (segundos, bytes, filas).

Solo usa la biblioteca estándar de Python 3 (no hay que instalar nada).

Ejemplos (desde la carpeta del repo):

  # Prueba: 5 celdas de la muestra, una semana
  python scripts/descargar_clima.py --celdas data/sample/celdas.csv \\
      --inicio 2023-02-01 --fin 2023-02-07 --salida ~/bigdata/clima_prueba

  # Descarga de prueba: una celda, histórico completo
  python scripts/descargar_clima.py --desde 0 --hasta 1 \\
      --inicio 2012-06-30 --fin 2025-06-30 --salida ~/bigdata/clima_prueba_1celda

  # Mi tercio de la descarga completa
  python scripts/descargar_clima.py --desde 0 --hasta 92 \\
      --inicio 2012-06-30 --fin 2025-06-30 --salida ~/bigdata/clima
"""
import argparse
import csv
import datetime as dt
import json
import os
import sys
import time
import urllib.error
import urllib.parse
import urllib.request

URL_API = os.environ.get("OPEN_METEO_URL", "https://archive-api.open-meteo.com/v1/archive")

VARIABLES_HORARIAS = ["temperature_2m", "relative_humidity_2m", "wind_speed_10m",
                      "wind_gusts_10m", "precipitation"]
VARIABLES_DIARIAS = ["temperature_2m_max", "relative_humidity_2m_min", "wind_speed_10m_max",
                     "wind_gusts_10m_max", "precipitation_sum"]

ESPERA_429 = 300        # segundos que se espera tras un 429 por minuto u hora
MAX_REINTENTOS = 5      # para errores de red o 5xx (los 429 no cuentan)
TIMEOUT = 300           # segundos por consulta (una consulta de 13 años es pesada)


def leer_celdas(ruta):
    with open(ruta, newline="", encoding="utf-8") as f:
        filas = list(csv.DictReader(f))
    for col in ("celda_id", "lat_celda", "lon_celda"):
        if filas and col not in filas[0]:
            sys.exit(f"{ruta} no tiene la columna {col}")
    return filas


def armar_url(lat, lon, inicio, fin, diario):
    params = {
        "latitude": lat,
        "longitude": lon,
        "start_date": inicio,
        "end_date": fin,
        "daily" if diario else "hourly": ",".join(VARIABLES_DIARIAS if diario else VARIABLES_HORARIAS),
        "timezone": "America/Santiago",
        "wind_speed_unit": "kmh",
        "format": "csv",
    }
    return URL_API + "?" + urllib.parse.urlencode(params, safe=",/")


def motivo_error(err):
    """Lee el campo 'reason' del JSON de error de Open-Meteo, si viene."""
    try:
        return json.loads(err.read().decode("utf-8")).get("reason", "")
    except Exception:
        return ""


def descargar(url):
    """Devuelve el contenido de la URL. Maneja 429 y reintenta errores pasajeros."""
    fallos = 0
    while True:
        try:
            req = urllib.request.Request(url, headers={"User-Agent": "radar-incendios/1.0"})
            with urllib.request.urlopen(req, timeout=TIMEOUT) as r:
                return r.read()
        except urllib.error.HTTPError as e:
            motivo = motivo_error(e)
            if e.code == 429:
                if "daily" in motivo.lower():
                    raise LimiteDiario(motivo)
                print(f"    429 ({motivo or 'límite superado'}). Espero {ESPERA_429 // 60} min y reintento...",
                      flush=True)
                time.sleep(ESPERA_429)
                continue
            if e.code >= 500 and fallos < MAX_REINTENTOS:
                fallos += 1
                print(f"    Error {e.code} del servidor, reintento {fallos}/{MAX_REINTENTOS} en 60 s", flush=True)
                time.sleep(60)
                continue
            raise RuntimeError(f"HTTP {e.code}: {motivo}") from e
        except (urllib.error.URLError, TimeoutError, ConnectionError) as e:
            if fallos < MAX_REINTENTOS:
                fallos += 1
                print(f"    Error de red ({e}), reintento {fallos}/{MAX_REINTENTOS} en 60 s", flush=True)
                time.sleep(60)
                continue
            raise


class LimiteDiario(Exception):
    pass


def validar(contenido, fin):
    """Revisa que el CSV traiga la tabla completa hasta la fecha final. Devuelve las filas de datos."""
    lineas = contenido.decode("utf-8").strip().splitlines()
    if len(lineas) < 5 or not lineas[0].startswith("latitude,") or not lineas[3].startswith("time,"):
        raise RuntimeError("La respuesta no tiene el formato CSV esperado de Open-Meteo")
    datos = lineas[4:]
    if not datos[-1].startswith(fin):
        raise RuntimeError(f"La respuesta termina en {datos[-1][:16]} y no en {fin}")
    return len(datos)


def anotar_log(carpeta, fila):
    ruta = os.path.join(carpeta, "descargas_log.csv")
    nuevo = not os.path.exists(ruta)
    with open(ruta, "a", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        if nuevo:
            w.writerow(["momento", "celda_id", "inicio", "fin", "tipo", "segundos", "bytes", "filas"])
        w.writerow(fila)


def main():
    p = argparse.ArgumentParser(description="Descarga el clima de Open-Meteo, un archivo por celda.")
    p.add_argument("--celdas", default="data/celdas.csv", help="ruta a celdas.csv (por defecto data/celdas.csv)")
    p.add_argument("--inicio", required=True, help="fecha inicial AAAA-MM-DD")
    p.add_argument("--fin", required=True, help="fecha final AAAA-MM-DD (incluida)")
    p.add_argument("--salida", required=True, help="carpeta de salida (fuera del repositorio)")
    p.add_argument("--desde", type=int, default=0, help="primera fila de celdas.csv (desde 0)")
    p.add_argument("--hasta", type=int, default=None, help="fila final, no incluida (por defecto, hasta el final)")
    p.add_argument("--diario", action="store_true", help="baja variables diarias (plan B)")
    p.add_argument("--pausa", type=float, default=5.0, help="segundos entre consultas (por defecto 5)")
    a = p.parse_args()

    for f in (a.inicio, a.fin):
        dt.date.fromisoformat(f)  # falla con un mensaje claro si la fecha está mal escrita

    celdas = leer_celdas(a.celdas)
    hasta = len(celdas) if a.hasta is None else min(a.hasta, len(celdas))
    if not 0 <= a.desde < hasta:
        sys.exit(f"Rango inválido: --desde {a.desde} --hasta {hasta} (celdas.csv tiene {len(celdas)} filas)")
    tramo = celdas[a.desde:hasta]

    salida = os.path.expanduser(a.salida)
    os.makedirs(salida, exist_ok=True)
    tipo = "diario" if a.diario else "horario"
    print(f"Celdas {a.desde} a {hasta - 1} ({len(tramo)} de {len(celdas)}), clima {tipo} "
          f"del {a.inicio} al {a.fin}, en {salida}", flush=True)

    bajadas = saltadas = fallidas = 0
    t_total = time.time()
    for i, c in enumerate(tramo, start=a.desde):
        cid = c["celda_id"]
        destino = os.path.join(salida, f"{cid}.csv")
        if os.path.exists(destino):
            saltadas += 1
            print(f"[{i}] {cid}: ya existe, la salto", flush=True)
            continue

        url = armar_url(c["lat_celda"], c["lon_celda"], a.inicio, a.fin, a.diario)
        print(f"[{i}] {cid}: descargando...", flush=True)
        t0 = time.time()
        try:
            contenido = descargar(url)
            filas = validar(contenido, a.fin)
        except LimiteDiario as e:
            print(f"\nSe agotó el límite diario de Open-Meteo para esta red ({e}).\n"
                  f"Vuelve a ejecutar el mismo comando mañana: las celdas ya bajadas se saltan.")
            break
        except Exception as e:
            fallidas += 1
            print(f"    FALLÓ: {e}", flush=True)
            continue

        tmp = destino + ".tmp"
        with open(tmp, "wb") as f:
            f.write(contenido)
        os.replace(tmp, destino)

        seg = time.time() - t0
        bajadas += 1
        anotar_log(salida, [dt.datetime.now().isoformat(timespec="seconds"), cid, a.inicio, a.fin,
                            tipo, round(seg, 1), len(contenido), filas])
        print(f"    ok: {filas} filas, {len(contenido) / 1e6:.2f} MB, {seg:.1f} s", flush=True)
        if a.pausa:
            time.sleep(a.pausa)

    print(f"\nListo: {bajadas} bajadas, {saltadas} ya existían, {fallidas} fallidas "
          f"({(time.time() - t_total) / 60:.1f} min). Log en {os.path.join(salida, 'descargas_log.csv')}")
    if fallidas:
        print("Vuelve a ejecutar el mismo comando para reintentar las fallidas.")
        sys.exit(1)


if __name__ == "__main__":
    main()
