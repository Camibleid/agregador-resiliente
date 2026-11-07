"""
Mide la latencia de GET /v1/equipos/{tipo}/resumen con caché y sin ella.

Uso (con el agregador y el servicio inestable ya arrancados):
    python medir_latencia.py                    # 10 peticiones por escenario, tipo fire
    python medir_latencia.py --n 20 --tipo water
    python medir_latencia.py --url http://127.0.0.1:8000

"Sin caché": se vacía la caché (DELETE /v1/cache) antes de cada petición,
así que cada una va a PokeAPI. "Con caché": se calienta una vez y luego todas
las peticiones salen de la caché.

Además del tiempo total se informa la parte de PokeAPI (campo latencias_ms.pokeapi),
porque el tiempo total también incluye el servicio inestable, cuyos fallos
aleatorios esconden el efecto de la caché. Para aislarlo, arranque el servicio
inestable sin fallos:  python servicio_inestable.py --tasa-fallos 0 --tasa-429 0 --tasa-lentitud 0
"""

import argparse
import statistics
import time

import httpx


def resumen(valores):
    return (statistics.mean(valores), statistics.median(valores), min(valores), max(valores))


def medir(cliente, base, url, n, vaciar_antes):
    totales, pokeapi, origenes = [], [], []
    for i in range(n):
        if vaciar_antes:
            cliente.delete(f"{base}/v1/cache")
        inicio = time.perf_counter()
        r = cliente.get(url)
        ms = (time.perf_counter() - inicio) * 1000
        if r.status_code != 200:
            print(f"  petición {i + 1}: HTTP {r.status_code} -> {r.text[:120]} (se descarta)")
            continue
        datos = r.json()
        totales.append(ms)
        pokeapi.append(datos["latencias_ms"]["pokeapi"])
        origenes.append(datos["origen_datos_pokemon"])
        print(f"  petición {i + 1:>2}: total {ms:8.1f} ms | PokeAPI {pokeapi[-1]:7.1f} ms | origen {origenes[-1]}")
    return totales, pokeapi


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--url", default="http://127.0.0.1:8000")
    parser.add_argument("--tipo", default="fire")
    parser.add_argument("--n", type=int, default=10)
    args = parser.parse_args()

    args.url = args.url.rstrip("/")
    endpoint = f"{args.url}/v1/equipos/{args.tipo}/resumen"
    with httpx.Client(timeout=180) as cliente:
        print("Sin caché:")
        sin_tot, sin_poke = medir(cliente, args.url, endpoint, args.n, vaciar_antes=True)
        print("Con caché (tras calentar):")
        cliente.delete(f"{args.url}/v1/cache")
        cliente.get(endpoint)
        con_tot, con_poke = medir(cliente, args.url, endpoint, args.n, vaciar_antes=False)

    def fila(nombre, valores):
        if not valores:
            return f"| {nombre} | – | – | – | – | 0 |"
        m, med, mn, mx = resumen(valores)
        return f"| {nombre} | {m:.1f} | {med:.1f} | {mn:.1f} | {mx:.1f} | {len(valores)} |"

    print("\n| Escenario | Media (ms) | Mediana (ms) | Mín (ms) | Máx (ms) | n |")
    print("|---|---|---|---|---|---|")
    print(fila("Sin caché – parte PokeAPI", sin_poke))
    print(fila("Con caché – parte PokeAPI", con_poke))
    print(fila("Sin caché – total endpoint", sin_tot))
    print(fila("Con caché – total endpoint", con_tot))
