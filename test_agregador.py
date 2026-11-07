"""
Pruebas automatizadas del agregador (pytest).

No necesitan red ni el servicio inestable: PokeAPI y el servicio de sensores se
simulan con httpx.MockTransport, y asyncio.sleep se reemplaza por una versión
que registra la espera pedida sin dormir, para comprobar backoff y Retry-After
sin que las pruebas tarden.

Uso:
    pytest -v
"""

import asyncio
import base64

import httpx
import pytest

import main

URL_SENSORES = main.SERVICIO_INESTABLE_URL
URL_POKEAPI = main.POKEAPI_URL

SENSORES = [{"id": f"S-{i:03d}", "tipo": "humedad" if i % 2 else "ruido", "ubicacion": "Edificio A"}
            for i in range(1, 13)]
TAM_PAGINA = 5


def cursor(desplazamiento: int) -> str:
    return base64.urlsafe_b64encode(f"o:{desplazamiento}".encode()).decode()


def pagina_sensores(request: httpx.Request) -> httpx.Response:
    """Imita GET /sensores del servicio inestable (sin fallos)."""
    c = request.url.params.get("cursor")
    inicio = 0 if c is None else int(base64.urlsafe_b64decode(c).decode().split(":")[1])
    fin = inicio + TAM_PAGINA
    siguiente = cursor(fin) if fin < len(SENSORES) else None
    return httpx.Response(200, json={"sensores": SENSORES[inicio:fin], "siguiente": siguiente})


def respuesta_pokeapi(request: httpx.Request) -> httpx.Response:
    """Imita /type/{tipo} y /pokemon/{id} de PokeAPI."""
    ruta = request.url.path
    if ruta.endswith("/type/fire"):
        return httpx.Response(200, json={"name": "fire", "pokemon": [
            {"pokemon": {"name": f"poke{i}", "url": f"{URL_POKEAPI}/pokemon/{i}/"}} for i in range(1, 6)]})
    if "/type/" in ruta:
        return httpx.Response(404, text="Not Found")
    if "/pokemon/" in ruta:
        n = int(ruta.rstrip("/").split("/")[-1])
        return httpx.Response(200, json={"id": n, "name": f"poke{n}", "base_experience": 50 + n,
                                         "weight": 10 * n, "height": n})
    return httpx.Response(404)


class Dependencias:
    """Transporte simulado: guarda cada petición y permite programar fallos por ruta."""

    def __init__(self):
        self.peticiones = []
        self.guion = {}  # ruta -> lista de respuestas/excepciones que se consumen en orden

    def programar(self, ruta: str, *respuestas):
        self.guion.setdefault(ruta, []).extend(respuestas)

    def contar(self, ruta: str) -> int:
        return sum(1 for r in self.peticiones if r.url.path == ruta)

    async def __call__(self, request: httpx.Request) -> httpx.Response:
        self.peticiones.append(request)
        pendientes = self.guion.get(request.url.path)
        if pendientes:
            siguiente = pendientes.pop(0)
            if isinstance(siguiente, Exception):
                raise siguiente
            return siguiente
        if str(request.url).startswith(URL_POKEAPI):
            return respuesta_pokeapi(request)
        if request.url.path == "/sensores":
            return pagina_sensores(request)
        if request.url.path.startswith("/sensores/"):
            sensor_id = request.url.path.split("/")[2]
            if sensor_id not in {s["id"] for s in SENSORES}:
                return httpx.Response(404, json={"detail": f"No existe el sensor {sensor_id}"})
            return httpx.Response(200, json={"sensor_id": sensor_id, "tipo": "humedad", "valor": 55.5,
                                             "unidad": "%", "marca_tiempo": "2026-10-25T00:00:00+00:00"})
        return httpx.Response(404)


@pytest.fixture
def deps(monkeypatch):
    """Reinicia el estado del agregador y conecta el transporte simulado."""
    main.cache_pokeapi.vaciar()
    main.no_llamar_antes_de.clear()
    main.alertas.clear()
    main.idempotencia.clear()

    d = Dependencias()
    main.app.state.http = httpx.AsyncClient(transport=httpx.MockTransport(d))

    d.esperas = []
    sleep_real = asyncio.sleep

    async def sleep_registrado(segundos, *args, **kwargs):
        if segundos > 0:
            d.esperas.append(segundos)
        await sleep_real(0)

    monkeypatch.setattr(asyncio, "sleep", sleep_registrado)
    return d


def pedir(metodo: str, ruta: str, **kwargs) -> httpx.Response:
    async def _pedir():
        transporte = httpx.ASGITransport(app=main.app)
        async with httpx.AsyncClient(transport=transporte, base_url="http://agregador") as cliente:
            return await cliente.request(metodo, ruta, **kwargs)
    return asyncio.run(_pedir())


# ---------------------------------------------------------------------------
# Reintentos selectivos
# ---------------------------------------------------------------------------

def test_reintenta_503_y_se_recupera(deps):
    deps.programar("/sensores", httpx.Response(503), httpx.Response(503))
    r = pedir("GET", "/v1/sensores")
    assert r.status_code == 200
    assert r.json()["total"] == len(SENSORES)
    # 2 fallos + 3 páginas correctas
    assert deps.contar("/sensores") == 5
    assert len(deps.esperas) == 2


def test_respeta_retry_after(deps):
    deps.programar("/sensores", httpx.Response(429, headers={"Retry-After": "2"}))
    r = pedir("GET", "/v1/sensores")
    assert r.status_code == 200
    assert deps.esperas, "debió esperar antes de reintentar"
    assert deps.esperas[0] >= 2, "nunca se espera menos de lo que pide Retry-After"


def test_retry_after_excesivo_no_se_reintenta_antes_de_tiempo(deps):
    deps.programar("/sensores", httpx.Response(429, headers={"Retry-After": "600"}))
    r = pedir("GET", "/v1/sensores")
    assert r.status_code == 502
    assert deps.contar("/sensores") == 1
    assert deps.esperas == []


def test_retry_after_compartido_entre_peticiones(deps):
    deps.programar("/sensores", httpx.Response(429, headers={"Retry-After": "2"}))
    assert pedir("GET", "/v1/sensores").status_code == 200
    # Simula que el plazo aún está vigente para una petición nueva
    main.no_llamar_antes_de[httpx.URL(URL_SENSORES).netloc.decode()] = main.time.monotonic() + 1.5
    deps.esperas.clear()
    assert pedir("GET", "/v1/sensores/S-001/lectura").status_code == 200
    assert deps.esperas and deps.esperas[0] >= 1.4


def test_404_no_se_reintenta(deps):
    r = pedir("GET", "/v1/sensores/S-999/lectura")
    assert r.status_code == 404
    assert deps.contar("/sensores/S-999/lectura") == 1
    assert deps.esperas == []


def test_400_no_se_reintenta(deps):
    deps.programar("/sensores", httpx.Response(400, json={"detail": "Cursor inválido"}))
    r = pedir("GET", "/v1/sensores")
    assert r.status_code == 502
    assert deps.contar("/sensores") == 1


def test_agota_intentos_devuelve_502(deps):
    deps.programar("/sensores", *[httpx.Response(503) for _ in range(main.MAX_INTENTOS)])
    r = pedir("GET", "/v1/sensores")
    assert r.status_code == 502
    assert deps.contar("/sensores") == main.MAX_INTENTOS
    assert len(deps.esperas) == main.MAX_INTENTOS - 1


def test_timeouts_devuelven_504(deps):
    deps.programar("/sensores", *[httpx.ReadTimeout("lento") for _ in range(main.MAX_INTENTOS)])
    r = pedir("GET", "/v1/sensores")
    assert r.status_code == 504
    assert deps.contar("/sensores") == main.MAX_INTENTOS


def test_backoff_exponencial_con_jitter():
    for intento in range(1, 8):
        techo = min(main.BACKOFF_TOPE_S, main.BACKOFF_BASE_S * 2 ** (intento - 1))
        esperas = [main.calcular_backoff(intento) for _ in range(200)]
        assert all(techo / 2 <= e <= techo for e in esperas)
        assert len(set(esperas)) > 1, "debe haber jitter"


# ---------------------------------------------------------------------------
# Paginación
# ---------------------------------------------------------------------------

def test_recorre_toda_la_paginacion(deps):
    r = pedir("GET", "/v1/sensores")
    datos = r.json()
    assert r.status_code == 200
    assert datos["paginas_recorridas"] == 3
    assert [s["id"] for s in datos["sensores"]] == [s["id"] for s in SENSORES]


def test_paginacion_que_no_termina(deps):
    bucle = httpx.Response(200, json={"sensores": [], "siguiente": cursor(5)})
    deps.programar("/sensores", bucle, bucle, bucle)
    assert pedir("GET", "/v1/sensores").status_code == 502


# ---------------------------------------------------------------------------
# PokeAPI, caché y códigos de estado
# ---------------------------------------------------------------------------

def test_resumen_y_cache(deps):
    r1 = pedir("GET", "/v1/equipos/fire/resumen?limite=3")
    assert r1.status_code == 200
    assert r1.json()["origen_datos_pokemon"] == "api"
    assert len(r1.json()["pokemones_destacados"]) == 3
    assert r1.json()["total_sensores_activos"] == len(SENSORES)
    llamadas_pokeapi = sum(1 for p in deps.peticiones if str(p.url).startswith(URL_POKEAPI))

    r2 = pedir("GET", "/v1/equipos/fire/resumen?limite=3")
    assert r2.json()["origen_datos_pokemon"] == "cache"
    assert sum(1 for p in deps.peticiones if str(p.url).startswith(URL_POKEAPI)) == llamadas_pokeapi

    r3 = pedir("GET", "/v1/equipos/fire/resumen?limite=4")
    assert r3.json()["origen_datos_pokemon"] == "mixto"


def test_tipo_inexistente_404(deps):
    assert pedir("GET", "/v1/equipos/tipoquenoexiste/resumen").status_code == 404


def test_respuesta_invalida_de_pokeapi_502(deps):
    deps.programar("/api/v2/type/fire", httpx.Response(200, json={"name": "fire"}))
    r = pedir("GET", "/v1/equipos/fire/resumen")
    assert r.status_code == 502
    assert "pokemon" in r.json()["detail"]


@pytest.mark.parametrize("ruta", [
    "/v1/equipos/f1re/resumen",
    "/v1/equipos/fire/resumen?limite=50",
    "/v1/sensores/sensor1/lectura",
])
def test_parametros_invalidos_422(deps, ruta):
    assert pedir("GET", ruta).status_code == 422


def test_si_una_tarea_falla_se_cancelan_las_demas():
    cancelada = asyncio.Event()

    async def lenta():
        try:
            await asyncio.sleep(10)
        except asyncio.CancelledError:
            cancelada.set()
            raise

    async def falla():
        await asyncio.sleep(0)
        raise main.DependenciaError(502, "fallo")

    async def escenario():
        with pytest.raises(main.DependenciaError):
            await main.gather_cancelando(lenta(), falla())
        assert cancelada.is_set()

    asyncio.run(escenario())


# ---------------------------------------------------------------------------
# Idempotencia
# ---------------------------------------------------------------------------

ALERTA = {"nivel": "critico", "mensaje": "Fallo en el servidor de sensores", "sensor_id": "S-001"}


def test_idempotencia_misma_clave_una_sola_alerta(deps):
    cabeceras = {"Idempotency-Key": "clave-1"}
    r1 = pedir("POST", "/v1/alertas", json=ALERTA, headers=cabeceras)
    r2 = pedir("POST", "/v1/alertas", json=ALERTA, headers=cabeceras)
    assert r1.status_code == r2.status_code == 201
    assert r1.json() == r2.json()
    assert r1.headers["Idempotent-Replayed"] == "false"
    assert r2.headers["Idempotent-Replayed"] == "true"
    assert len(pedir("GET", "/v1/alertas").json()) == 1


def test_idempotencia_misma_clave_otro_cuerpo_422(deps):
    cabeceras = {"Idempotency-Key": "clave-2"}
    pedir("POST", "/v1/alertas", json=ALERTA, headers=cabeceras)
    r = pedir("POST", "/v1/alertas", json={**ALERTA, "nivel": "info"}, headers=cabeceras)
    assert r.status_code == 422
    assert len(pedir("GET", "/v1/alertas").json()) == 1


def test_alerta_sin_clave_422(deps):
    assert pedir("POST", "/v1/alertas", json=ALERTA).status_code == 422


def test_claves_distintas_crean_alertas_distintas(deps):
    a = pedir("POST", "/v1/alertas", json=ALERTA, headers={"Idempotency-Key": "a"}).json()
    b = pedir("POST", "/v1/alertas", json=ALERTA, headers={"Idempotency-Key": "b"}).json()
    assert a["id_alerta"] != b["id_alerta"]
    assert pedir("GET", f"/v1/alertas/{a['id_alerta']}").status_code == 200
    assert pedir("GET", "/v1/alertas/ALT-NOEXISTE").status_code == 404


def test_claves_vencidas_se_purgan(deps, monkeypatch):
    pedir("POST", "/v1/alertas", json=ALERTA, headers={"Idempotency-Key": "vieja"})
    ahora = main.time.monotonic()
    monkeypatch.setattr(main.time, "monotonic", lambda: ahora + main.IDEMPOTENCIA_TTL_S + 1)
    pedir("POST", "/v1/alertas", json=ALERTA, headers={"Idempotency-Key": "nueva"})
    assert "vieja" not in main.idempotencia
    assert "nueva" in main.idempotencia
