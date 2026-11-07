"""
Agregador resiliente de APIs — Sistemas Distribuidos (UIS)

Combina:
  * PokeAPI (API externa): GET /type/{tipo} -> GET /pokemon/{id} (llamadas encadenadas)
  * Servicio inestable de sensores (dependencia propia): GET /sensores?cursor=...

Patrones aplicados: timeouts explícitos, reintentos selectivos con backoff
exponencial y jitter, respeto de Retry-After, recorrido completo de la
paginación, caché en memoria con TTL e idempotencia en POST /v1/alertas.

Variables de entorno:
  SERVICIO_INESTABLE_URL  URL del servicio inestable (por defecto http://127.0.0.1:9000)
  POKEAPI_URL             URL base de PokeAPI       (por defecto https://pokeapi.co/api/v2)
  TIMEOUT_SEGUNDOS        Timeout de cada llamada saliente (por defecto 5)
  MAX_INTENTOS            Intentos máximos por llamada     (por defecto 5)
  CACHE_TTL_SEGUNDOS      TTL de la caché de PokeAPI       (por defecto 60)

Arranque:
  uvicorn main:app --reload --port 8000
"""

import asyncio
import hashlib
import json
import logging
import math
import os
import random
import time
import uuid
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from typing import Any, Dict, List, Literal, Optional, Tuple

import httpx
from fastapi import FastAPI, Header, HTTPException, Path, Query, Request, Response, status
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field, ValidationError

# ==========================================
# Configuración
# ==========================================
SERVICIO_INESTABLE_URL = os.getenv("SERVICIO_INESTABLE_URL", "http://127.0.0.1:9000").rstrip("/")
POKEAPI_URL = os.getenv("POKEAPI_URL", "https://pokeapi.co/api/v2").rstrip("/")
TIMEOUT_SEGUNDOS = float(os.getenv("TIMEOUT_SEGUNDOS", "5"))
MAX_INTENTOS = int(os.getenv("MAX_INTENTOS", "5"))
CACHE_TTL_SEGUNDOS = float(os.getenv("CACHE_TTL_SEGUNDOS", "60"))

BACKOFF_BASE_S = 0.5     # espera "techo" del primer reintento
BACKOFF_TOPE_S = 8.0     # nunca esperar más que esto entre reintentos
# Si la dependencia pide esperar más que esto, NO se vuelve antes de tiempo:
# se deja de reintentar y se responde 502 de inmediato (no tiene sentido tener
# al cliente esperando minutos). Así Retry-After se respeta siempre.
RETRY_AFTER_MAX_S = 30.0
MAX_PAGINAS = 1000       # protección ante un cursor que no termina nunca

# Límites de memoria de los almacenes en memoria
IDEMPOTENCIA_TTL_S = 24 * 3600  # una Idempotency-Key se recuerda 24 h (como Stripe)
MAX_ALERTAS = 10_000            # al llenarse se descartan las alertas más antiguas

CODIGOS_REINTENTABLES = {429, 502, 503, 504}

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s.%(msecs)03d %(levelname)-7s %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("agregador")
# httpx registra cada petición en INFO; los intentos ya los registra fetch_resiliente
logging.getLogger("httpx").setLevel(logging.WARNING)


# ==========================================
# Errores de dependencia -> códigos HTTP coherentes
# ==========================================
class DependenciaError(Exception):
    """Fallo de una dependencia remota que se traduce a 502/504 (nunca a 500)."""

    def __init__(self, status_code: int, detalle: str):
        self.status_code = status_code
        self.detalle = detalle
        super().__init__(detalle)


# ==========================================
# Caché en memoria con TTL
# ==========================================
class CacheTTL:
    def __init__(self, ttl_segundos: float, max_entradas: int = 1000):
        self.ttl = ttl_segundos
        self.max_entradas = max_entradas
        self._datos: Dict[str, Tuple[float, Any]] = {}
        self.aciertos = 0
        self.fallos = 0

    def obtener(self, clave: str) -> Optional[Any]:
        entrada = self._datos.get(clave)
        if entrada is None:
            self.fallos += 1
            return None
        expira, valor = entrada
        if time.monotonic() >= expira:
            del self._datos[clave]
            self.fallos += 1
            return None
        self.aciertos += 1
        return valor

    def guardar(self, clave: str, valor: Any) -> None:
        if len(self._datos) >= self.max_entradas:
            # Expulsa la entrada que vence primero
            mas_antigua = min(self._datos, key=lambda k: self._datos[k][0])
            del self._datos[mas_antigua]
        self._datos[clave] = (time.monotonic() + self.ttl, valor)

    def vaciar(self) -> int:
        n = len(self._datos)
        self._datos.clear()
        return n

    def estado(self) -> Dict[str, Any]:
        return {"entradas": len(self._datos), "aciertos": self.aciertos,
                "fallos": self.fallos, "ttl_segundos": self.ttl}


cache_pokeapi = CacheTTL(CACHE_TTL_SEGUNDOS)

# Almacenes en memoria (un solo proceso; ver README, pregunta d). Ambos están
# acotados: las claves vencen a las IDEMPOTENCIA_TTL_S y como mucho se guardan
# MAX_ALERTAS alertas (los dict conservan el orden de inserción).
alertas: Dict[str, "AlertaResponse"] = {}
# clave -> (instante de expiración, huella del cuerpo, respuesta)
idempotencia: Dict[str, Tuple[float, str, "AlertaResponse"]] = {}


def purgar_almacenes() -> None:
    """Elimina las claves de idempotencia vencidas y las alertas que sobran."""
    ahora = time.monotonic()
    # Las claves se insertan en orden de expiración: basta con mirar el principio
    for clave in list(idempotencia):
        if idempotencia[clave][0] > ahora:
            break
        del idempotencia[clave]
    while len(alertas) > MAX_ALERTAS:
        del alertas[next(iter(alertas))]
    while len(idempotencia) > MAX_ALERTAS:
        del idempotencia[next(iter(idempotencia))]


async def gather_cancelando(*corrutinas) -> List[Any]:
    """
    Como asyncio.gather, pero si una tarea falla cancela las demás y espera a
    que terminen, para no dejar llamadas trabajando en segundo plano.
    """
    tareas = [asyncio.ensure_future(c) for c in corrutinas]
    try:
        return await asyncio.gather(*tareas)
    except BaseException:
        for t in tareas:
            t.cancel()
        await asyncio.gather(*tareas, return_exceptions=True)
        raise


# ==========================================
# Ciclo de vida: cliente HTTP compartido
# ==========================================
@asynccontextmanager
async def lifespan(app: FastAPI):
    app.state.http = httpx.AsyncClient(
        timeout=httpx.Timeout(TIMEOUT_SEGUNDOS),
        headers={"User-Agent": "agregador-resiliente-uis/1.0"},
        follow_redirects=True,
    )
    log.info("Servicio inestable: %s | PokeAPI: %s | timeout=%ss | intentos=%s | TTL=%ss",
             SERVICIO_INESTABLE_URL, POKEAPI_URL, TIMEOUT_SEGUNDOS, MAX_INTENTOS, CACHE_TTL_SEGUNDOS)
    yield
    await app.state.http.aclose()


app = FastAPI(
    title="Agregador Resiliente de APIs",
    description=(
        "Combina PokeAPI con un servicio de sensores inestable aplicando timeouts, "
        "reintentos selectivos con backoff exponencial y jitter, respeto de Retry-After, "
        "caché con TTL e idempotencia.\n\n"
        "Códigos de error: **404** recurso inexistente en la dependencia, **422** parámetros "
        "inválidos, **502** la dependencia respondió mal o agotó los reintentos, **504** la "
        "dependencia no respondió a tiempo."
    ),
    version="1.1.0",
    lifespan=lifespan,
)


@app.middleware("http")
async def medir_tiempo(request: Request, call_next):
    inicio = time.perf_counter()
    respuesta = await call_next(request)
    ms = (time.perf_counter() - inicio) * 1000
    respuesta.headers["X-Tiempo-Respuesta-ms"] = f"{ms:.1f}"
    log.info("%s %s -> %s (%.1f ms)", request.method, request.url.path, respuesta.status_code, ms)
    return respuesta


@app.exception_handler(DependenciaError)
async def manejar_dependencia(request: Request, exc: DependenciaError):
    return JSONResponse(status_code=exc.status_code, content={"detail": exc.detalle})


# ==========================================
# Modelos Pydantic v2
# ==========================================
class ErrorRespuesta(BaseModel):
    detail: str


# --- Respuestas de PokeAPI (solo los campos que usamos) ---
class RecursoNombrado(BaseModel):
    name: str
    url: str


class EntradaPokemonTipo(BaseModel):
    pokemon: RecursoNombrado


class TipoPokeAPI(BaseModel):
    name: str
    pokemon: List[EntradaPokemonTipo]


class PokemonPokeAPI(BaseModel):
    id: int
    name: str
    base_experience: Optional[int] = None  # PokeAPI devuelve null en algunas formas
    weight: int
    height: int


# --- Respuestas del servicio inestable ---
class SensorRemoto(BaseModel):
    id: str
    tipo: str
    ubicacion: str


class PaginaSensores(BaseModel):
    sensores: List[SensorRemoto]
    siguiente: Optional[str] = None


class LecturaRemota(BaseModel):
    sensor_id: str
    tipo: str
    valor: float
    unidad: str
    marca_tiempo: str


# --- API propia ---
class PokemonBaseInfo(BaseModel):
    id: int
    name: str
    base_experience: Optional[int]
    weight: int
    height: int


class LatenciasMs(BaseModel):
    pokeapi: float
    sensores: float
    total: float


class ResumenEquiposResponse(BaseModel):
    tipo_pokemon: str
    pokemones_destacados: List[PokemonBaseInfo]
    total_sensores_activos: int
    sensores_por_tipo: Dict[str, int]
    origen_datos_pokemon: Literal["cache", "api", "mixto"] = Field(
        description="'cache' si todo salió de la caché, 'api' si todo se pidió a PokeAPI, 'mixto' si hubo de ambos")
    latencias_ms: LatenciasMs


class SensoresResponse(BaseModel):
    total: int
    paginas_recorridas: int
    sensores: List[SensorRemoto]


class AlertaPayload(BaseModel):
    nivel: Literal["info", "advertencia", "critico"] = Field(..., examples=["critico"])
    mensaje: str = Field(..., min_length=1, max_length=500, examples=["Fallo general en el sector B"])
    sensor_id: str = Field(..., pattern=r"^S-\d{3}$", examples=["S-001"])


class AlertaResponse(BaseModel):
    id_alerta: str
    estado: str
    creada_en: str
    datos: AlertaPayload
    mensaje: str


# ==========================================
# Resiliencia: reintentos selectivos, backoff con jitter, Retry-After
# ==========================================
def calcular_backoff(intento: int) -> float:
    """Backoff exponencial con 'equal jitter': mitad fija + mitad aleatoria."""
    techo = min(BACKOFF_TOPE_S, BACKOFF_BASE_S * (2 ** (intento - 1)))
    return techo / 2 + random.uniform(0, techo / 2)


# Retry-After compartido: host -> instante (monotónico) antes del cual NO se le
# debe volver a llamar. Así lo respetan también las demás peticiones en curso,
# no solo la que recibió el 429 (el servicio inestable bloquea por IP).
no_llamar_antes_de: Dict[str, float] = {}


async def esperar_retry_after_pendiente(host: str, nombre: str) -> None:
    pendiente = no_llamar_antes_de.get(host, 0.0) - time.monotonic()
    if pendiente <= 0:
        return
    if pendiente > RETRY_AFTER_MAX_S:
        raise DependenciaError(status.HTTP_502_BAD_GATEWAY,
                               f"{nombre} pidió no llamar durante {pendiente:.0f} s más (Retry-After)")
    # Jitter: si varias peticiones esperan el mismo plazo, no salen todas a la vez
    espera = pendiente + random.uniform(0, 0.3)
    log.info("[%s] Retry-After vigente para %s: esperando %.2f s antes de llamar", nombre, host, espera)
    await asyncio.sleep(espera)


def leer_retry_after(respuesta: httpx.Response) -> Optional[float]:
    """Acepta Retry-After en segundos o como fecha HTTP. Devuelve segundos >= 0."""
    valor = respuesta.headers.get("Retry-After")
    if valor is None:
        return None
    try:
        segundos = float(valor)
    except ValueError:
        try:
            fecha = parsedate_to_datetime(valor)
            segundos = (fecha - datetime.now(timezone.utc)).total_seconds()
        except (TypeError, ValueError):
            return None
    if not math.isfinite(segundos):
        return None
    return max(segundos, 0.0)


async def fetch_resiliente(url: str, params: Optional[dict] = None, *, nombre: str) -> httpx.Response:
    """
    GET con timeout explícito y reintentos SOLO para lo reintentable
    (timeouts, errores de conexión, 429, 502, 503, 504).

    Devuelve la respuesta si es 2xx o un error NO reintentable (400, 404...),
    para que quien llama decida qué código devolver. Si se agotan los intentos
    lanza DependenciaError con 504 (timeouts) o 502 (resto).
    """
    cliente: httpx.AsyncClient = app.state.http
    ultimo_fallo = ""
    hubo_timeout = False

    host = httpx.URL(url).netloc.decode()

    for intento in range(1, MAX_INTENTOS + 1):
        await esperar_retry_after_pendiente(host, nombre)
        try:
            respuesta = await cliente.get(url, params=params, timeout=TIMEOUT_SEGUNDOS)
        except httpx.TimeoutException:
            hubo_timeout = True
            ultimo_fallo = f"timeout de {TIMEOUT_SEGUNDOS} s"
            espera = calcular_backoff(intento)
        except httpx.RequestError as e:
            hubo_timeout = False
            ultimo_fallo = f"error de conexión ({type(e).__name__})"
            espera = calcular_backoff(intento)
        else:
            if respuesta.status_code not in CODIGOS_REINTENTABLES:
                if intento > 1:
                    log.info("[%s] intento %d/%d -> %d (recuperado)", nombre, intento,
                             MAX_INTENTOS, respuesta.status_code)
                return respuesta  # 2xx o error legítimo: no se reintenta
            hubo_timeout = False
            ultimo_fallo = f"HTTP {respuesta.status_code}"
            retry_after = leer_retry_after(respuesta)
            if retry_after is not None:
                # Se respeta Retry-After: nunca se espera menos de lo pedido.
                espera = retry_after + random.uniform(0, 0.3)
                no_llamar_antes_de[host] = max(no_llamar_antes_de.get(host, 0.0),
                                               time.monotonic() + espera)
                ultimo_fallo += f" (Retry-After: {respuesta.headers.get('Retry-After')})"
                if retry_after > RETRY_AFTER_MAX_S:
                    # Esperar tanto no sirve al cliente; volver antes violaría Retry-After.
                    log.error("[%s] intento %d/%d falló: %s -> no se reintenta (supera %.0f s)",
                              nombre, intento, MAX_INTENTOS, ultimo_fallo, RETRY_AFTER_MAX_S)
                    raise DependenciaError(status.HTTP_502_BAD_GATEWAY,
                                           f"{nombre} pidió esperar {retry_after:.0f} s: {ultimo_fallo}")
            else:
                espera = calcular_backoff(intento)

        if intento == MAX_INTENTOS:
            break
        log.warning("[%s] intento %d/%d falló: %s -> esperando %.2f s", nombre, intento,
                    MAX_INTENTOS, ultimo_fallo, espera)
        await asyncio.sleep(espera)

    log.error("[%s] se agotaron los %d intentos (último: %s)", nombre, MAX_INTENTOS, ultimo_fallo)
    codigo = status.HTTP_504_GATEWAY_TIMEOUT if hubo_timeout else status.HTTP_502_BAD_GATEWAY
    raise DependenciaError(codigo, f"{nombre} no disponible tras {MAX_INTENTOS} intentos: {ultimo_fallo}")


def validar(modelo, datos: Any, nombre: str):
    """Rechaza con 502 y un mensaje claro una respuesta que no cumple el modelo."""
    try:
        return modelo.model_validate(datos)
    except ValidationError as e:
        errores = "; ".join(f"{'.'.join(map(str, err['loc']))}: {err['msg']}" for err in e.errors()[:3])
        raise DependenciaError(status.HTTP_502_BAD_GATEWAY,
                               f"Respuesta inválida de {nombre}: {errores}")


def leer_json(respuesta: httpx.Response, nombre: str) -> Any:
    try:
        return respuesta.json()
    except ValueError:
        raise DependenciaError(status.HTTP_502_BAD_GATEWAY, f"{nombre} devolvió algo que no es JSON")


# ==========================================
# Cliente de PokeAPI (con caché)
# ==========================================
async def get_pokeapi(url: str) -> Tuple[Any, bool]:
    """GET a PokeAPI pasando por la caché. Devuelve (json, vino_de_cache)."""
    cacheado = cache_pokeapi.obtener(url)
    if cacheado is not None:
        return cacheado, True

    respuesta = await fetch_resiliente(url, nombre="PokeAPI")
    if respuesta.status_code == 404:
        raise HTTPException(status_code=404, detail="Recurso no encontrado en PokeAPI")
    if respuesta.is_error:
        raise DependenciaError(status.HTTP_502_BAD_GATEWAY,
                               f"PokeAPI respondió HTTP {respuesta.status_code}")
    datos = leer_json(respuesta, "PokeAPI")
    cache_pokeapi.guardar(url, datos)
    return datos, False


async def obtener_datos_pokemon(tipo: str, limite: int) -> Tuple[List[PokemonBaseInfo], str]:
    """Encadena: GET /type/{tipo} -> GET /pokemon/{id} (en paralelo para cada uno)."""
    try:
        datos_tipo, cache_tipo = await get_pokeapi(f"{POKEAPI_URL}/type/{tipo}")
    except HTTPException as e:
        if e.status_code == 404:
            raise HTTPException(status_code=404, detail=f"El tipo de Pokémon '{tipo}' no existe")
        raise
    tipo_valido = validar(TipoPokeAPI, datos_tipo, "PokeAPI /type")

    seleccion = tipo_valido.pokemon[:limite]
    try:
        resultados = await gather_cancelando(*(get_pokeapi(p.pokemon.url) for p in seleccion))
    except HTTPException as e:
        if e.status_code == 404:
            # El tipo existe pero PokeAPI no tiene un Pokémon que él mismo listó:
            # es una incoherencia de la dependencia, no un recurso inexistente.
            raise DependenciaError(status.HTTP_502_BAD_GATEWAY,
                                   "PokeAPI no encontró un Pokémon listado en el tipo")
        raise

    detalles = []
    flags_cache = [cache_tipo]
    for datos, de_cache in resultados:
        flags_cache.append(de_cache)
        p = validar(PokemonPokeAPI, datos, "PokeAPI /pokemon")
        detalles.append(PokemonBaseInfo(**p.model_dump()))

    if all(flags_cache):
        origen = "cache"
    elif not any(flags_cache):
        origen = "api"
    else:
        origen = "mixto"
    return detalles, origen


# ==========================================
# Cliente del servicio inestable
# ==========================================
async def obtener_sensores_paginados() -> Tuple[List[SensorRemoto], int]:
    """Recorre TODA la paginación (cursor 'siguiente') del servicio inestable."""
    sensores: List[SensorRemoto] = []
    cursor: Optional[str] = None
    vistos = set()
    paginas = 0

    while True:
        params = {"cursor": cursor} if cursor else None
        respuesta = await fetch_resiliente(f"{SERVICIO_INESTABLE_URL}/sensores", params,
                                           nombre="Servicio inestable /sensores")
        if respuesta.status_code == 400:
            # Error legítimo (cursor inválido): no se reintenta. Desde el punto de
            # vista de nuestro cliente es un fallo de la dependencia -> 502.
            raise DependenciaError(status.HTTP_502_BAD_GATEWAY,
                                   "El servicio de sensores rechazó el cursor de paginación (400)")
        if respuesta.is_error:
            raise DependenciaError(status.HTTP_502_BAD_GATEWAY,
                                   f"El servicio de sensores respondió HTTP {respuesta.status_code}")

        pagina = validar(PaginaSensores, leer_json(respuesta, "Servicio inestable"), "Servicio inestable")
        paginas += 1
        sensores.extend(pagina.sensores)
        cursor = pagina.siguiente
        if not cursor:
            break
        if cursor in vistos or paginas >= MAX_PAGINAS:
            raise DependenciaError(status.HTTP_502_BAD_GATEWAY, "La paginación del servicio de sensores no termina")
        vistos.add(cursor)

    return sensores, paginas


async def cronometrar(corrutina) -> Tuple[Any, float]:
    inicio = time.perf_counter()
    resultado = await corrutina
    return resultado, (time.perf_counter() - inicio) * 1000


# ==========================================
# Endpoints de la API propia
# ==========================================
ERRORES_DEPENDENCIA = {
    502: {"model": ErrorRespuesta, "description": "La dependencia falló o agotó los reintentos"},
    504: {"model": ErrorRespuesta, "description": "La dependencia no respondió a tiempo"},
}


@app.get("/salud", tags=["Mantenimiento"])
async def comprobacion_salud():
    """Comprobación de vida del agregador (no consulta las dependencias)."""
    return {"estado": "ok", "marca_tiempo": datetime.now(timezone.utc).isoformat(timespec="seconds")}


@app.get(
    "/v1/equipos/{tipo}/resumen",
    response_model=ResumenEquiposResponse,
    tags=["Agregación"],
    responses={404: {"model": ErrorRespuesta, "description": "El tipo no existe en PokeAPI"},
               **ERRORES_DEPENDENCIA},
)
async def resumen_equipos(
    tipo: str = Path(..., min_length=2, max_length=30, pattern=r"^[A-Za-z-]+$",
                     description="Tipo de Pokémon en inglés (fire, water, grass...)", examples=["fire"]),
    limite: int = Query(3, ge=1, le=10, description="Cuántos Pokémon del tipo detallar"),
):
    """
    Combina PokeAPI (tipo -> detalle de cada Pokémon, con caché TTL) con el
    conteo total de sensores del servicio inestable (paginación completa).
    Ambas fuentes se consultan en paralelo.
    """
    inicio = time.perf_counter()
    # Si una de las dos falla, gather_cancelando cancela la otra (y, en cascada,
    # las llamadas a PokeAPI que estén en vuelo dentro de obtener_datos_pokemon).
    ((pokemones, origen), ms_poke), ((sensores, _), ms_sens) = await gather_cancelando(
        cronometrar(obtener_datos_pokemon(tipo.lower(), limite)),
        cronometrar(obtener_sensores_paginados()),
    )

    por_tipo: Dict[str, int] = {}
    for s in sensores:
        por_tipo[s.tipo] = por_tipo.get(s.tipo, 0) + 1

    return ResumenEquiposResponse(
        tipo_pokemon=tipo.lower(),
        pokemones_destacados=pokemones,
        total_sensores_activos=len(sensores),
        sensores_por_tipo=dict(sorted(por_tipo.items())),
        origen_datos_pokemon=origen,
        latencias_ms=LatenciasMs(pokeapi=round(ms_poke, 1), sensores=round(ms_sens, 1),
                                 total=round((time.perf_counter() - inicio) * 1000, 1)),
    )


@app.get("/v1/sensores", response_model=SensoresResponse, tags=["Sensores"], responses=ERRORES_DEPENDENCIA)
async def listar_sensores():
    """Recorre todas las páginas del servicio inestable y devuelve la lista completa."""
    sensores, paginas = await obtener_sensores_paginados()
    return SensoresResponse(total=len(sensores), paginas_recorridas=paginas, sensores=sensores)


@app.get(
    "/v1/sensores/{sensor_id}/lectura",
    response_model=LecturaRemota,
    tags=["Sensores"],
    responses={404: {"model": ErrorRespuesta, "description": "El sensor no existe"}, **ERRORES_DEPENDENCIA},
)
async def lectura_sensor(sensor_id: str = Path(..., pattern=r"^S-\d{3}$", examples=["S-001"])):
    """Lectura actual de un sensor. Un 404 del servicio inestable NO se reintenta."""
    respuesta = await fetch_resiliente(f"{SERVICIO_INESTABLE_URL}/sensores/{sensor_id}/lectura",
                                       nombre="Servicio inestable /lectura")
    if respuesta.status_code == 404:
        raise HTTPException(status_code=404, detail=f"No existe el sensor {sensor_id}")
    if respuesta.is_error:
        raise DependenciaError(status.HTTP_502_BAD_GATEWAY,
                               f"El servicio de sensores respondió HTTP {respuesta.status_code}")
    return validar(LecturaRemota, leer_json(respuesta, "Servicio inestable"), "Servicio inestable")


@app.post(
    "/v1/alertas",
    response_model=AlertaResponse,
    status_code=status.HTTP_201_CREATED,
    tags=["Alertas"],
    responses={422: {"description": "Cuerpo inválido, falta Idempotency-Key, o la clave ya se usó con otro cuerpo"}},
)
async def crear_alerta(
    alerta: AlertaPayload,
    response: Response,
    idempotency_key: str = Header(..., alias="Idempotency-Key", min_length=1, max_length=255,
                                  description="Clave única por operación; reintentos con la misma clave no duplican la alerta"),
):
    """
    Crea una alerta. Si llega otra petición con la misma Idempotency-Key y el
    mismo cuerpo, se devuelve la respuesta original (misma id_alerta) sin crear
    nada nuevo, con la cabecera `Idempotent-Replayed: true`.
    """
    huella = hashlib.sha256(json.dumps(alerta.model_dump(), sort_keys=True).encode()).hexdigest()

    purgar_almacenes()
    guardada = idempotencia.get(idempotency_key)
    if guardada is not None:
        _, huella_original, respuesta_original = guardada
        if huella_original != huella:
            raise HTTPException(status_code=422,
                                detail="La Idempotency-Key ya se usó con un cuerpo distinto")
        response.headers["Idempotent-Replayed"] = "true"
        return respuesta_original

    # Entre la comprobación y el guardado no hay ningún 'await', así que en un
    # solo proceso dos peticiones simultáneas con la misma clave no pueden colarse.
    nueva = AlertaResponse(
        id_alerta=f"ALT-{uuid.uuid4().hex[:8].upper()}",
        estado="creada",
        creada_en=datetime.now(timezone.utc).isoformat(timespec="seconds"),
        datos=alerta,
        mensaje="Alerta procesada correctamente",
    )
    alertas[nueva.id_alerta] = nueva
    idempotencia[idempotency_key] = (time.monotonic() + IDEMPOTENCIA_TTL_S, huella, nueva)
    response.headers["Idempotent-Replayed"] = "false"
    return nueva


@app.get("/v1/alertas", response_model=List[AlertaResponse], tags=["Alertas"])
async def listar_alertas():
    """Todas las alertas creadas (sirve para comprobar que un reintento no duplicó nada)."""
    return list(alertas.values())


@app.get("/v1/alertas/{id_alerta}", response_model=AlertaResponse, tags=["Alertas"],
         responses={404: {"model": ErrorRespuesta}})
async def obtener_alerta(id_alerta: str):
    alerta = alertas.get(id_alerta)
    if alerta is None:
        raise HTTPException(status_code=404, detail=f"No existe la alerta {id_alerta}")
    return alerta


@app.get("/v1/cache", tags=["Mantenimiento"])
async def estado_cache():
    """Estadísticas de la caché de PokeAPI."""
    return cache_pokeapi.estado()


@app.delete("/v1/cache", status_code=status.HTTP_204_NO_CONTENT, tags=["Mantenimiento"])
async def vaciar_cache():
    """Vacía la caché (para medir la latencia 'sin caché')."""
    cache_pokeapi.vaciar()


if __name__ == "__main__":
    # Equivale a: uvicorn main:app --port 8000
    import uvicorn

    uvicorn.run(app, host="127.0.0.1", port=int(os.getenv("PUERTO", "8000")))
