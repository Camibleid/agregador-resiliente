# Agregador Resiliente de APIs

**Sistemas Distribuidos · Evaluación-taller — Escuela de Ingeniería de Sistemas e Informática, UIS**

Servicio en FastAPI que combina una API externa (PokeAPI) con un servicio de sensores inestable y expone una API propia que tolera los fallos de sus dependencias mediante timeouts, reintentos selectivos con backoff exponencial y jitter, respeto de `Retry-After`, caché con TTL e idempotencia.

## Pruebas

```bash
pytest -q
```

23 pruebas automáticas en `test_agregador.py`; pasan sin levantar el servicio inestable.

## Integrantes y roles

| Integrante | Rol |
|---|---|
| Camilo Amaya | Dirección del trabajo; API propia, modelos Pydantic, pruebas y README |
| Daniel Rodriguez | Operación del servicio inestable |
| Johen Castañeda | Cliente de la API externa (PokeAPI) |
| Hector Higuera | Resiliencia frente al servicio inestable |

## API elegida: PokeAPI

Endpoints encadenados en `GET /v1/equipos/{tipo}/resumen`:

1. `GET https://pokeapi.co/api/v2/type/{tipo}` → lista de Pokémon de ese tipo.
2. `GET https://pokeapi.co/api/v2/pokemon/{id}/` → detalle de cada uno de los primeros `limite` Pokémon (estas llamadas se hacen en paralelo).

Ambas respuestas se validan con modelos Pydantic v2 (`TipoPokeAPI`, `PokemonPokeAPI`); si PokeAPI devuelve algo que no cumple el modelo, el agregador responde 502 con un mensaje que indica el campo problemático. Todas las respuestas de PokeAPI pasan por una caché en memoria con TTL, porque todos los equipos de la sala comparten la IP pública y, con ella, el límite de peticiones.

## Estructura

```
agregador-resiliente/
├── main.py                  # Agregador (FastAPI)
├── servicio_inestable.py    # Servicio del docente (no incluido en este repositorio)
├── medir_latencia.py        # Medición de latencia con y sin caché
├── pruebas.http             # Colección de peticiones (REST Client de VS Code)
├── requirements.txt
├── evidencias/              # Registros de los reintentos
└── README.md
```

## Instalación y ejecución

```bash
python -m venv venv
venv\Scripts\activate            # Windows
# source venv/bin/activate       # Linux / Mac
pip install -r requirements.txt
```

Terminal 1 — servicio inestable (puerto 9000):

```bash
python servicio_inestable.py                       # valores por defecto
python servicio_inestable.py --tasa-fallos 0.4 --semilla 42
python servicio_inestable.py --host 0.0.0.0        # accesible desde otros equipos
```

Terminal 2 — agregador (puerto 8000). La URL del servicio inestable se lee de la variable de entorno `SERVICIO_INESTABLE_URL`:

```bash
# Windows (PowerShell)
$env:SERVICIO_INESTABLE_URL = "http://127.0.0.1:9000"
# Windows (cmd):    set SERVICIO_INESTABLE_URL=http://127.0.0.1:9000
# Linux / Mac:      export SERVICIO_INESTABLE_URL=http://127.0.0.1:9000

uvicorn main:app --port 8000
```

Documentación OpenAPI: <http://127.0.0.1:8000/docs>

## Variables de entorno

| Variable | Por defecto | Uso |
|---|---|---|
| `SERVICIO_INESTABLE_URL` | `http://127.0.0.1:9000` | URL del servicio inestable |
| `POKEAPI_URL` | `https://pokeapi.co/api/v2` | URL base de PokeAPI |
| `TIMEOUT_SEGUNDOS` | `5` | Timeout de cada llamada saliente |
| `MAX_INTENTOS` | `5` | Intentos máximos por llamada (1 original + 4 reintentos) |
| `CACHE_TTL_SEGUNDOS` | `60` | Tiempo de vida de las entradas de la caché |

## Endpoints de la API propia

| Método y ruta | Descripción | Códigos |
|---|---|---|
| `GET /salud` | Comprobación de vida | 200 |
| `GET /v1/equipos/{tipo}/resumen?limite=3` | Pokémon del tipo (PokeAPI) + total de sensores y conteo por tipo (servicio inestable, paginación completa) | 200, 404, 422, 502, 504 |
| `GET /v1/sensores` | Lista completa de sensores recorriendo todas las páginas | 200, 502, 504 |
| `GET /v1/sensores/{id}/lectura` | Lectura de un sensor | 200, 404, 422, 502, 504 |
| `POST /v1/alertas` | Crea una alerta; exige la cabecera `Idempotency-Key` | 201, 422 |
| `GET /v1/alertas` | Lista las alertas creadas | 200 |
| `GET /v1/alertas/{id}` | Detalle de una alerta | 200, 404 |
| `GET /v1/cache` · `DELETE /v1/cache` | Estadísticas de la caché / vaciarla (para medir sin caché) | 200 / 204 |

Criterio de códigos: 404 cuando el recurso no existe en la dependencia (tipo de Pokémon o sensor), 422 para parámetros o cuerpos inválidos (los valida FastAPI), 502 cuando la dependencia responde con error o se agotan los reintentos, 504 cuando el último fallo fue un timeout. Nunca se devuelve un 500 por culpa de una dependencia. Todas las respuestas llevan la cabecera `X-Tiempo-Respuesta-ms`.

## Resiliencia

Toda llamada saliente pasa por `fetch_resiliente()` en `main.py`:

- Timeout explícito de `TIMEOUT_SEGUNDOS` en cada llamada.
- Reintentos selectivos: se reintentan timeouts, errores de conexión y los códigos 429, 502, 503 y 504. Los 400 y 404 se devuelven de inmediato, sin reintentar.
- Backoff exponencial con jitter ("equal jitter"): en el intento *n* el techo es `min(8, 0,5·2ⁿ⁻¹)` segundos y se espera la mitad fija más una mitad aleatoria. Esperas aproximadas: 0,25–0,5 s, 0,5–1 s, 1–2 s, 2–4 s.
- Respeto de `Retry-After`: ante un 429 se espera al menos lo indicado (más un pequeño jitter). Ese plazo se guarda por host en `no_llamar_antes_de`, de modo que cualquier petición posterior al mismo servicio espera también, no solo la que recibió el 429. Esto es necesario porque el servicio inestable bloquea por IP.
- Paginación completa: se sigue el cursor `siguiente` hasta que es `null`, con protección ante cursores repetidos.
- Número máximo de intentos: al agotarlos se responde 502 o 504 con un detalle claro.
- En `/v1/equipos/{tipo}/resumen` PokeAPI y el servicio inestable se consultan en paralelo; si una falla, la otra tarea se cancela.

## Evidencias de los reintentos

En `evidencias/` hay dos registros de una misma ejecución contra el servicio inestable con valores por defecto y `--semilla 42`: cinco `GET /v1/sensores` seguidos, un `GET /v1/sensores/S-999/lectura` y tres `GET /v1/sensores` simultáneos.

- `log_agregador_semilla42.txt` muestra cada intento fallido, el motivo y la espera elegida. Por ejemplo:
  ```
  WARNING [Servicio inestable /sensores] intento 1/5 falló: HTTP 503 -> esperando 0.29 s
  WARNING [Servicio inestable /sensores] intento 1/5 falló: HTTP 429 (Retry-After: 2) -> esperando 2.20 s
  INFO    [Servicio inestable /sensores] intento 2/5 -> 200 (recuperado)
  INFO    [Servicio inestable /sensores] Retry-After vigente para 127.0.0.1:9000: esperando 1.80 s antes de llamar
  ERROR   [Servicio inestable /sensores] se agotaron los 5 intentos (último: HTTP 503)
  INFO    GET /v1/sensores/S-999/lectura -> 404 (4.6 ms)
  ```
- `log_servicio_inestable_semilla42.txt` es la salida del propio servicio. En la parte secuencial no aparece ningún `429 (no respetó Retry-After)`. En la parte de peticiones simultáneas aparecen algunos: son peticiones que ya estaban en vuelo (1 ms después) cuando el servicio emitió el 429, así que era imposible que el cliente lo conociera. Después del primer 429, todas las peticiones esperan el plazo pendiente.
- El 404 del sensor inexistente aparece una sola vez: no se reintenta.

## Latencia con caché y sin ella

Se mide con `medir_latencia.py`. "Sin caché" vacía la caché antes de cada petición; "con caché" la calienta una vez. Se informa aparte la parte de PokeAPI (`latencias_ms.pokeapi` en la respuesta), porque el tiempo total incluye el servicio inestable, cuyos fallos aleatorios ocultan el efecto de la caché. Para aislarlo, el servicio inestable se arrancó sin fallos:

```bash
python servicio_inestable.py --tasa-fallos 0 --tasa-429 0 --tasa-lentitud 0
python medir_latencia.py --n 10 --tipo fire
```

| Escenario | Media (ms) | Mediana (ms) | Mín (ms) | Máx (ms) | n |
|---|---|---|---|---|---|
| Sin caché – parte PokeAPI | 222.3 | 173.1 | 153.9 | 534.2 | 10 |
| Con caché – parte PokeAPI | 3.0 | 2.8 | 2.4 | 4.3 | 10 |
| Sin caché – total endpoint | 238.7 | 182.1 | 157.2 | 551.9 | 10 |
| Con caché – total endpoint | 65.0 | 64.0 | 58.9 | 82.2 | 10 |

Con caché, la parte de PokeAPI baja a alrededor de 3 ms (lectura de un diccionario) y se evitan 1 + `limite` peticiones a PokeAPI por cada consulta.

## Demostración de idempotencia

Peticiones 14 a 18 de `pruebas.http`:

```bash
curl -i -X POST http://127.0.0.1:8000/v1/alertas \
  -H "Content-Type: application/json" -H "Idempotency-Key: prueba-idem-001" \
  -d '{"nivel":"critico","mensaje":"Fallo en el servidor de sensores","sensor_id":"S-001"}'
```

Resultado (dos envíos idénticos):

```
HTTP/1.1 201 Created
idempotent-replayed: false
{"id_alerta":"ALT-854CF71E","estado":"creada","creada_en":"2026-10-26T00:22:36+00:00", ...}

HTTP/1.1 201 Created
idempotent-replayed: true
{"id_alerta":"ALT-854CF71E","estado":"creada","creada_en":"2026-10-26T00:22:36+00:00", ...}
```

Las dos respuestas tienen la misma `id_alerta` y `GET /v1/alertas` devuelve una sola alerta. Si se reutiliza la clave con un cuerpo distinto, la respuesta es 422, para que un error del cliente no quede oculto devolviendo una alerta que no corresponde. Sin la cabecera también se responde 422.

## Preguntas de análisis

### a) ¿Por qué es peligroso reintentar un POST sin clave de idempotencia?
Cuando un POST termina en timeout o se corta la conexión, el cliente no sabe si el servidor lo procesó: la petición pudo perderse a la ida, o pudo ejecutarse y perderse la respuesta a la vuelta. Un POST no es idempotente, así que si el cliente reintenta en el segundo caso la operación se ejecuta dos veces: dos alertas, dos cobros, dos pedidos. Con `Idempotency-Key` el servidor reconoce el reintento y devuelve el resultado guardado en lugar de repetir el efecto. Eso convierte el "al menos una vez" de los reintentos en "exactamente una vez" en cuanto al efecto.

### b) ¿Qué ocurriría si 50 clientes reintentaran al mismo tiempo sin jitter?
Si todos fallan a la vez (por ejemplo, porque el servicio se reinició), todos calculan las mismas esperas (1 s, 2 s, 4 s...) y vuelven a llegar juntos, en ráfagas sincronizadas. Es el efecto de "manada atronadora" (*thundering herd*). Cada ráfaga de 50 peticiones simultáneas puede volver a tumbar o saturar el servicio justo cuando intenta recuperarse, generar más 503 y 429, y provocar otra ráfaga sincronizada. El backoff solo separa las ráfagas en el tiempo, pero no las deshace. El jitter reparte los reintentos de forma aleatoria dentro de cada ventana, de modo que la carga llega como un flujo más uniforme que el servicio sí puede atender. Por eso `calcular_backoff()` combina las dos cosas.

### c) ¿Qué tan desactualizados pueden estar los datos de la caché y cuándo sería inaceptable?
Como máximo `CACHE_TTL_SEGUNDOS` (60 s por defecto): una entrada guardada justo después de un cambio en PokeAPI se sigue sirviendo hasta que vence. Para PokeAPI eso es perfectamente aceptable, porque sus datos (tipos, peso, experiencia base) cambian casi nunca; incluso se podría subir el TTL a horas. Sería inaceptable cachear así datos que deben ser frescos o cuya obsolescencia tiene consecuencias. Es el caso de las lecturas de sensores en tiempo real (por eso no se cachean): una alarma de CO₂ basada en un valor de hace un minuto podría llegar tarde. Lo mismo vale para saldos, inventario o precios en una compra, permisos o estados de autorización (un acceso revocado que sigue vigente en caché), y en general cualquier dato sobre el que se tome una decisión que no se puede deshacer.

### d) ¿Qué cambiaría si el servicio corriera en varias réplicas detrás de un balanceador?
- Idempotencia: el diccionario `idempotencia` vive en la memoria de cada proceso. Un reintento que el balanceador envíe a otra réplica no encontraría la clave y crearía una alerta duplicada. Habría que guardar las claves en un almacén compartido (Redis o una base de datos) con una operación atómica (`SET NX` o una restricción `UNIQUE` sobre la clave) y un TTL. También habría que contemplar el caso de dos réplicas procesando la misma clave a la vez, marcándola como "en proceso". Hoy eso no es un problema porque, en un solo proceso asíncrono, entre la comprobación y el guardado no hay ningún `await`.
- Alertas: también deberían ir a una base de datos compartida; si no, `GET /v1/alertas` devolvería resultados distintos según la réplica.
- Caché: cada réplica tendría su propia caché, con menos aciertos y más llamadas a PokeAPI. Se podría aceptar (son datos inmutables), o pasar a una caché compartida (Redis) para que la carga sobre PokeAPI no crezca con el número de réplicas.
- Retry-After y límites: el bloqueo `no_llamar_antes_de` también es local. Si todas las réplicas salen por la misma IP, una puede respetar el 429 mientras otra sigue llamando y prolonga el bloqueo. Haría falta coordinarlo (estado compartido o un limitador de tasa central), o asumir que cada réplica lo aprenderá con su propio 429.
- Reintentos en cascada: con más réplicas hay más clientes del servicio inestable, así que el jitter importa todavía más. También convendría un presupuesto de reintentos o un *circuit breaker* para no amplificar la carga cuando la dependencia está caída.
- `/salud` le sirve al balanceador para sacar de rotación las réplicas que no responden.

## Uso de inteligencia artificial

Herramientas usadas: Claude.

Usamos IA de forma amplia en este trabajo: cuando encontrábamos callejones sin salida o múltiples errores que no comprendíamos, le pedíamos ayuda para escribir el código del agregador (main.py) , el script de medición, la colección de pruebas y para depurar errores.

Lo que hicimos nosotros: Programamos hasta donde pudimos y entendíamos, leímos y revisamos el código generado, levantamos los tres servicios en nuestros equipos, ejecutamos la colección de pruebas, medimos la latencia con y sin caché con datos reales de PokeAPI y comprobamos que la idempotencia funciona, y realizamos y escribimos el análisis y respondimos las preguntas.

## Notas

- `servicio_inestable.py` es el archivo del docente del curso. No se publica en este repositorio; para ejecutar el agregador contra él hay que obtenerlo del curso y ponerlo junto a `main.py`. Las pruebas automáticas (`pytest`) no lo necesitan.
- No incluir en el comprimido de entrega las carpetas `venv/` ni `__pycache__/` (archivos `.pyc`): son generadas automáticamente y dependen de la versión de Python de cada equipo.
