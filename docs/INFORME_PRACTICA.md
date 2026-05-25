# Informe de la Práctica DataOps

## Master en Ciberseguridad e Inteligencia de Datos — Asignatura: Visualización

---

## 1. Objetivo de la práctica

El objetivo es construir un pipeline de visualización de datos sobre la **distribución de rentas en Canarias**, aplicando los principios de **DataOps** con la herramienta **Dagster** y la librería de visualización **plotnine**. El pipeline integra tres fuentes de datos, genera gráficos mediante la gramática de gráficos de Wickham, incorpora un modelo LLM para generación automática de código de visualización, valida la calidad de los datos en cada capa y publica los resultados automáticamente en GitHub Pages.

---

## 2. Fuentes de datos

| Archivo                           | Descripción                                                                                                                   |
| --------------------------------- | ----------------------------------------------------------------------------------------------------------------------------- |
| `distribucion-renta-canarias.csv` | Dataset principal con ingresos por municipio, isla, año (2015–2023) y tipo de medida (sueldos, pensiones, prestaciones, etc.) |
| `codislas.csv`                    | Tabla de municipios de Canarias con códigos INE, nombre normalizado e isla asignada                                           |
| `nivelestudios.xlsx`              | Datos de nivel de estudios por municipio, sexo y año (2021–2023), fuente INE                                                  |

---

## 3. Arquitectura del pipeline

El pipeline se implementa en Dagster como un grafo de **assets** organizado en cinco capas:

```
CAPA 1: Extracción
  cargar_dataset_renta
  cargar_codigos_municipios
  cargar_nivel_estudios

CAPA 2: Transformación / Enriquecimiento
  dataset_renta_con_municipios   ← merge renta + municipios
  dataset_renta_con_estudios     ← merge enriquecido + estudios
  dataset_estudios_limpio        ← estudios preparados de forma independiente
  dataset_renta_limpio           ← dataset final limpio y tipado

CAPA 3: Visualización
  grafico_distribucion_ingressos ← barras: distribución de medidas, último año
  grafico_tendencia_total        ← línea + punto: evolución ingresos 2015–2023
  grafico_ingresos_por_isla      ← barras apiladas: ingresos por isla y medida
  grafico_nivel_estudios_distribucion ← barras: niveles educativos en Canarias

CAPA 4: Guardado
  guardar_graficos_resumen       ← guarda los 4 PNG en graficos_salida_pipeline/

CAPA IA: Generación con LLM
  islas_raw                      ← agrega renta media por isla y año
  template_ia                    ← construye el prompt para el LLM
  codigo_generado_ia             ← llama a litellm / ollama/llama3.1:8b
  codigo_limpio_ia               ← limpia, valida y envuelve el código generado
  visualizacion_png              ← ejecuta el código y guarda visualizacion_ia.png

CAPA 5: Publicación
  publicar_en_ghpages            ← sube visualizacion_ia.png a la rama gh-pages
```

### Diagrama de dependencias simplificado

```
cargar_dataset_renta ─┐
                      ├── dataset_renta_con_municipios ─┐
cargar_codigos ───────┘                                  ├── dataset_renta_con_estudios
                      ┌──────────────────────────────────┘
cargar_nivel_estudios ┘ └── dataset_estudios_limpio

dataset_renta_con_estudios ─── dataset_renta_limpio ─────┬── grafico_distribucion_ingressos ─┐
                                                         ├── grafico_tendencia_total          ├── guardar_graficos_resumen
                                                         └── grafico_ingresos_por_isla ───────┘
dataset_estudios_limpio ─── grafico_nivel_estudios_distribucion ──────────────────────────────┘

dataset_renta_limpio ─── islas_raw ── template_ia ── codigo_generado_ia ── codigo_limpio_ia ── visualizacion_png ── publicar_en_ghpages
```

---

## 4. Normalización de datos

### 4.1 Normalización de nombres geográficos invertidos

El fichero `codislas.csv` presenta nombres en formato invertido (p.ej. `"Palma, La"`, `"Gomera, La"`), convención del INE. Se implementa la función `normalizar_nombre_invertido()` en tres assets independientes que la necesitan (`cargar_codigos_municipios`, `cargar_nivel_estudios`, `dataset_estudios_limpio`):

```python
def normalizar_nombre_invertido(nombre_str):
    nombre_str = nombre_str.strip()
    if ',' in nombre_str:
        partes = [p.strip() for p in nombre_str.split(',')]
        if len(partes) == 2:
            return f"{partes[1]} {partes[0]}"
    return nombre_str
```

### 4.2 Estrategia de merge en dos fases

El enriquecimiento del dataset de rentas con información geográfica se hace en dos pasadas:

1. **Merge por nombre de municipio** (cubre la mayoría de registros detallados)
2. **Merge por nombre de isla normalizado** (cubre registros agregados a nivel insular)

Esto garantiza que tanto los registros municipales como los totales insulares queden georreferenciados.

---

## 5. Visualizaciones implementadas

Todos los gráficos siguen la **gramática de gráficos de Wickham** using `plotnine`, que descompone cada visualización en: _dataset_, _estéticas (aes)_, _geometría_, _escalas_ y _etiquetas_.

### Gráfico 1 — Distribución de fuentes de ingreso (último año)

- **Geometría:** `geom_bar(stat='identity')`
- **Estéticas:** `x = MEDIDAS#es`, `y = OBS_VALUE`, `fill = MEDIDAS#es`
- **Decisión de diseño:** Barras verticales con etiquetas rotadas 45° para legibilidad. Sin leyenda (redundante con el eje X). Paleta de colores por defecto de plotnine para diferenciación categórica.

### Gráfico 2 — Tendencia temporal de ingresos (2015–2023)

- **Geometría:** `geom_line() + geom_point()`
- **Estéticas:** `x = Año`, `y = Total_Ingresos`
- **Decisión de diseño:** Combinación línea + punto para enfatizar la tendencia continua y los valores discretos anuales. Color verde `#2ca02c` proporciona contraste suficiente con fondo blanco.

### Gráfico 3 — Ingresos por isla y medida

- **Geometría:** `geom_bar(stat='identity')` (barras apiladas por defecto)
- **Estéticas:** `x = ISLA_FINAL`, `y = OBS_VALUE`, `fill = MEDIDAS#es`
- **Decisión de diseño:** Las barras apiladas permiten comparar el total insular y la composición por medida simultáneamente. Leyenda a la derecha para no comprimir el área del gráfico.

### Gráfico 4 — Distribución de nivel de estudios

- **Geometría:** `geom_bar(stat='identity')`
- **Estéticas:** `x = Nivel_Corto`, `y = Total`, `fill = Nivel_Corto`
- **Decisión de diseño:** Se crean etiquetas cortas para los niveles educativos (p.ej. `"Formación Profesional"` en lugar del literal completo del INE) para evitar solapamiento de texto. Sin leyenda porque cada barra ya está etiquetada en el eje X.

---

## 6. Checks de calidad de datos

Se implementa un sistema de **asset checks** en `checks_pipeline.py` organizado en cuatro capas que reflejan la arquitectura del pipeline. Dagster ejecuta estos checks automáticamente tras materializar cada asset.

| Capa               | Asset                          | Checks implementados                                                                          |
| ------------------ | ------------------------------ | --------------------------------------------------------------------------------------------- |
| **Extracción**     | `cargar_dataset_renta`         | Schema obligatorio, no vacío, años 2015–2023 presentes, medidas conocidas                     |
| **Extracción**     | `cargar_codigos_municipios`    | Schema obligatorio, no vacío                                                                  |
| **Extracción**     | `cargar_nivel_estudios`        | Schema obligatorio, no vacío, períodos ≥ 2019                                                 |
| **Transformación** | `dataset_renta_con_municipios` | Las 7 islas canarias representadas                                                            |
| **Transformación** | `dataset_renta_limpio`         | No vacío tras filtrado, `OBS_VALUE` no negativo, tipos correctos                              |
| **Transformación** | `dataset_renta_con_estudios`   | Columnas de joined correctas, `Total_Estudiantes` positivo                                    |
| **Visualización**  | `dataset_renta_limpio`         | ≥ 2 medidas disponibles para gráfico 1, ≥ 3 años para gráfico 2, ≥ 2 islas para gráfico 3     |
| **Visualización**  | `dataset_estudios_limpio`      | ≥ 3 niveles educativos para gráfico 4                                                         |
| **IA**             | `islas_raw`                    | Schema `['isla','año','valor']`, no vacío, 7 islas presentes, valores positivos               |
| **IA**             | `template_ia`                  | `model` no vacío, roles `system`/`user` presentes, variables `isla`/`año`/`valor` mencionadas |
| **IA**             | `codigo_generado_ia`           | Respuesta no vacía, contiene palabras clave Python (`import`, `ggplot`, `grafico`)            |
| **IA**             | `codigo_limpio_ia`             | Contiene `def generar_plot(df):`, variable `grafico` presente, sintaxis Python válida         |

Los checks de capa 1–3 usan severidad `ERROR` para condiciones bloqueantes y `WARN` para advertencias no bloqueantes.

### Tests negativos (pytest)

En `test_checks_pipeline.py` se implementan **tests negativos**: cada test construye un DataFrame sintético que viola deliberadamente la condición validada, verificando que el check devuelve `passed=False`. Esto garantiza que los checks son efectivamente sensibles a anomalías en los datos.

---

## 7. Pipeline de IA generativa

### 7.1 Arquitectura

El componente de IA genera automáticamente código Python de visualización usando un LLM local (ollama/llama3.1:8b via litellm). El flujo es:

```
islas_raw → template_ia → codigo_generado_ia → codigo_limpio_ia → visualizacion_png
```

### 7.2 Justificación del diseño del prompt (template_ia)

El diseño del prompt es un elemento central de la calidad del pipeline de IA. A continuación se justifican las decisiones tomadas:

#### Mensaje de sistema (instrucciones de rol)

```python
"Eres un experto en visualización de datos con Python y la librería plotnine. "
"Tu única tarea es generar código Python ejecutable. "
"Responde ÚNICAMENTE con código Python puro. ..."
```

**Justificación:** Definir el rol como "experto en visualización" orienta al modelo a producir código idiomático de plotnine en lugar de código genérico de pandas o matplotlib. La instrucción de responder solo con código Python puro elimina texto adicional que requeriría parsing posterior y es fuente habitual de errores en pipelines de IA.

#### Prohibición explícita de crear DataFrames

```python
"1. NUNCA crees un DataFrame, ni con pd.DataFrame(), ni con diccionarios,
    ni con ningún otro método. El DataFrame ya existe en la variable `df`..."
```

**Justificación:** Los LLMs tienden a generar datos de ejemplo como parte del código cuando el prompt menciona columnas o datos. Esta prohibición es necesaria porque el DataFrame real (`islas_raw`) ya es pasado al entorno de ejecución. Sin esta instrucción, el modelo genera un `df = pd.DataFrame(...)` con datos inventados, lo que hace fallar la visualización con datos incorrectos.

#### Inyección de datos reales en el prompt

```python
muestra_df = islas_raw.head(10).to_string(index=False)
```

**Justificación:** Mostrar una muestra real del DataFrame tiene dos efectos: (1) el modelo conoce los valores y tipos reales de las columnas, lo que reduce errores de tipo (p.ej. usar `'año'` en lugar de `'Año'`); (2) reduce la tentación del modelo de inventar datos propios porque ya "ve" que los datos existen. Es una técnica de _grounding_ del prompt.

#### Especificación precisa de la variable de salida

```python
"2. La variable del gráfico DEBE llamarse exactamente `grafico` (sin tilde, sin acento)."
```

**Justificación:** El asset `visualizacion_png` recupera el gráfico accediendo a `entorno_ejecucion['grafico']`. Un nombre diferente (`gráfico` con tilde, `plot`, `p`, etc.) causa un `KeyError` que tumba el pipeline. La instrucción explícita, reforzada por el proceso de limpieza (`código_limpio_ia` normaliza `gráfico` → `grafico`), garantiza la interoperabilidad entre el código generado y el entorno de ejecución controlado.

#### Operacionalización de la gramática de gráficos

El prompt estructura la solicitud siguiendo exactamente los cinco componentes de la gramática de gráficos de Wickham:

```python
"ESTÉTICAS (aes): Eje X → año, Eje Y → valor, Color → isla"
"GEOMETRÍA: geom_line()"
"ESCALA DE COLOR: scale_color_manual con el siguiente diccionario..."
"TEMA: theme_minimal()"
"ETIQUETAS: Título, Eje X, Eje Y"
```

**Justificación:** Estructurar el prompt siguiendo la gramática de gráficos cumple dos objetivos: (1) el modelo produce código más correcto porque los términos coinciden directamente con los argumentos de plotnine (`aes`, `geom_line`, `scale_color_manual`, `theme_minimal`, `labs`); (2) hace el prompt más fácil de mantener porque cualquier cambio en el diseño se localiza en la sección correspondiente de la gramática, sin necesidad de reformular todo el prompt.

#### Técnica de resaltado (spotlight): Tenerife en naranja

```python
color_map = {isla: '#D3D3D3' for isla in islas}
color_map['Tenerife'] = '#FF8C00'
```

**Justificación:** Esta técnica, conocida como _spotlight_ en visualización de datos, reduce la carga cognitiva del lector: todas las series irrelevantes se neutralizan al mismo color de fondo (gris claro) y la serie de interés (Tenerife, la isla más grande) se destaca en un color saturado. Es una práctica recomendable de diseño de información según Edward Tufte y Cairo (principio de "relación señal/ruido").

### 7.3 Sistema de limpieza y fallback

El asset `codigo_limpio_ia` implementa un sistema defensivo de saneamiento del output del LLM:

| Problema detectado                                                                         | Solución                                                             |
| ------------------------------------------------------------------------------------------ | -------------------------------------------------------------------- |
| Bloques markdown (` ```python `)                                                           | Extracción con regex                                                 |
| Comillas tipográficas (`'`, `"`, `"`)                                                      | Tabla de sustitución Unicode → ASCII                                 |
| `from plotnine import *` dentro de función                                                 | Eliminación (prohibido en funciones)                                 |
| `gráfico` con tilde                                                                        | Renombrado a `grafico`                                               |
| `df = pd.DataFrame(...)` / `df = pd.read_csv(...)` / `df = ...` / cualquier `df = <no-df>` | Eliminación con negative lookahead regex: `r'^\s*df\s*=\s*(?!df\b)'` |
| `SyntaxError` en el código generado                                                        | Activación del fallback hardcodeado                                  |

El **fallback** es una implementación hardcodeada del mismo gráfico que se activa automáticamente si el código del LLM no pasa la validación de sintaxis. Dagster registra un `context.log.warning()` y el metadato `fallback_activado: True` queda visible en el lineage, garantizando trazabilidad sin interrumpir el pipeline.

---

## 8. Publicación automática en GitHub Pages (publicar_en_ghpages)

### 8.1 Descripción

El asset `publicar_en_ghpages` es el último eslabón del pipeline. Toma la ruta de `visualizacion_ia.png` devuelta por `visualizacion_png` y la publica en la rama `gh-pages` del repositorio, generando un `index.html` que la muestra.

### 8.2 Implementación técnica

Se usa `git worktree` para trabajar sobre la rama `gh-pages` en un directorio temporal sin tocar la rama de trabajo actual (`main`):

```python
# Primera ejecución → rama orphan sin historial compartido
git worktree add --orphan -b gh-pages <tmpdir>

# Ejecuciones siguientes → checkout de la rama existente
git worktree add <tmpdir> gh-pages
```

Esto evita contaminar el historial de `main` con archivos binarios (PNG) y mantiene `gh-pages` como una rama dedicada únicamente a los artefactos de publicación.

### 8.3 Diseño del index.html

Se genera un HTML mínimo, semánticamente correcto, con:

- Viewport responsivo para visualización en móvil
- Timestamp de última actualización automático
- Descripción del origen del gráfico (pipeline DataOps con IA)

### 8.4 Seguridad y robustez

- Si el push falla (credenciales, red, etc.), se emite `context.log.warning()` sin propagar la excepción. El pipeline no se interrumpe.
- El worktree siempre se limpia en el bloque `finally`, evitando estados inconsistentes en el repositorio git local.
- Los comandos subprocess usan listas (no strings), evitando la vulnerabilidad de inyección de comandos shell (OWASP A03).

**URL pública:** `https://MacyFonseca.github.io/practica_dataops/`

---

## 9. Sensor de cambio en datos (sensor_cambio_datos)

### 9.1 Descripción

Se implementa un **sensor** de Dagster que monitoriza continuamente los tres archivos de datos de entrada. Cuando detecta que alguno ha sido modificado (o reemplazado por datos nuevos), lanza automáticamente el job `pipeline_renta_canarias`.

### 9.2 Implementación

```python
@sensor(job=renta_canarias_job, minimum_interval_seconds=30)
def sensor_cambio_datos(context):
    cursor_actual = json.loads(context.cursor or "{}")
    nuevo_cursor = {}
    hay_cambios = False

    for ruta in _ARCHIVOS_VIGILADOS:
        mtime = os.path.getmtime(ruta)
        nuevo_cursor[str(ruta)] = mtime
        if cursor_actual.get(str(ruta)) != mtime:
            hay_cambios = True
    ...
```

### 9.3 Mecanismo del cursor

El cursor de Dagster actúa como memoria persistente del sensor entre evaluaciones. Almacena el `mtime` (fecha de última modificación) de cada archivo en formato JSON. En cada evaluación (cada 30 s):

- Si algún `mtime` difiere → actualiza el cursor + `yield RunRequest(run_key=...)`
- Si nada cambió → `yield SkipReason("Sin cambios en los archivos de datos")`

### 9.4 Archivos vigilados

| Archivo                           | Justificación                                                                                                                     |
| --------------------------------- | --------------------------------------------------------------------------------------------------------------------------------- |
| `distribucion-renta-canarias.csv` | Dataset principal; cualquier actualización de datos históricos o incorporación de un nuevo año debe relanzar el análisis completo |
| `codislas.csv`                    | Tabla de referencia geográfica; un cambio puede afectar el enriquecimiento de todos los registros                                 |
| `nivelestudios.xlsx`              | Datos de educación; nuevos períodos disponibles requieren regenerar los gráficos de estudios                                      |

### 9.5 Activación

Para activar el sensor: en la UI de Dagster (pestaña **Automation** → **Sensors**), hacer clic en el toggle de `sensor_cambio_datos`. Una vez activo, cualquier guardado de los ficheros de datos dispara el pipeline automáticamente.

---

## 10. Tests negativos con pytest

El fichero `test_checks_pipeline.py` contiene tests negativos de todos los checks de calidad de las capas 1–3. La estrategia es:

1. Construir un DataFrame sintético **válido** con la función helper correspondiente
2. Aplicar una modificación que **viola** la condición del check
3. Afirmar que `result.passed == False`

Ejemplo:

```python
def test_check_anios_renta_falla_si_faltan_anios():
    df = _df_renta_valido()
    df_incompleto = df[df['TIME_PERIOD#es'] != 2015]  # eliminar el año 2015
    result = check_anios_renta(df_incompleto)
    assert result.passed == False
```

Los checks se invocan directamente como funciones Python puras (sin levantar Dagster), lo que hace los tests rápidos y sin dependencias externas.

---

## 11. Estructura de ficheros del proyecto

```
practica_dataops/
├── assets_renta_canarias.py      # Todos los assets del pipeline (5 capas)
├── checks_pipeline.py             # Asset checks de calidad por capas
├── dagster_definitions.py         # Definiciones Dagster: job, sensor, Definitions
├── test_checks_pipeline.py        # Tests negativos con pytest
├── distribucion-renta-canarias.csv
├── codislas.csv
├── nivelestudios.xlsx
├── requirements.txt
├── setup.sh
├── README.md
├── graficos_salida_pipeline/      # PNGs generados por el pipeline
│   ├── 01_distribucion_ingressos.png
│   ├── 02_tendencia_ingressos.png
│   ├── 03_ingresos_por_isla.png
│   ├── 04_nivel_estudios_distribucion.png
│   └── visualizacion_ia.png
├── graficos_salida_lab/           # PNGs del laboratorio exploratorio
└── extras_pipeline/               # Assets experimentales y análisis adicional
```

---

## 12. Dependencias

```
dagster==1.5.0
dagster-webserver==1.5.0
pandas==2.0.3
plotnine==0.14.1
openpyxl>=3.0.0
pytest>=7.0
litellm>=1.0.0
```

El modelo LLM requiere **Ollama** instalado localmente con el modelo `llama3.1:8b` descargado (`ollama pull llama3.1:8b`).

---

## 13. Instrucciones de ejecución

```bash
# 1. Crear y activar entorno virtual
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt

# 2. Lanzar el pipeline completo con UI
dagster dev -f dagster_definitions.py --port 3000
# → Abrir http://127.0.0.1:3000

# 3. Solo los checks (sin UI)
dagster asset check --select "*"

# 4. Tests negativos
python -m pytest test_checks_pipeline.py -v
```

Para publicar en GitHub Pages, configurar en el repositorio: **Settings → Pages → Branch: gh-pages / (root)**.
