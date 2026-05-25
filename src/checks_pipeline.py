"""
Checks de calidad de datos para el pipeline de distribución de rentas en Canarias.
Organizados en cuatro capas:
  - Capa 1: Extracción  — validaciones sobre los datos cargados desde ficheros
  - Capa 2: Transformación — validaciones sobre los datasets enriquecidos y limpios
  - Capa 3: Visualización — validaciones previas a la generación de gráficos
  - Capa 4: IA — validaciones sobre los assets del pipeline de IA generativa
"""

import pandas as pd
from dagster import asset_check, AssetCheckResult, AssetCheckSeverity


# ===========================================================================
# CAPA 1: EXTRACCIÓN
# ===========================================================================

# --- cargar_dataset_renta ---

@asset_check(asset="cargar_dataset_renta", description="Comprueba que las columnas obligatorias están presentes")
def check_schema_renta(cargar_dataset_renta: pd.DataFrame) -> AssetCheckResult:
    columnas_esperadas = {
        'TIME_PERIOD#es', 'MEDIDAS#es', 'OBS_VALUE',
        'TERRITORIO#es', 'CONFIDENCIALIDAD_OBSERVACION#es'
    }
    columnas_presentes = set(cargar_dataset_renta.columns)
    faltantes = columnas_esperadas - columnas_presentes
    passed = len(faltantes) == 0
    return AssetCheckResult(
        passed=passed,
        severity=AssetCheckSeverity.ERROR,
        metadata={
            "columnas_faltantes": str(sorted(faltantes)) if faltantes else "ninguna",
            "columnas_totales": len(columnas_presentes),
        },
    )


@asset_check(asset="cargar_dataset_renta", description="Comprueba que el dataset no está vacío")
def check_no_vacio_renta(cargar_dataset_renta: pd.DataFrame) -> AssetCheckResult:
    n_filas = len(cargar_dataset_renta)
    passed = n_filas > 0
    return AssetCheckResult(
        passed=passed,
        severity=AssetCheckSeverity.ERROR,
        metadata={"n_filas": n_filas},
    )


@asset_check(asset="cargar_dataset_renta", description="Comprueba que los años esperados (2015–2023) están presentes")
def check_anios_renta(cargar_dataset_renta: pd.DataFrame) -> AssetCheckResult:
    anios_esperados = set(range(2015, 2024))
    anios_presentes = set(cargar_dataset_renta['TIME_PERIOD#es'].dropna().astype(int).unique())
    anios_faltantes = anios_esperados - anios_presentes
    passed = len(anios_faltantes) == 0
    return AssetCheckResult(
        passed=passed,
        severity=AssetCheckSeverity.WARN,
        metadata={
            "anios_faltantes": str(sorted(anios_faltantes)) if anios_faltantes else "ninguno",
            "anios_presentes": str(sorted(anios_presentes)),
        },
    )


@asset_check(asset="cargar_dataset_renta", description="Comprueba que las medidas de ingreso conocidas están presentes")
def check_medidas_renta(cargar_dataset_renta: pd.DataFrame) -> AssetCheckResult:
    medidas_esperadas = {"Sueldos y salarios", "Pensiones"}
    medidas_presentes = set(cargar_dataset_renta['MEDIDAS#es'].dropna().unique())
    faltantes = medidas_esperadas - medidas_presentes
    passed = len(faltantes) == 0
    return AssetCheckResult(
        passed=passed,
        severity=AssetCheckSeverity.WARN,
        metadata={
            "medidas_faltantes": str(sorted(faltantes)) if faltantes else "ninguna",
            "medidas_disponibles": str(sorted(medidas_presentes)),
        },
    )


# --- cargar_codigos_municipios ---

@asset_check(asset="cargar_codigos_municipios", description="Comprueba que las columnas clave del CSV de municipios están presentes")
def check_schema_municipios(cargar_codigos_municipios: pd.DataFrame) -> AssetCheckResult:
    columnas_esperadas = {'NOMBRE', 'ISLA', 'CMUN', 'CISLA', 'NOMBRE_NORMALIZADO', 'ISLA_NORMALIZADO'}
    columnas_presentes = set(cargar_codigos_municipios.columns)
    faltantes = columnas_esperadas - columnas_presentes
    passed = len(faltantes) == 0
    return AssetCheckResult(
        passed=passed,
        severity=AssetCheckSeverity.ERROR,
        metadata={
            "columnas_faltantes": str(sorted(faltantes)) if faltantes else "ninguna",
        },
    )


@asset_check(asset="cargar_codigos_municipios", description="Comprueba que el fichero de municipios no está vacío")
def check_no_vacio_municipios(cargar_codigos_municipios: pd.DataFrame) -> AssetCheckResult:
    n_filas = len(cargar_codigos_municipios)
    passed = n_filas > 0
    return AssetCheckResult(
        passed=passed,
        severity=AssetCheckSeverity.ERROR,
        metadata={"n_filas": n_filas},
    )


# --- cargar_nivel_estudios ---

@asset_check(asset="cargar_nivel_estudios", description="Comprueba que las columnas clave del XLSX de estudios están presentes")
def check_schema_estudios(cargar_nivel_estudios: pd.DataFrame) -> AssetCheckResult:
    columnas_esperadas = {
        'Año_Estudios', 'Nivel de estudios en curso', 'Sexo', 'Total',
        'CMUN_EST', 'MUNICIPIO_EST'
    }
    columnas_presentes = set(cargar_nivel_estudios.columns)
    faltantes = columnas_esperadas - columnas_presentes
    passed = len(faltantes) == 0
    return AssetCheckResult(
        passed=passed,
        severity=AssetCheckSeverity.ERROR,
        metadata={
            "columnas_faltantes": str(sorted(faltantes)) if faltantes else "ninguna",
        },
    )


@asset_check(asset="cargar_nivel_estudios", description="Comprueba que el fichero de estudios no está vacío")
def check_no_vacio_estudios(cargar_nivel_estudios: pd.DataFrame) -> AssetCheckResult:
    n_filas = len(cargar_nivel_estudios)
    passed = n_filas > 0
    return AssetCheckResult(
        passed=passed,
        severity=AssetCheckSeverity.ERROR,
        metadata={"n_filas": n_filas},
    )


@asset_check(asset="cargar_nivel_estudios", description="Comprueba que los períodos de estudios están en un rango válido (≥ 2019)")
def check_periodos_estudios(cargar_nivel_estudios: pd.DataFrame) -> AssetCheckResult:
    anios = cargar_nivel_estudios['Año_Estudios'].dropna().astype(int)
    anio_min = int(anios.min()) if len(anios) > 0 else None
    anio_max = int(anios.max()) if len(anios) > 0 else None
    passed = anio_min is not None and anio_min >= 2019
    return AssetCheckResult(
        passed=passed,
        severity=AssetCheckSeverity.WARN,
        metadata={
            "anio_minimo": anio_min,
            "anio_maximo": anio_max,
            "umbrral_minimo_esperado": 2019,
        },
    )


# ===========================================================================
# CAPA 2: TRANSFORMACIÓN
# ===========================================================================

# --- dataset_renta_con_municipios ---

@asset_check(
    asset="dataset_renta_con_municipios",
    description="Comprueba que las 7 islas principales de Canarias están representadas"
)
def check_islas_canarias(dataset_renta_con_municipios: pd.DataFrame) -> AssetCheckResult:
    islas_esperadas = {
        "Tenerife", "Gran Canaria", "La Palma", "La Gomera",
        "El Hierro", "Lanzarote", "Fuerteventura"
    }
    islas_presentes = set(
        dataset_renta_con_municipios['ISLA_FINAL'].dropna().str.strip().unique()
    )
    # Comprobación insensible a mayúsculas
    islas_presentes_lower = {i.lower() for i in islas_presentes}
    faltantes = {i for i in islas_esperadas if i.lower() not in islas_presentes_lower}
    passed = len(faltantes) == 0
    return AssetCheckResult(
        passed=passed,
        severity=AssetCheckSeverity.WARN,
        metadata={
            "islas_faltantes": str(sorted(faltantes)) if faltantes else "ninguna",
            "islas_encontradas": str(sorted(islas_presentes)),
        },
    )


# --- dataset_renta_limpio ---

@asset_check(
    asset="dataset_renta_limpio",
    description="Comprueba que el dataset limpio no está vacío tras el filtrado"
)
def check_no_vacio_renta_limpio(dataset_renta_limpio: pd.DataFrame) -> AssetCheckResult:
    n_filas = len(dataset_renta_limpio)
    passed = n_filas > 0
    return AssetCheckResult(
        passed=passed,
        severity=AssetCheckSeverity.ERROR,
        metadata={"n_filas_tras_filtrado": n_filas},
    )


@asset_check(
    asset="dataset_renta_limpio",
    description="Comprueba que OBS_VALUE no contiene valores negativos"
)
def check_obs_value_limpio(dataset_renta_limpio: pd.DataFrame) -> AssetCheckResult:
    negativos = (dataset_renta_limpio['OBS_VALUE'] < 0).sum()
    passed = int(negativos) == 0
    return AssetCheckResult(
        passed=passed,
        severity=AssetCheckSeverity.ERROR,
        metadata={
            "valores_negativos": int(negativos),
            "total_filas": len(dataset_renta_limpio),
        },
    )


@asset_check(
    asset="dataset_renta_limpio",
    description="Comprueba que los tipos de datos de columnas críticas son correctos"
)
def check_tipos_renta_limpio(dataset_renta_limpio: pd.DataFrame) -> AssetCheckResult:
    obs_es_numerico = pd.api.types.is_numeric_dtype(dataset_renta_limpio['OBS_VALUE'])
    periodo_es_entero = pd.api.types.is_integer_dtype(dataset_renta_limpio['TIME_PERIOD#es'])
    passed = obs_es_numerico and periodo_es_entero
    return AssetCheckResult(
        passed=passed,
        severity=AssetCheckSeverity.ERROR,
        metadata={
            "OBS_VALUE_dtype": str(dataset_renta_limpio['OBS_VALUE'].dtype),
            "TIME_PERIOD_dtype": str(dataset_renta_limpio['TIME_PERIOD#es'].dtype),
            "OBS_VALUE_es_numerico": obs_es_numerico,
            "TIME_PERIOD_es_entero": periodo_es_entero,
        },
    )


# --- dataset_renta_con_estudios ---

@asset_check(
    asset="dataset_renta_con_estudios",
    description="Comprueba que las columnas añadidas por el join con estudios están presentes"
)
def check_schema_renta_con_estudios(dataset_renta_con_estudios: pd.DataFrame) -> AssetCheckResult:
    columnas_esperadas = {'Nivel de estudios en curso', 'Total_Estudiantes'}
    columnas_presentes = set(dataset_renta_con_estudios.columns)
    faltantes = columnas_esperadas - columnas_presentes
    passed = len(faltantes) == 0
    return AssetCheckResult(
        passed=passed,
        severity=AssetCheckSeverity.ERROR,
        metadata={
            "columnas_faltantes": str(sorted(faltantes)) if faltantes else "ninguna",
        },
    )


@asset_check(
    asset="dataset_renta_con_estudios",
    description="Comprueba que Total_Estudiantes es positivo donde no es nulo"
)
def check_total_estudiantes_positivo(dataset_renta_con_estudios: pd.DataFrame) -> AssetCheckResult:
    con_valor = dataset_renta_con_estudios['Total_Estudiantes'].dropna()
    no_positivos = (con_valor <= 0).sum()
    passed = int(no_positivos) == 0
    return AssetCheckResult(
        passed=passed,
        severity=AssetCheckSeverity.ERROR,
        metadata={
            "valores_no_positivos": int(no_positivos),
            "filas_con_valor": len(con_valor),
        },
    )


# ===========================================================================
# CAPA 3: VISUALIZACIÓN (validaciones previas a la generación de gráficos)
# ===========================================================================

@asset_check(
    asset="dataset_renta_limpio",
    description="Comprueba que hay al menos 2 medidas distintas en el último año (grafico_distribucion_ingressos)"
)
def check_datos_grafico_distribucion(dataset_renta_limpio: pd.DataFrame) -> AssetCheckResult:
    ultimo_anio = dataset_renta_limpio['TIME_PERIOD#es'].max()
    df_ult = dataset_renta_limpio[dataset_renta_limpio['TIME_PERIOD#es'] == ultimo_anio]
    n_medidas = df_ult['MEDIDAS#es'].nunique()
    passed = n_medidas >= 2
    return AssetCheckResult(
        passed=passed,
        severity=AssetCheckSeverity.WARN,
        metadata={
            "ultimo_anio": int(ultimo_anio),
            "n_medidas_disponibles": int(n_medidas),
            "umbral_minimo": 2,
        },
    )


@asset_check(
    asset="dataset_renta_limpio",
    description="Comprueba que hay al menos 3 años distintos para la línea de tendencia (grafico_tendencia_total)"
)
def check_datos_grafico_tendencia(dataset_renta_limpio: pd.DataFrame) -> AssetCheckResult:
    n_anios = dataset_renta_limpio['TIME_PERIOD#es'].nunique()
    passed = n_anios >= 3
    return AssetCheckResult(
        passed=passed,
        severity=AssetCheckSeverity.WARN,
        metadata={
            "n_anios_disponibles": int(n_anios),
            "umbral_minimo": 3,
            "anios": str(sorted(dataset_renta_limpio['TIME_PERIOD#es'].unique().tolist())),
        },
    )


@asset_check(
    asset="dataset_renta_limpio",
    description="Comprueba que al menos 2 islas tienen datos asignados (grafico_ingresos_por_isla)"
)
def check_datos_grafico_islas(dataset_renta_limpio: pd.DataFrame) -> AssetCheckResult:
    n_islas = dataset_renta_limpio['ISLA_FINAL'].dropna().nunique()
    passed = n_islas >= 2
    return AssetCheckResult(
        passed=passed,
        severity=AssetCheckSeverity.WARN,
        metadata={
            "n_islas_con_datos": int(n_islas),
            "umbral_minimo": 2,
        },
    )


@asset_check(
    asset="dataset_estudios_limpio",
    description="Comprueba que hay al menos 3 niveles de estudios distintos (grafico_nivel_estudios_distribucion)"
)
def check_datos_grafico_estudios(dataset_estudios_limpio: pd.DataFrame) -> AssetCheckResult:
    n_niveles = dataset_estudios_limpio['Nivel de estudios en curso'].nunique()
    passed = n_niveles >= 3
    return AssetCheckResult(
        passed=passed,
        severity=AssetCheckSeverity.WARN,
        metadata={
            "n_niveles_disponibles": int(n_niveles),
            "umbral_minimo": 3,
            "niveles": str(sorted(dataset_estudios_limpio['Nivel de estudios en curso'].dropna().unique().tolist())),
        },
    )


# ===========================================================================
# CAPA 4: IA
# ===========================================================================

# --- islas_raw ---

@asset_check(asset="islas_raw", description="Comprueba que las columnas obligatorias ['isla', 'año', 'valor'] están presentes")
def check_schema_islas_raw(islas_raw: pd.DataFrame) -> AssetCheckResult:
    columnas_esperadas = {'isla', 'año', 'valor'}
    faltantes = columnas_esperadas - set(islas_raw.columns)
    passed = len(faltantes) == 0
    return AssetCheckResult(
        passed=passed,
        severity=AssetCheckSeverity.ERROR,
        metadata={
            "columnas_faltantes": str(sorted(faltantes)) if faltantes else "ninguna",
        },
    )


@asset_check(asset="islas_raw", description="Comprueba que el DataFrame de islas no está vacío")
def check_no_vacio_islas_raw(islas_raw: pd.DataFrame) -> AssetCheckResult:
    n_filas = len(islas_raw)
    passed = n_filas > 0
    return AssetCheckResult(
        passed=passed,
        severity=AssetCheckSeverity.ERROR,
        metadata={"n_filas": n_filas},
    )


@asset_check(asset="islas_raw", description="Comprueba que las 7 islas canarias están representadas")
def check_islas_canarias_raw(islas_raw: pd.DataFrame) -> AssetCheckResult:
    islas_esperadas = {
        'Tenerife', 'Gran Canaria', 'La Palma',
        'La Gomera', 'El Hierro', 'Lanzarote', 'Fuerteventura'
    }
    islas_presentes = set(islas_raw['isla'].dropna().unique())
    faltantes = islas_esperadas - islas_presentes
    passed = len(faltantes) == 0
    return AssetCheckResult(
        passed=passed,
        severity=AssetCheckSeverity.WARN,
        metadata={
            "islas_faltantes": str(sorted(faltantes)) if faltantes else "ninguna",
            "islas_presentes": str(sorted(islas_presentes)),
        },
    )


@asset_check(asset="islas_raw", description="Comprueba que todos los valores de renta son positivos")
def check_valores_positivos_islas_raw(islas_raw: pd.DataFrame) -> AssetCheckResult:
    negativos = (islas_raw['valor'] <= 0).sum()
    passed = int(negativos) == 0
    return AssetCheckResult(
        passed=passed,
        severity=AssetCheckSeverity.WARN,
        metadata={"valores_no_positivos": int(negativos)},
    )


# --- template_ia ---

@asset_check(asset="template_ia", description="Comprueba que el diccionario tiene la clave 'model' con valor no vacío")
def check_template_tiene_modelo(template_ia: dict) -> AssetCheckResult:
    modelo = template_ia.get('model', '')
    passed = bool(modelo)
    return AssetCheckResult(
        passed=passed,
        severity=AssetCheckSeverity.ERROR,
        metadata={"model": modelo or "(vacío)"},
    )


@asset_check(asset="template_ia", description="Comprueba que 'messages' contiene roles 'system' y 'user'")
def check_template_tiene_mensajes(template_ia: dict) -> AssetCheckResult:
    mensajes = template_ia.get('messages', [])
    roles_presentes = {m.get('role') for m in mensajes}
    roles_faltantes = {'system', 'user'} - roles_presentes
    passed = len(roles_faltantes) == 0
    return AssetCheckResult(
        passed=passed,
        severity=AssetCheckSeverity.ERROR,
        metadata={
            "roles_faltantes": str(sorted(roles_faltantes)) if roles_faltantes else "ninguno",
            "n_mensajes": len(mensajes),
        },
    )


@asset_check(asset="template_ia", description="Comprueba que el user message menciona las variables isla, año y valor")
def check_template_menciona_variables(template_ia: dict) -> AssetCheckResult:
    mensajes = template_ia.get('messages', [])
    user_content = next(
        (m.get('content', '') for m in mensajes if m.get('role') == 'user'), ''
    )
    variables_esperadas = ['isla', 'año', 'valor']
    faltantes = [v for v in variables_esperadas if v not in user_content]
    passed = len(faltantes) == 0
    return AssetCheckResult(
        passed=passed,
        severity=AssetCheckSeverity.WARN,
        metadata={
            "variables_faltantes": str(faltantes) if faltantes else "ninguna",
        },
    )


# --- codigo_generado_ia ---

@asset_check(asset="codigo_generado_ia", description="Comprueba que la respuesta del LLM no es un string vacío")
def check_respuesta_no_vacia(codigo_generado_ia: str) -> AssetCheckResult:
    passed = bool(codigo_generado_ia and codigo_generado_ia.strip())
    return AssetCheckResult(
        passed=passed,
        severity=AssetCheckSeverity.ERROR,
        metadata={"longitud": len(codigo_generado_ia) if codigo_generado_ia else 0},
    )


@asset_check(asset="codigo_generado_ia", description="Comprueba que la respuesta contiene palabras clave Python esperadas")
def check_respuesta_contiene_python(codigo_generado_ia: str) -> AssetCheckResult:
    palabras_clave = ['import', 'ggplot', 'grafico']
    faltantes = [p for p in palabras_clave if p not in codigo_generado_ia]
    passed = len(faltantes) == 0
    return AssetCheckResult(
        passed=passed,
        severity=AssetCheckSeverity.WARN,
        metadata={
            "palabras_clave_faltantes": str(faltantes) if faltantes else "ninguna",
        },
    )


# --- codigo_limpio_ia ---

@asset_check(asset="codigo_limpio_ia", description="Comprueba que el código limpio contiene la función generar_plot(df)")
def check_limpio_tiene_funcion(codigo_limpio_ia: str) -> AssetCheckResult:
    passed = 'def generar_plot(df):' in codigo_limpio_ia
    return AssetCheckResult(
        passed=passed,
        severity=AssetCheckSeverity.ERROR,
        metadata={"contiene_funcion": passed},
    )


@asset_check(asset="codigo_limpio_ia", description="Comprueba que no quedan delimitadores de markdown en el código")
def check_limpio_sin_markdown(codigo_limpio_ia: str) -> AssetCheckResult:
    passed = '```' not in codigo_limpio_ia
    return AssetCheckResult(
        passed=passed,
        severity=AssetCheckSeverity.ERROR,
        metadata={"contiene_markdown": not passed},
    )


@asset_check(asset="codigo_limpio_ia", description="Comprueba que el código termina con 'return grafico'")
def check_limpio_tiene_return(codigo_limpio_ia: str) -> AssetCheckResult:
    passed = 'return grafico' in codigo_limpio_ia
    return AssetCheckResult(
        passed=passed,
        severity=AssetCheckSeverity.ERROR,
        metadata={"contiene_return_grafico": passed},
    )


# ===========================================================================
# Lista exportada para registrar en Definitions
# ===========================================================================

all_checks = [
    # Capa 1 — Extracción
    check_schema_renta,
    check_no_vacio_renta,
    check_anios_renta,
    check_medidas_renta,
    check_schema_municipios,
    check_no_vacio_municipios,
    check_schema_estudios,
    check_no_vacio_estudios,
    check_periodos_estudios,
    # Capa 2 — Transformación
    check_islas_canarias,
    check_schema_renta_con_estudios,
    check_total_estudiantes_positivo,
    check_no_vacio_renta_limpio,
    check_obs_value_limpio,
    check_tipos_renta_limpio,
    # Capa 3 — Visualización
    check_datos_grafico_distribucion,
    check_datos_grafico_tendencia,
    check_datos_grafico_islas,
    check_datos_grafico_estudios,
    # Capa 4 — IA
    check_schema_islas_raw,
    check_no_vacio_islas_raw,
    check_islas_canarias_raw,
    check_valores_positivos_islas_raw,
    check_template_tiene_modelo,
    check_template_tiene_mensajes,
    check_template_menciona_variables,
    check_respuesta_no_vacia,
    check_respuesta_contiene_python,
    check_limpio_tiene_funcion,
    check_limpio_sin_markdown,
    check_limpio_tiene_return,
]
