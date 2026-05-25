"""
Checks de calidad de datos para el pipeline de secciones censales de Tenerife.
Organizados en dos capas:
  - Capa 1: Extracción  — validaciones sobre los datos cargados desde ficheros
  - Capa 2: Transformación — validaciones sobre la calidad del join con cartografía
"""

import re
import pandas as pd
import geopandas as gpd
from dagster import asset_check, AssetCheckResult, AssetCheckSeverity

# Formato esperado: YYYYMMDD_XXXXX_DXX_SXXX  (ej. 20220101_38001_D01_S001)
_GEOCODE_RE = re.compile(r"^\d{8}_\d{5}_D\d{2}_S\d{3}$")


def _check_geocode_format(serie: pd.Series) -> tuple[int, int]:
    """Devuelve (n_válidos, n_totales) para una serie de geocodes."""
    muestra = serie.dropna().astype(str).head(500)
    validos = muestra.apply(lambda g: bool(_GEOCODE_RE.match(g))).sum()
    return int(validos), len(muestra)


# ===========================================================================
# CAPA 1: EXTRACCIÓN
# ===========================================================================

# --- cargar_renta_media_sc ---

@asset_check(
    asset="cargar_renta_media_sc",
    description="Comprueba que las columnas obligatorias de rentamedia-sc-3.csv están presentes",
)
def check_schema_renta_media_sc(cargar_renta_media_sc: pd.DataFrame) -> AssetCheckResult:
    esperadas = {"año", "MEDIDAS_CODE", "MEDIDAS#es", "TERRITORIO_CODE", "OBS_VALUE"}
    presentes = set(cargar_renta_media_sc.columns)
    faltantes = esperadas - presentes
    return AssetCheckResult(
        passed=len(faltantes) == 0,
        severity=AssetCheckSeverity.ERROR,
        metadata={
            "columnas_faltantes": str(sorted(faltantes)) if faltantes else "ninguna",
        },
    )


@asset_check(
    asset="cargar_renta_media_sc",
    description="Comprueba que el dataset de renta media por sección no está vacío",
)
def check_no_vacio_renta_media_sc(cargar_renta_media_sc: pd.DataFrame) -> AssetCheckResult:
    n = len(cargar_renta_media_sc)
    return AssetCheckResult(
        passed=n > 0,
        severity=AssetCheckSeverity.ERROR,
        metadata={"n_filas": n},
    )


@asset_check(
    asset="cargar_renta_media_sc",
    description="Comprueba que OBS_VALUE de renta media no supera el 20% de nulos",
)
def check_nulos_renta_media_sc(cargar_renta_media_sc: pd.DataFrame) -> AssetCheckResult:
    tasa = cargar_renta_media_sc["OBS_VALUE"].isna().mean()
    return AssetCheckResult(
        passed=bool(tasa <= 0.20),
        severity=AssetCheckSeverity.WARN,
        metadata={
            "tasa_nulos": f"{tasa:.1%}",
            "umbral": "20%",
        },
    )


@asset_check(
    asset="cargar_renta_media_sc",
    description="Comprueba que TERRITORIO_CODE tiene el formato de geocode esperado",
)
def check_geocode_renta_media_sc(cargar_renta_media_sc: pd.DataFrame) -> AssetCheckResult:
    validos, total = _check_geocode_format(cargar_renta_media_sc["TERRITORIO_CODE"])
    tasa = validos / total if total > 0 else 0
    return AssetCheckResult(
        passed=tasa >= 0.95,
        severity=AssetCheckSeverity.ERROR,
        metadata={
            "geocodes_validos": validos,
            "muestra_analizada": total,
            "tasa_validos": f"{tasa:.1%}",
        },
    )


# --- cargar_distribucion_renta_sc ---

@asset_check(
    asset="cargar_distribucion_renta_sc",
    description="Comprueba que las columnas obligatorias de distribucion-renta-ingresos.csv están presentes",
)
def check_schema_distribucion_renta_sc(cargar_distribucion_renta_sc: pd.DataFrame) -> AssetCheckResult:
    esperadas = {"año", "MEDIDAS_CODE", "MEDIDAS#es", "TERRITORIO_CODE", "OBS_VALUE"}
    presentes = set(cargar_distribucion_renta_sc.columns)
    faltantes = esperadas - presentes
    return AssetCheckResult(
        passed=len(faltantes) == 0,
        severity=AssetCheckSeverity.ERROR,
        metadata={
            "columnas_faltantes": str(sorted(faltantes)) if faltantes else "ninguna",
        },
    )


@asset_check(
    asset="cargar_distribucion_renta_sc",
    description="Comprueba que el dataset de distribución de renta no está vacío",
)
def check_no_vacio_distribucion_renta_sc(cargar_distribucion_renta_sc: pd.DataFrame) -> AssetCheckResult:
    n = len(cargar_distribucion_renta_sc)
    return AssetCheckResult(
        passed=n > 0,
        severity=AssetCheckSeverity.ERROR,
        metadata={"n_filas": n},
    )


@asset_check(
    asset="cargar_distribucion_renta_sc",
    description="Comprueba que OBS_VALUE de distribución de renta no supera el 20% de nulos",
)
def check_nulos_distribucion_renta_sc(cargar_distribucion_renta_sc: pd.DataFrame) -> AssetCheckResult:
    tasa = cargar_distribucion_renta_sc["OBS_VALUE"].isna().mean()
    return AssetCheckResult(
        passed=bool(tasa <= 0.20),
        severity=AssetCheckSeverity.WARN,
        metadata={
            "tasa_nulos": f"{tasa:.1%}",
            "umbral": "20%",
        },
    )


# --- cargar_ocupacion_sc ---

@asset_check(
    asset="cargar_ocupacion_sc",
    description="Comprueba que las columnas obligatorias de ocupacion-sc-3.csv están presentes",
)
def check_schema_ocupacion_sc(cargar_ocupacion_sc: pd.DataFrame) -> AssetCheckResult:
    esperadas = {"geocode", "año", "ocupacion", "num_casos", "municipio"}
    presentes = set(cargar_ocupacion_sc.columns)
    faltantes = esperadas - presentes
    return AssetCheckResult(
        passed=len(faltantes) == 0,
        severity=AssetCheckSeverity.ERROR,
        metadata={
            "columnas_faltantes": str(sorted(faltantes)) if faltantes else "ninguna",
        },
    )


@asset_check(
    asset="cargar_ocupacion_sc",
    description="Comprueba que el dataset de ocupación no está vacío",
)
def check_no_vacio_ocupacion_sc(cargar_ocupacion_sc: pd.DataFrame) -> AssetCheckResult:
    n = len(cargar_ocupacion_sc)
    return AssetCheckResult(
        passed=n > 0,
        severity=AssetCheckSeverity.ERROR,
        metadata={"n_filas": n},
    )


@asset_check(
    asset="cargar_ocupacion_sc",
    description="Comprueba que el geocode de ocupacion-sc-3.csv tiene el formato correcto",
)
def check_geocode_ocupacion_sc(cargar_ocupacion_sc: pd.DataFrame) -> AssetCheckResult:
    validos, total = _check_geocode_format(cargar_ocupacion_sc["geocode"])
    tasa = validos / total if total > 0 else 0
    return AssetCheckResult(
        passed=tasa >= 0.95,
        severity=AssetCheckSeverity.ERROR,
        metadata={
            "geocodes_validos": validos,
            "muestra_analizada": total,
            "tasa_validos": f"{tasa:.1%}",
        },
    )


# --- cargar_actividad_sc ---

@asset_check(
    asset="cargar_actividad_sc",
    description="Comprueba que las columnas obligatorias de actividad-sc-3.csv están presentes",
)
def check_schema_actividad_sc(cargar_actividad_sc: pd.DataFrame) -> AssetCheckResult:
    esperadas = {"geocode", "Periodo", "Actividad económica", "num_casos", "municipio"}
    presentes = set(cargar_actividad_sc.columns)
    faltantes = esperadas - presentes
    return AssetCheckResult(
        passed=len(faltantes) == 0,
        severity=AssetCheckSeverity.ERROR,
        metadata={
            "columnas_faltantes": str(sorted(faltantes)) if faltantes else "ninguna",
        },
    )


@asset_check(
    asset="cargar_actividad_sc",
    description="Comprueba que el dataset de actividad no está vacío",
)
def check_no_vacio_actividad_sc(cargar_actividad_sc: pd.DataFrame) -> AssetCheckResult:
    n = len(cargar_actividad_sc)
    return AssetCheckResult(
        passed=n > 0,
        severity=AssetCheckSeverity.ERROR,
        metadata={"n_filas": n},
    )


@asset_check(
    asset="cargar_actividad_sc",
    description="Comprueba que el geocode de actividad-sc-3.csv tiene el formato correcto",
)
def check_geocode_actividad_sc(cargar_actividad_sc: pd.DataFrame) -> AssetCheckResult:
    validos, total = _check_geocode_format(cargar_actividad_sc["geocode"])
    tasa = validos / total if total > 0 else 0
    return AssetCheckResult(
        passed=tasa >= 0.95,
        severity=AssetCheckSeverity.ERROR,
        metadata={
            "geocodes_validos": validos,
            "muestra_analizada": total,
            "tasa_validos": f"{tasa:.1%}",
        },
    )


# --- cargar_cartografia ---

@asset_check(
    asset="cargar_cartografia",
    description="Comprueba que el GeoDataFrame de cartografía no está vacío",
)
def check_no_vacio_cartografia(cargar_cartografia: gpd.GeoDataFrame) -> AssetCheckResult:
    n = len(cargar_cartografia)
    return AssetCheckResult(
        passed=n > 0,
        severity=AssetCheckSeverity.ERROR,
        metadata={"n_secciones": n},
    )


@asset_check(
    asset="cargar_cartografia",
    description="Comprueba que el GeoDataFrame de cartografía contiene los 4 años esperados (2021–2024)",
)
def check_anios_cartografia(cargar_cartografia: gpd.GeoDataFrame) -> AssetCheckResult:
    anios_esperados = {2021, 2022, 2023, 2024}
    anios_presentes = set(cargar_cartografia["año_mapa"].dropna().astype(int).unique())
    faltantes = anios_esperados - anios_presentes
    return AssetCheckResult(
        passed=len(faltantes) == 0,
        severity=AssetCheckSeverity.ERROR,
        metadata={
            "anios_faltantes": str(sorted(faltantes)) if faltantes else "ninguno",
            "anios_presentes": str(sorted(anios_presentes)),
        },
    )


@asset_check(
    asset="cargar_cartografia",
    description="Comprueba que la geometría de la cartografía no tiene valores nulos",
)
def check_geometria_no_nula(cargar_cartografia: gpd.GeoDataFrame) -> AssetCheckResult:
    n_nulas = cargar_cartografia.geometry.isna().sum()
    return AssetCheckResult(
        passed=int(n_nulas) == 0,
        severity=AssetCheckSeverity.ERROR,
        metadata={"geometrias_nulas": int(n_nulas)},
    )


# ===========================================================================
# CAPA 2: TRANSFORMACIÓN — calidad de los joins con cartografía
# ===========================================================================

@asset_check(
    asset="geodata_renta_media",
    description="Comprueba que el join renta media × cartografía cubre al menos el 50% de las secciones",
)
def check_join_renta_media(geodata_renta_media: gpd.GeoDataFrame) -> AssetCheckResult:
    cobertura = geodata_renta_media["OBS_VALUE"].notna().mean()
    return AssetCheckResult(
        passed=bool(cobertura >= 0.50),
        severity=AssetCheckSeverity.WARN,
        metadata={
            "cobertura_join": f"{cobertura:.1%}",
            "umbral_minimo": "50%",
            "n_secciones_con_dato": int(geodata_renta_media["OBS_VALUE"].notna().sum()),
            "n_secciones_total": len(geodata_renta_media),
        },
    )


@asset_check(
    asset="geodata_distribucion_renta",
    description="Comprueba que el join distribución renta × cartografía cubre al menos el 50% de las secciones",
)
def check_join_distribucion_renta(geodata_distribucion_renta: gpd.GeoDataFrame) -> AssetCheckResult:
    cobertura = geodata_distribucion_renta["medida_dominante"].notna().mean()
    return AssetCheckResult(
        passed=bool(cobertura >= 0.50),
        severity=AssetCheckSeverity.WARN,
        metadata={
            "cobertura_join": f"{cobertura:.1%}",
            "umbral_minimo": "50%",
        },
    )


@asset_check(
    asset="geodata_ocupacion",
    description="Comprueba que el join ocupación × cartografía cubre al menos el 50% de las secciones",
)
def check_join_ocupacion(geodata_ocupacion: gpd.GeoDataFrame) -> AssetCheckResult:
    cobertura = geodata_ocupacion["ocupacion_dominante"].notna().mean()
    return AssetCheckResult(
        passed=bool(cobertura >= 0.50),
        severity=AssetCheckSeverity.WARN,
        metadata={
            "cobertura_join": f"{cobertura:.1%}",
            "umbral_minimo": "50%",
        },
    )


@asset_check(
    asset="geodata_actividad",
    description="Comprueba que el join actividad × cartografía cubre al menos el 50% de las secciones",
)
def check_join_actividad(geodata_actividad: gpd.GeoDataFrame) -> AssetCheckResult:
    cobertura = geodata_actividad["actividad_dominante"].notna().mean()
    return AssetCheckResult(
        passed=bool(cobertura >= 0.50),
        severity=AssetCheckSeverity.WARN,
        metadata={
            "cobertura_join": f"{cobertura:.1%}",
            "umbral_minimo": "50%",
        },
    )


# ===========================================================================
# Lista exportada para registrar en Definitions
# ===========================================================================

all_checks_secciones = [
    # Capa 1 — Extracción
    check_schema_renta_media_sc,
    check_no_vacio_renta_media_sc,
    check_nulos_renta_media_sc,
    check_geocode_renta_media_sc,
    check_schema_distribucion_renta_sc,
    check_no_vacio_distribucion_renta_sc,
    check_nulos_distribucion_renta_sc,
    check_schema_ocupacion_sc,
    check_no_vacio_ocupacion_sc,
    check_geocode_ocupacion_sc,
    check_schema_actividad_sc,
    check_no_vacio_actividad_sc,
    check_geocode_actividad_sc,
    check_no_vacio_cartografia,
    check_anios_cartografia,
    check_geometria_no_nula,
    # Capa 2 — Transformación (calidad de joins)
    check_join_renta_media,
    check_join_distribucion_renta,
    check_join_ocupacion,
    check_join_actividad,
]
