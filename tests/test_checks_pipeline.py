"""
Tests negativos para checks_pipeline.py.

Cada test construye un DataFrame sintético que viola deliberadamente
la condición que valida el check y verifica que el resultado es passed=False.

Los checks son funciones puras sobre DataFrames, por lo que se pueden invocar
directamente sin necesidad de levantar el pipeline de Dagster.
"""

import pandas as pd
import pytest
from src.checks_pipeline import (
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
)


# ===========================================================================
# HELPERS
# ===========================================================================

def _df_renta_valido():
    """DataFrame mínimo válido de rentas."""
    return pd.DataFrame({
        'TIME_PERIOD#es': list(range(2015, 2024)) * 2,
        'MEDIDAS#es': ['Sueldos y salarios', 'Pensiones'] * 9,
        'OBS_VALUE': [1000.0] * 18,
        'TERRITORIO#es': ['Tenerife'] * 18,
        'CONFIDENCIALIDAD_OBSERVACION#es': [None] * 18,
        'ISLA_FINAL': ['Tenerife'] * 18,
        'MUNICIPIO_ISLA': ['Tenerife (Tenerife)'] * 18,
    })


def _df_municipios_valido():
    """DataFrame mínimo válido de municipios."""
    islas = ['Tenerife', 'Gran Canaria', 'La Palma', 'La Gomera',
             'El Hierro', 'Lanzarote', 'Fuerteventura']
    return pd.DataFrame({
        'NOMBRE': islas,
        'ISLA': islas,
        'CMUN': list(range(1, 8)),
        'CISLA': list(range(1, 8)),
        'NOMBRE_NORMALIZADO': islas,
        'ISLA_NORMALIZADO': islas,
    })


def _df_estudios_valido():
    """DataFrame mínimo válido de nivel de estudios."""
    return pd.DataFrame({
        'Año_Estudios': [2021, 2022, 2023],
        'Nivel de estudios en curso': ['Primaria', 'Secundaria', 'Superior'],
        'Sexo': ['Total', 'Total', 'Total'],
        'Total': [1000, 2000, 1500],
        'CMUN_EST': [1, 2, 3],
        'MUNICIPIO_EST': ['Arona', 'Las Palmas', 'Santa Cruz'],
    })


def _df_renta_con_municipios_valido():
    """DataFrame válido post-merge renta–municipios con las 7 islas."""
    islas = ['Tenerife', 'Gran Canaria', 'La Palma', 'La Gomera',
             'El Hierro', 'Lanzarote', 'Fuerteventura']
    return pd.DataFrame({
        'ISLA_FINAL': islas,
        'OBS_VALUE': [5000.0] * 7,
    })


def _df_renta_con_estudios_valido():
    """DataFrame válido post-merge renta–estudios."""
    return pd.DataFrame({
        'CMUN': [1, 2, 3],
        'TIME_PERIOD#es': [2021, 2022, 2023],
        'Nivel de estudios en curso': ['Primaria', 'Secundaria', 'Superior'],
        'Total_Estudiantes': [1000.0, 2000.0, 1500.0],
        'OBS_VALUE': [500.0, 600.0, 700.0],
    })


# ===========================================================================
# CAPA 1: EXTRACCIÓN — Tests negativos
# ===========================================================================

class TestChecksExtraccion:

    def test_schema_renta_falta_columna(self):
        """Falta la columna OBS_VALUE → check debe fallar."""
        df = pd.DataFrame({
            'TIME_PERIOD#es': [2020],
            'MEDIDAS#es': ['Pensiones'],
            # OBS_VALUE ausente deliberadamente
            'TERRITORIO#es': ['Tenerife'],
            'CONFIDENCIALIDAD_OBSERVACION#es': [None],
        })
        resultado = check_schema_renta(df)
        assert resultado.passed is False, \
            "El check debe fallar cuando falta OBS_VALUE"

    def test_no_vacio_renta_dataframe_vacio(self):
        """DataFrame sin filas → check debe fallar."""
        df = pd.DataFrame(columns=[
            'TIME_PERIOD#es', 'MEDIDAS#es', 'OBS_VALUE',
            'TERRITORIO#es', 'CONFIDENCIALIDAD_OBSERVACION#es'
        ])
        resultado = check_no_vacio_renta(df)
        assert resultado.passed is False, \
            "El check debe fallar con un DataFrame vacío"

    def test_anios_renta_faltan_anios_recientes(self):
        """Serie temporal solo hasta 2020 (faltan 2021–2023) → check debe fallar."""
        df = pd.DataFrame({
            'TIME_PERIOD#es': list(range(2015, 2021)),  # sin 2021,2022,2023
            'MEDIDAS#es': ['Pensiones'] * 6,
            'OBS_VALUE': [100.0] * 6,
        })
        resultado = check_anios_renta(df)
        assert resultado.passed is False, \
            "El check debe fallar cuando faltan años del rango 2015–2023"

    def test_medidas_renta_falta_medida_conocida(self):
        """Falta 'Pensiones' en MEDIDAS#es → check debe fallar."""
        df = pd.DataFrame({
            'TIME_PERIOD#es': [2020, 2021],
            'MEDIDAS#es': ['Sueldos y salarios', 'Otros ingresos'],
            'OBS_VALUE': [100.0, 200.0],
        })
        resultado = check_medidas_renta(df)
        assert resultado.passed is False, \
            "El check debe fallar cuando falta la medida 'Pensiones'"

    def test_schema_municipios_falta_columna_cmun(self):
        """Falta la columna CMUN → check debe fallar."""
        df = pd.DataFrame({
            'NOMBRE': ['Arona'],
            'ISLA': ['Tenerife'],
            # CMUN ausente deliberadamente
            'CISLA': [1],
            'NOMBRE_NORMALIZADO': ['Arona'],
            'ISLA_NORMALIZADO': ['Tenerife'],
        })
        resultado = check_schema_municipios(df)
        assert resultado.passed is False, \
            "El check debe fallar cuando falta la columna CMUN"

    def test_no_vacio_municipios_dataframe_vacio(self):
        """DataFrame de municipios sin filas → check debe fallar."""
        df = pd.DataFrame(columns=['NOMBRE', 'ISLA', 'CMUN', 'CISLA',
                                   'NOMBRE_NORMALIZADO', 'ISLA_NORMALIZADO'])
        resultado = check_no_vacio_municipios(df)
        assert resultado.passed is False, \
            "El check debe fallar con un DataFrame de municipios vacío"

    def test_schema_estudios_falta_columna_anio(self):
        """Falta la columna Año_Estudios → check debe fallar."""
        df = pd.DataFrame({
            # 'Año_Estudios' ausente deliberadamente
            'Nivel de estudios en curso': ['Primaria'],
            'Sexo': ['Total'],
            'Total': [500],
            'CMUN_EST': [1],
            'MUNICIPIO_EST': ['Arona'],
        })
        resultado = check_schema_estudios(df)
        assert resultado.passed is False, \
            "El check debe fallar cuando falta la columna Año_Estudios"

    def test_no_vacio_estudios_dataframe_vacio(self):
        """DataFrame de estudios sin filas → check debe fallar."""
        df = pd.DataFrame(columns=[
            'Año_Estudios', 'Nivel de estudios en curso', 'Sexo',
            'Total', 'CMUN_EST', 'MUNICIPIO_EST'
        ])
        resultado = check_no_vacio_estudios(df)
        assert resultado.passed is False, \
            "El check debe fallar con un DataFrame de estudios vacío"

    def test_periodos_estudios_anio_demasiado_antiguo(self):
        """Año mínimo = 2010 (< 2019) → check debe fallar."""
        df = pd.DataFrame({
            'Año_Estudios': [2010, 2015, 2020],
            'Nivel de estudios en curso': ['Primaria', 'Secundaria', 'Superior'],
            'Sexo': ['Total', 'Total', 'Total'],
            'Total': [100, 200, 300],
            'CMUN_EST': [1, 2, 3],
            'MUNICIPIO_EST': ['A', 'B', 'C'],
        })
        resultado = check_periodos_estudios(df)
        assert resultado.passed is False, \
            "El check debe fallar cuando el año mínimo es anterior a 2019"


# ===========================================================================
# CAPA 2: TRANSFORMACIÓN — Tests negativos
# ===========================================================================

class TestChecksTransformacion:

    def test_islas_canarias_solo_dos_islas(self):
        """Solo 2 de las 7 islas en ISLA_FINAL → check debe fallar."""
        df = pd.DataFrame({
            'ISLA_FINAL': ['Tenerife', 'Lanzarote'],
            'OBS_VALUE': [1000.0, 2000.0],
        })
        resultado = check_islas_canarias(df)
        assert resultado.passed is False, \
            "El check debe fallar cuando faltan 5 de las 7 islas"

    def test_schema_renta_con_estudios_falta_total_estudiantes(self):
        """Falta la columna Total_Estudiantes tras el join → check debe fallar."""
        df = pd.DataFrame({
            'CMUN': [1],
            'TIME_PERIOD#es': [2021],
            'Nivel de estudios en curso': ['Primaria'],
            # 'Total_Estudiantes' ausente deliberadamente
        })
        resultado = check_schema_renta_con_estudios(df)
        assert resultado.passed is False, \
            "El check debe fallar cuando falta la columna Total_Estudiantes"

    def test_total_estudiantes_valor_negativo(self):
        """Total_Estudiantes con valor −5 → check debe fallar."""
        df = pd.DataFrame({
            'Nivel de estudios en curso': ['Primaria', 'Secundaria'],
            'Total_Estudiantes': [-5.0, 1000.0],  # primer valor negativo
        })
        resultado = check_total_estudiantes_positivo(df)
        assert resultado.passed is False, \
            "El check debe fallar cuando Total_Estudiantes contiene valores negativos"

    def test_no_vacio_renta_limpio_dataframe_vacio(self):
        """dataset_renta_limpio sin filas → check debe fallar."""
        df = pd.DataFrame(columns=['OBS_VALUE', 'TIME_PERIOD#es', 'MEDIDAS#es', 'ISLA_FINAL'])
        resultado = check_no_vacio_renta_limpio(df)
        assert resultado.passed is False, \
            "El check debe fallar con un dataset_renta_limpio vacío"

    def test_obs_value_limpio_valor_negativo(self):
        """OBS_VALUE con −100 → check debe fallar."""
        df = _df_renta_valido().copy()
        df.loc[0, 'OBS_VALUE'] = -100.0
        resultado = check_obs_value_limpio(df)
        assert resultado.passed is False, \
            "El check debe fallar cuando OBS_VALUE contiene un valor negativo"

    def test_tipos_renta_limpio_obs_value_es_string(self):
        """OBS_VALUE es string en lugar de numérico → check debe fallar."""
        df = _df_renta_valido().copy()
        df['OBS_VALUE'] = df['OBS_VALUE'].astype(str)  # convertir a str
        resultado = check_tipos_renta_limpio(df)
        assert resultado.passed is False, \
            "El check debe fallar cuando OBS_VALUE no es un tipo numérico"


# ===========================================================================
# CAPA 3: VISUALIZACIÓN — Tests negativos
# ===========================================================================

class TestChecksVisualizacion:

    def test_datos_grafico_distribucion_una_sola_medida(self):
        """Solo 1 medida en el último año → check debe fallar."""
        ultimo_anio = 2023
        df = pd.DataFrame({
            'TIME_PERIOD#es': [ultimo_anio, ultimo_anio - 1],
            'MEDIDAS#es': ['Pensiones', 'Sueldos y salarios'],
            # En el último año solo hay 'Pensiones'
            'OBS_VALUE': [500.0, 600.0],
            'ISLA_FINAL': ['Tenerife', 'Gran Canaria'],
        })
        # Dejar solo 1 medida en el último año
        df.loc[df['TIME_PERIOD#es'] == ultimo_anio, 'MEDIDAS#es'] = 'Pensiones'
        resultado = check_datos_grafico_distribucion(df)
        assert resultado.passed is False, \
            "El check debe fallar cuando hay menos de 2 medidas en el último año"

    def test_datos_grafico_tendencia_solo_dos_anios(self):
        """Solo 2 años distintos → check debe fallar (mínimo requerido: 3)."""
        df = pd.DataFrame({
            'TIME_PERIOD#es': [2020, 2021],  # solo 2 años
            'MEDIDAS#es': ['Pensiones', 'Sueldos y salarios'],
            'OBS_VALUE': [300.0, 400.0],
            'ISLA_FINAL': ['Tenerife', 'Gran Canaria'],
        })
        resultado = check_datos_grafico_tendencia(df)
        assert resultado.passed is False, \
            "El check debe fallar cuando hay menos de 3 años distintos"

    def test_datos_grafico_islas_solo_una_isla(self):
        """Solo 1 isla en ISLA_FINAL → check debe fallar."""
        df = pd.DataFrame({
            'TIME_PERIOD#es': [2020, 2021, 2022],
            'MEDIDAS#es': ['Pensiones', 'Pensiones', 'Pensiones'],
            'OBS_VALUE': [100.0, 200.0, 300.0],
            'ISLA_FINAL': ['Tenerife', 'Tenerife', 'Tenerife'],  # solo 1 isla
        })
        resultado = check_datos_grafico_islas(df)
        assert resultado.passed is False, \
            "El check debe fallar cuando solo hay una isla con datos"

    def test_datos_grafico_estudios_solo_dos_niveles(self):
        """Solo 2 niveles de estudios distintos → check debe fallar."""
        df = pd.DataFrame({
            'Nivel de estudios en curso': ['Primaria', 'Secundaria'],  # solo 2
            'Total': [1000, 2000],
            'ISLA_NORMALIZADO': ['Tenerife', 'Gran Canaria'],
            'MUNICIPIO': ['Arona', 'Las Palmas'],
        })
        resultado = check_datos_grafico_estudios(df)
        assert resultado.passed is False, \
            "El check debe fallar cuando hay menos de 3 niveles de estudios"
