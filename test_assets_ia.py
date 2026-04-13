"""
Tests unitarios para los assets del pipeline de IA generativa.

Se prueban directamente las funciones Python sin levantar Dagster ni
conectarse al LLM, usando datos sintéticos y código plotnine hardcodeado.

Cubre:
  - codigo_limpio_ia: extracción de bloques markdown y wrapping en generar_plot()
  - visualizacion_png: ejecución de código plotnine y generación de PNG

El asset codigo_generado_ia no se prueba aquí porque requiere conexión
a un servicio LLM externo (Ollama).
"""

import textwrap
from pathlib import Path

import pandas as pd
import pytest

from assets_renta_canarias import codigo_limpio_ia, visualizacion_png


# ===========================================================================
# HELPERS
# ===========================================================================

def _islas_raw_sintetico() -> pd.DataFrame:
    """DataFrame mínimo válido con la estructura que espera visualizacion_png."""
    islas = ['Tenerife', 'Gran Canaria', 'La Palma', 'La Gomera',
             'El Hierro', 'Lanzarote', 'Fuerteventura']
    filas = []
    for isla in islas:
        for anio in [2020, 2021, 2022]:
            filas.append({'isla': isla, 'año': anio, 'valor': 15000.0})
    return pd.DataFrame(filas)


_CODIGO_PLOTNINE_VALIDO = textwrap.dedent("""\
    from plotnine import ggplot, aes, geom_line, theme_minimal, labs, scale_color_manual
    color_map = {isla: '#D3D3D3' for isla in df['isla'].unique()}
    color_map['Tenerife'] = '#FF8C00'
    grafico = (
        ggplot(df, aes(x='año', y='valor', color='isla'))
        + geom_line()
        + scale_color_manual(values=color_map)
        + theme_minimal()
        + labs(title='Distribución de Renta por Isla - Canarias', x='Año', y='Valor (€)')
    )
""")


# ===========================================================================
# Tests de codigo_limpio_ia
# ===========================================================================

class TestCodigoLimpioIa:

    def test_codigo_sin_markdown_genera_funcion(self):
        """Código limpio sin markdown debe quedar envuelto en generar_plot(df)."""
        resultado = codigo_limpio_ia(_CODIGO_PLOTNINE_VALIDO)
        assert resultado.startswith("def generar_plot(df):"), \
            "El resultado debe empezar con 'def generar_plot(df):'"

    def test_codigo_sin_markdown_termina_return(self):
        """El wrapper debe añadir 'return grafico' al final."""
        resultado = codigo_limpio_ia(_CODIGO_PLOTNINE_VALIDO)
        assert resultado.strip().endswith("return grafico"), \
            "El resultado debe terminar con 'return grafico'"

    def test_extrae_bloque_markdown_python(self):
        """Respuesta con ```python...``` debe extraer solo el código interior."""
        con_markdown = f"Aquí tienes el código:\n```python\n{_CODIGO_PLOTNINE_VALIDO}\n```"
        resultado = codigo_limpio_ia(con_markdown)
        assert "```" not in resultado, \
            "El resultado no debe contener delimitadores de markdown"
        assert "def generar_plot(df):" in resultado

    def test_extrae_bloque_markdown_sin_lenguaje(self):
        """Respuesta con ```...``` (sin 'python') también debe extraerse."""
        con_markdown = f"```\n{_CODIGO_PLOTNINE_VALIDO}\n```"
        resultado = codigo_limpio_ia(con_markdown)
        assert "```" not in resultado
        assert "def generar_plot(df):" in resultado

    def test_codigo_original_indentado_en_funcion(self):
        """Cada línea del código original debe estar indentada dentro de la función."""
        resultado = codigo_limpio_ia(_CODIGO_PLOTNINE_VALIDO)
        lineas_cuerpo = resultado.splitlines()[1:]  # saltar la línea def
        for linea in lineas_cuerpo:
            if linea.strip():  # ignorar líneas vacías
                assert linea.startswith("    "), \
                    f"La línea debería estar indentada 4 espacios: {repr(linea)}"


# ===========================================================================
# Tests de visualizacion_png
# ===========================================================================

class TestVisualizacionPng:

    def test_genera_archivo_png(self):
        """Ejecutar código plotnine válido debe producir un archivo PNG."""
        codigo_envuelto = codigo_limpio_ia(_CODIGO_PLOTNINE_VALIDO)
        df = _islas_raw_sintetico()

        output = visualizacion_png(codigo_envuelto, df)

        ruta = Path(output.value)
        assert ruta.exists(), f"El PNG no fue creado en {ruta}"
        assert ruta.suffix == ".png"

    def test_devuelve_output_con_metadata_ruta(self):
        """El Output debe incluir la clave 'ruta' en sus metadatos."""
        codigo_envuelto = codigo_limpio_ia(_CODIGO_PLOTNINE_VALIDO)
        df = _islas_raw_sintetico()

        output = visualizacion_png(codigo_envuelto, df)

        assert 'ruta' in output.metadata, \
            "El Output debe tener la clave 'ruta' en metadata"

    def test_codigo_limpio_integrado_con_visualizacion(self):
        """Pipeline codigo_limpio_ia → visualizacion_png funciona end-to-end sin LLM."""
        # Simular respuesta del LLM con bloque markdown
        respuesta_llm = f"Claro, aquí el código:\n```python\n{_CODIGO_PLOTNINE_VALIDO}\n```"

        codigo_envuelto = codigo_limpio_ia(respuesta_llm)
        df = _islas_raw_sintetico()
        output = visualizacion_png(codigo_envuelto, df)

        assert Path(output.value).exists()

