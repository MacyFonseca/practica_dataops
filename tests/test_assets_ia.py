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

from src.assets_renta_canarias import codigo_limpio_ia, visualizacion_png


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

    def test_elimina_wildcard_import(self):
        """Líneas 'from X import *' deben ser eliminadas para evitar SyntaxError en exec()."""
        codigo_con_wildcard = (
            "from plotnine import *\n"
            "from pandas import *\n"
            + _CODIGO_PLOTNINE_VALIDO
        )
        resultado = codigo_limpio_ia(codigo_con_wildcard)
        assert "import *" not in resultado, \
            "El resultado no debe contener 'import *'"
        assert "def generar_plot(df):" in resultado

    def test_normaliza_comillas_tipograficas(self):
        """Comillas tipográficas (\u2018\u2019\u201c\u201d) deben convertirse a ASCII para evitar SyntaxError."""
        codigo_con_curly = _CODIGO_PLOTNINE_VALIDO.replace("'isla'", "\u2018isla\u2019").replace("'año'", "\u2018año\u2019")
        resultado = codigo_limpio_ia(codigo_con_curly)
        assert '\u2018' not in resultado and '\u2019' not in resultado, \
            "El resultado no debe contener comillas tipográficas simples"
        assert "def generar_plot(df):" in resultado

    def test_codigo_con_sintaxis_invalida_usa_fallback(self):
        """Código con SyntaxError irreparable debe devolver la implementación de fallback."""
        codigo_invalido = "def foo(\ngrafico = ggplot()\n"
        resultado = codigo_limpio_ia(codigo_invalido)
        assert "def generar_plot(df):" in resultado, \
            "El fallback debe contener la función generar_plot(df)"
        assert "return grafico" in resultado
        # El fallback debe ser código Python válido
        compile(resultado, '<fallback>', 'exec')

    def test_fallback_genera_png_ejecutable(self):
        """El código de fallback debe poder ejecutarse y generar un gráfico real."""
        codigo_invalido = "grafico = (p9.ggplot(df, p9.aes(x='año',\n"  # truncado
        codigo_envuelto = codigo_limpio_ia(codigo_invalido)
        df = _islas_raw_sintetico()
        output = visualizacion_png(codigo_envuelto, df)
        assert Path(output.value).exists()


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
