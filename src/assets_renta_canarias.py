"""
Assets para visualizar la distribución de rentas en Canarias siguiendo principios DataOps con Dagster.
Utiliza plotnine para crear gráficos de forma iterativa.
Integra información de municipios desde codislas.csv
y niveles de estudios desde nivelestudios.xlsx para enriquecer las visualizaciones.
"""

import re
import shutil
import subprocess
import tempfile
import pandas as pd
import plotnine
from datetime import datetime
from pathlib import Path
from dagster import asset, Output, MetadataValue, AssetExecutionContext
from plotnine import (
    ggplot, aes, geom_bar, geom_line, geom_point,
    theme_minimal, labs, theme, element_text
)
import litellm

# CAPA 1: Extracción de Datos

@asset
def cargar_dataset_renta() -> pd.DataFrame:
    """
    Carga el dataset de distribución de rentas en Canarias desde el CSV.
    El dataset contiene información de ingresos por municipio, isla, año y tipo de ingreso.
    Normaliza los nombres de columnas para facilitar su uso en etapas posteriores."""
    csv_path = Path(__file__).parent.parent / "data" / "distribucion-renta-canarias.csv"

    df = pd.read_csv(csv_path)

    # Normalizar nombres de columnas
    df.columns = df.columns.str.strip()

    # Mostrar información del dataset
    print(f"\n Dataset cargado: {df.shape[0]} registros, {df.shape[1]} columnas")
    print(f"Años disponibles: {sorted(df['TIME_PERIOD#es'].unique())}")
    print(f"Medidas disponibles: {df['MEDIDAS#es'].unique().tolist()}")

    return df


@asset
def cargar_codigos_municipios() -> pd.DataFrame:
    """
    Carga el archivo codislas.csv que contiene la información de municipios.
    Proporciona códigos, nombres de municipios e islas para el enriquecimiento de datos.
    Normaliza dinámicamente formato de nombres invertidos (ej: "Palma, La" → "La Palma").
    """
    csv_path = Path(__file__).parent.parent / "data" / "codislas.csv"

    df = pd.read_csv(csv_path, sep=';', encoding='latin-1')

    # Normalizar nombres de columnas
    df.columns = df.columns.str.strip()

    # Función genérica para normalizar nombres invertidos
    # Invierte formato "Nombre, Artículo" (ej: "Palma, La") a "Artículo Nombre" (ej: "La Palma")
    def normalizar_nombre_invertido(nombre_str):
        nombre_str = nombre_str.strip()
        if ',' in nombre_str:
            partes = [p.strip() for p in nombre_str.split(',')]
            if len(partes) == 2:
                return f"{partes[1]} {partes[0]}"
        return nombre_str

    # Aplicar normalización a nombres de municipios
    df['NOMBRE_NORMALIZADO'] = df['NOMBRE'].str.strip().apply(normalizar_nombre_invertido)

    # Aplicar normalización a nombres de islas
    df['ISLA_NORMALIZADO'] = df['ISLA'].str.strip().apply(normalizar_nombre_invertido)

    print(f"\n Códigos de municipios cargados: {df.shape[0]} registros")
    print(f"Islas disponibles (normalizadas): {sorted(df['ISLA_NORMALIZADO'].unique().tolist())}")

    return df


@asset
def cargar_nivel_estudios() -> pd.DataFrame:
    """
    Carga el archivo nivelestudios.xlsx con información de nivel de estudios.
    Contiene datos desglosados por municipio, sexo, nacionalidad y período.
    """
    xlsx_path = Path(__file__).parent.parent / "data" / "nivelestudios.xlsx"

    df = pd.read_excel(xlsx_path, sheet_name=0)

    # Normalizar nombres de columnas
    df.columns = df.columns.str.strip()

    def normalizar_nombre_invertido(nombre_str):
        nombre_str = nombre_str.strip()
        if ',' in nombre_str:
            partes = [p.strip() for p in nombre_str.split(',')]
            if len(partes) == 2:
                return f"{partes[1]} {partes[0]}"
        return nombre_str

    # Extraer código de municipio y nombre del campo 'Municipios de 500 habitantes o más'
    # Formato: "35001 Agaete" -> código: 35001, nombre: Agaete
    df[['CMUN_EST', 'MUNICIPIO_EST']] = df['Municipios de 500 habitantes o más'].str.split(' ', n=1, expand=True)
    df['CMUN_EST'] = df['CMUN_EST'].astype(int)
    df['MUNICIPIO_EST'] = df['MUNICIPIO_EST'].str.strip().apply(normalizar_nombre_invertido)

    # Normalizar columnas para merge
    df['Periodo'] = pd.to_datetime(df['Periodo']).dt.year
    df.rename(columns={'Periodo': 'Año_Estudios'}, inplace=True)

    print(f"\n Nivel de estudios cargado: {df.shape[0]} registros")
    print(f"Períodos: {sorted(df['Año_Estudios'].unique())}")
    print(f"Niveles de estudios: {df['Nivel de estudios en curso'].nunique()} categorías")
    print(f"Municipios: {df['MUNICIPIO_EST'].nunique()} únicos")

    return df


@asset
def dataset_renta_con_municipios(
    cargar_dataset_renta: pd.DataFrame,
    cargar_codigos_municipios: pd.DataFrame
) -> pd.DataFrame:
    """
    Integra la información de municipios e islas con el dataset de rentas.
    Enriquece los datos haciendo merge en dos fases:
    1. Merge por nombre de municipio para registros municipales
    2. Merge por nombre de isla normalizado para registros agregados
    """
    df_renta = cargar_dataset_renta.copy()
    df_municipios = cargar_codigos_municipios.copy()

    # Normalizar nombres de territorio en el dataset de rentas
    df_renta['TERRITORIO#es'] = df_renta['TERRITORIO#es'].str.strip()
    df_renta['TERRITORIO_LIMPIO'] = df_renta['TERRITORIO#es'].str.strip().str.lower()

    # Preparar dataframe para merge por municipio
    df_municipios_merge = df_municipios[['NOMBRE_NORMALIZADO', 'ISLA_NORMALIZADO', 'ISLA', 'CMUN', 'CISLA']].copy()
    df_municipios_merge['NOMBRE_LIMPIO'] = df_municipios_merge['NOMBRE_NORMALIZADO'].str.lower()
    df_municipios_merge = df_municipios_merge[['NOMBRE_LIMPIO', 'ISLA_NORMALIZADO', 'ISLA', 'CMUN', 'CISLA']].drop_duplicates(subset=['NOMBRE_LIMPIO'])

    # Preparar dataframe para merge por isla
    df_islas_merge = df_municipios[['ISLA_NORMALIZADO', 'ISLA']].drop_duplicates()
    df_islas_merge['ISLA_LIMPIA'] = df_islas_merge['ISLA_NORMALIZADO'].str.lower()
    df_islas_merge = df_islas_merge[['ISLA_LIMPIA', 'ISLA_NORMALIZADO', 'ISLA']]

    # Merge con municipios
    df_enriquecido = df_renta.merge(
        df_municipios_merge.rename(columns={'NOMBRE_LIMPIO': 'TERRITORIO_LIMPIO'}),
        on='TERRITORIO_LIMPIO',
        how='left'
    )

    # Merge con islas para los registros que no encontraron municipio
    sin_isla = df_enriquecido[df_enriquecido['ISLA_NORMALIZADO'].isna()].copy()
    if len(sin_isla) > 0:
        sin_isla_merge = sin_isla.merge(
            df_islas_merge.rename(columns={'ISLA_LIMPIA': 'TERRITORIO_LIMPIO'}),
            on='TERRITORIO_LIMPIO',
            how='left'
        )
        # Actualizar solo las filas que obtuvieron información de isla
        mask = sin_isla_merge['ISLA_NORMALIZADO_y'].notna()
        df_enriquecido.loc[sin_isla.index, 'ISLA_NORMALIZADO'] = sin_isla_merge.loc[mask, 'ISLA_NORMALIZADO_y']
        df_enriquecido.loc[sin_isla.index, 'ISLA'] = sin_isla_merge.loc[mask, 'ISLA_y']

    # Usar ISLA_NORMALIZADO como columna principal, fallback a ISLA si es necesario
    df_enriquecido['ISLA_FINAL'] = df_enriquecido['ISLA_NORMALIZADO'].fillna(df_enriquecido['ISLA'])

    print(f"\n Dataset enriquecido: {df_enriquecido.shape[0]} registros")
    print(f"Registros con información de isla: {df_enriquecido['ISLA_FINAL'].notna().sum()}")
    print(f"Islas encontradas: {sorted(df_enriquecido[df_enriquecido['ISLA_FINAL'].notna()]['ISLA_FINAL'].unique().tolist())}")

    return df_enriquecido


@asset
def dataset_renta_con_estudios(
    dataset_renta_con_municipios: pd.DataFrame,
    cargar_nivel_estudios: pd.DataFrame
) -> pd.DataFrame:
    """
    Integra información de nivel de estudios con datos de rentas.
    Agrega datos de educación por municipio a nivel agregado (sin desglose por sexo/nacionalidad).
    Nota: Usa left join para mantener todos los datos de rentas, aunque algunos registros
    agregados no tengan correspondencia en estudios (que es normal).
    """
    df_renta = dataset_renta_con_municipios.copy()
    df_estudios = cargar_nivel_estudios.copy()

    # Obtener solo totales agregados (Sexo='Total', Nacionalidad no importa para agregado)
    df_estudios_total = df_estudios[df_estudios['Sexo'] == 'Total'].copy()

    # Agrupar por municipio, año y nivel de estudios
    df_estudios_agg = df_estudios_total.groupby(
        ['CMUN_EST', 'MUNICIPIO_EST', 'Año_Estudios', 'Nivel de estudios en curso']
    )['Total'].sum().reset_index()

    # Preparar nombres y columnas para merge
    df_estudios_agg['CMUN'] = df_estudios_agg['CMUN_EST']
    df_estudios_agg['TIME_PERIOD#es'] = df_estudios_agg['Año_Estudios'].astype(int)
    df_estudios_agg['Total_Estudiantes'] = df_estudios_agg['Total']

    # Merge con datos de renta (left join para mantener todos los registros de renta)
    # Esto mantiene registros agregados incluso si no tienen correspondencia en estudios
    df_enriquecido = df_renta.merge(
        df_estudios_agg[['CMUN', 'TIME_PERIOD#es', 'Nivel de estudios en curso', 'Total_Estudiantes']],
        on=['CMUN', 'TIME_PERIOD#es'],
        how='left'
    )

    print(f"\n Dataset con estudios integrado: {df_enriquecido.shape[0]} registros")
    print(f"Registros con información de nivel de estudios: {df_enriquecido['Nivel de estudios en curso'].notna().sum()}")

    return df_enriquecido


# CAPA 2: Transformación y Preparación de Datos

@asset
def dataset_estudios_limpio(
    cargar_nivel_estudios: pd.DataFrame,
    cargar_codigos_municipios: pd.DataFrame
) -> pd.DataFrame:
    """
    Prepara datos de nivel de estudios de forma independiente.
    Enriquece con información de municipios e islas para visualizaciones específicas de educación.
    """
    df_estudios = cargar_nivel_estudios.copy()
    df_municipios = cargar_codigos_municipios.copy()

    # Obtener solo totales agregados por sexo
    df_estudios = df_estudios[df_estudios['Sexo'] == 'Total'].copy()

    # Normalizar años
    df_estudios['Año'] = pd.to_datetime(df_estudios['Año_Estudios']).dt.year

    # Extraer CMUN del campo 'Municipios de 500 habitantes o más'
    df_estudios[['CMUN', 'MUNICIPIO']] = df_estudios['Municipios de 500 habitantes o más'].str.split(' ', n=1, expand=True)
    df_estudios['CMUN'] = df_estudios['CMUN'].astype(int)

    # Normalizar nombre de municipio
    def normalizar_nombre_invertido(nombre_str):
        nombre_str = nombre_str.strip()
        if ',' in nombre_str:
            partes = [p.strip() for p in nombre_str.split(',')]
            if len(partes) == 2:
                return f"{partes[1]} {partes[0]}"
        return nombre_str

    df_estudios['MUNICIPIO'] = df_estudios['MUNICIPIO'].str.strip().apply(normalizar_nombre_invertido)

    # Agregar información de isla
    df_municipios_isla = df_municipios[['CMUN', 'ISLA', 'ISLA_NORMALIZADO']].copy()
    df_estudios = df_estudios.merge(df_municipios_isla, on='CMUN', how='left')

    # Excluir categoría "Total" y Nan en nivel de estudios
    df_estudios = df_estudios[df_estudios['Nivel de estudios en curso'] != 'Total'].copy()
    df_estudios = df_estudios.dropna(subset=['Nivel de estudios en curso'])

    print(f"\n Dataset de estudios limpio: {df_estudios.shape[0]} registros válidos")
    print(f"Años disponibles: {sorted(df_estudios['Año'].unique())}")
    print(f"Municipios: {df_estudios['MUNICIPIO'].nunique()} únicos")
    print(f"Islas: {df_estudios['ISLA_NORMALIZADO'].nunique()} islas")

    return df_estudios


@asset
def dataset_renta_limpio(dataset_renta_con_estudios: pd.DataFrame) -> pd.DataFrame:
    """
    Limpia y prepara el dataset para el análisis.
    - Filtra valores nulos
    - Convierte tipos de datos
    - Crea columnas adicionales para el análisis
    - Utiliza datos enriquecidos con información de municipios y nivel de estudios
    """
    df = dataset_renta_con_estudios.copy()

    # Eliminar registros con valores nulos o confidenciales
    df = df.dropna(subset=['OBS_VALUE'])
    df = df[df['CONFIDENCIALIDAD_OBSERVACION#es'].isna()]

    # Convertir columnas a tipos apropiados
    df['OBS_VALUE'] = pd.to_numeric(df['OBS_VALUE'], errors='coerce')
    df['TIME_PERIOD#es'] = df['TIME_PERIOD#es'].astype(int)

    # Crear etiqueta de municipio enriquecida (con isla si está disponible)
    df['MUNICIPIO_ISLA'] = df.apply(
        lambda row: f"{row['TERRITORIO#es']} ({row['ISLA_FINAL']})" if pd.notna(row['ISLA_FINAL'])
        else row['TERRITORIO#es'],
        axis=1
    )

    print(f"\n Dataset limpio: {df.shape[0]} registros válidos")
    print(f"Registros con información de isla: {df['ISLA_FINAL'].notna().sum()}")
    print(f"Registros con información de nivel de estudios: {df['Nivel de estudios en curso'].notna().sum()}")

    return df

@asset
def grafico_distribucion_ingressos(dataset_renta_limpio: pd.DataFrame):
    """
    Crea un gráfico que muestra la distribución de las medidas para el último año disponible.
    Incluye información de municipios cuando está disponible.
    """
    # Filtrar el último año
    ultimo_año = dataset_renta_limpio['TIME_PERIOD#es'].max()
    df_ultimo_año = dataset_renta_limpio[dataset_renta_limpio['TIME_PERIOD#es'] == ultimo_año]

    # Agrupar por medida y sumar
    df_agrupado = df_ultimo_año.groupby('MEDIDAS#es')['OBS_VALUE'].sum().reset_index()

    grafico = (
        ggplot(df_agrupado, aes(x='MEDIDAS#es', y='OBS_VALUE', fill='MEDIDAS#es')) +
        geom_bar(stat='identity', show_legend=False) +
        labs(
            title=f'Distribución de Fuentes de Ingreso - Canarias {ultimo_año}\n(por Municipios)',
            x='Tipo de Ingreso',
            y='Ingreso Total',
            caption='Fuente: Estadísticas de Ingresos en Canarias\nDatos integrados con información de municipios'
        ) +
        theme_minimal() +
        theme(
            figure_size=(12, 6),
            plot_title=element_text(size=14, weight='bold'),
            axis_title_x=element_text(size=11),
            axis_title_y=element_text(size=11),
            axis_text_x=element_text(angle=45, hjust=1),
        )
    )

    return grafico


@asset
def grafico_tendencia_total(dataset_renta_limpio: pd.DataFrame):
    """
    Crea un gráfico que muestra la tendencia temporal de la suma total de ingresos.
    Integra datos a nivel de municipio.
    """
    # Agrupar por año y sumar los valores
    df_tendencia = dataset_renta_limpio.groupby('TIME_PERIOD#es')['OBS_VALUE'].sum().reset_index()
    df_tendencia.columns = ['Año', 'Total_Ingresos']

    grafico = (
        ggplot(df_tendencia, aes(x='Año', y='Total_Ingresos')) +
        geom_line(color='#2ca02c', size=1.2) +
        geom_point(color='#2ca02c', size=3) +
        labs(
            title='Tendencia Total de Ingresos en Canarias (2015-2023)\n(Agregado a nivel de Municipios)',
            x='Año',
            y='Ingreso Total',
            caption='Fuente: Estadísticas de Ingresos en Canarias\nDatos integrados con información de municipios'
        ) +
        theme_minimal() +
        theme(
            figure_size=(10, 6),
            plot_title=element_text(size=14, weight='bold'),
            axis_title_x=element_text(size=11),
            axis_title_y=element_text(size=11),
        )
    )

    return grafico


@asset
def grafico_ingresos_por_isla(dataset_renta_limpio: pd.DataFrame):
    """
    Crea un gráfico que muestra la distribución de ingresos por isla.
    Visualiza cómo se distribuyen las diferentes medidas de ingreso en cada isla.
    """
    # Filtrar solo los registros con información de isla
    df_islas = dataset_renta_limpio[dataset_renta_limpio['ISLA_FINAL'].notna()].copy()

    # Agrupar por isla y medida
    df_isla_medida = df_islas.groupby(['ISLA_FINAL', 'MEDIDAS#es'])['OBS_VALUE'].sum().reset_index()

    grafico = (
        ggplot(df_isla_medida, aes(x='ISLA_FINAL', y='OBS_VALUE', fill='MEDIDAS#es')) +
        geom_bar(stat='identity') +
        labs(
            title='Distribución de Fuentes de Ingreso por Isla (Canarias)',
            x='Isla',
            y='Ingreso Total',
            fill='Tipo de Ingreso',
            caption='Fuente: Estadísticas de Ingresos en Canarias\nDatos agrupados por isla'
        ) +
        theme_minimal() +
        theme(
            figure_size=(14, 6),
            plot_title=element_text(size=14, weight='bold'),
            axis_title_x=element_text(size=11),
            axis_title_y=element_text(size=11),
            axis_text_x=element_text(angle=45, hjust=1),
            legend_position='right'
        )
    )

    return grafico


@asset
def grafico_nivel_estudios_distribucion(dataset_estudios_limpio: pd.DataFrame):
    """
    Crea un gráfico de distribución de nivel de estudios en Canarias.
    Muestra la proporción de estudiantes en cada nivel educativo (años 2021-2023).
    Utiliza datos de estudios independientes sin dependencia de rentas.
    """
    df_estudios = dataset_estudios_limpio.copy()

    # Agrupar por nivel de estudios sumando estudiantes
    df_nivel = df_estudios.groupby('Nivel de estudios en curso')['Total'].sum().reset_index()

    # Crear etiquetas más cortas para mejor visualización
    nivel_labels = {
        'Educación primaria e inferior': 'Primaria e inferior',
        'Primera etapa de Educación Secundaria y similar': 'ESO o similar',
        'Segunda etapa de educación secundaria, con orientación general': 'Bachillerato',
        'Segunda etapa de Educación Secundaria, con orientación profesional (con y sin continuidad en la educación superior); Educación postsecundaria no superior': 'Formación Profissional',
        'Educación superior': 'Superior',
        'Cursa estudios pero no hay información sobre los mismos': 'Información faltante',
        'No cursa estudios': 'No cursa'
    }
    df_nivel['Nivel_Corto'] = df_nivel['Nivel de estudios en curso'].map(nivel_labels)

    grafico = (
        ggplot(df_nivel, aes(x='Nivel_Corto', y='Total', fill='Nivel_Corto')) +
        geom_bar(stat='identity', show_legend=False) +
        labs(
            title='Distribución de Nivel de Estudios en Canarias',
            x='Nivel de Estudios',
            y='Total de Estudiantes',
            caption='Fuente: Estadísticas de Nivel de Estudios 2021-2023'
        ) +
        theme_minimal() +
        theme(
            figure_size=(14, 6),
            plot_title=element_text(size=14, weight='bold'),
            axis_title_x=element_text(size=11),
            axis_title_y=element_text(size=11),
            axis_text_x=element_text(angle=45, hjust=1),
        )
    )

    return grafico

# CAPA 4: Guardar Resultados

# @asset
# def guardar_graficos_dinamicos(generar_graficos_por_medida):
#     """
#     Guarda cada gráfico dinámico generado en archivos PNG.
#     Este asset depende de los gráficos generados dinámicamente.
#     """
#     output_dir = Path(__file__).parent.parent / "outputs" / "pipeline"
#     output_dir.mkdir(exist_ok=True)

#     rutas_guardadas = []

#     for grafico_data in generar_graficos_por_medida:
#         try:
#             ruta_archivo = output_dir / f"grafico_{grafico_data['nombre_archivo']}.png"
#             grafico_data['grafico'].save(str(ruta_archivo), dpi=300, verbose=False)
#             rutas_guardadas.append(str(ruta_archivo))
#             print(f"✅ Guardado: {ruta_archivo}")
#         except Exception as e:
#             print(f"❌ Error guardando gráfico {grafico_data['medida']}: {e}")

#     print(f"\n Gráficos dinámicos guardados en: {output_dir}")

#     return rutas_guardadas

@asset
def guardar_graficos_resumen(
    grafico_distribucion_ingressos,
    grafico_tendencia_total,
    grafico_ingresos_por_isla,
    grafico_nivel_estudios_distribucion
):
    """
    Guarda todos los gráficos de resumen generados en PNG.
    Incluye visualizaciones de ingresos, municipios, islas y nivel de estudios.
    """
    output_dir = Path(__file__).parent.parent / "outputs" / "pipeline"
    output_dir.mkdir(exist_ok=True)

    # Guardar gráfico de distribución de ingresos
    ruta_distribucion = output_dir / "01_distribucion_ingressos.png"
    grafico_distribucion_ingressos.save(str(ruta_distribucion), dpi=300, verbose=False)
    print(f"✅ Guardado: {ruta_distribucion}")

    # Guardar gráfico de tendencia de ingresos
    ruta_tendencia = output_dir / "02_tendencia_ingressos.png"
    grafico_tendencia_total.save(str(ruta_tendencia), dpi=300, verbose=False)
    print(f"✅ Guardado: {ruta_tendencia}")

    # Guardar gráfico de ingresos por isla
    ruta_islas = output_dir / "03_ingresos_por_isla.png"
    grafico_ingresos_por_isla.save(str(ruta_islas), dpi=300, verbose=False)
    print(f"✅ Guardado: {ruta_islas}")

    # Guardar gráfico de distribución de nivel de estudios
    ruta_nivel_dist = output_dir / "04_nivel_estudios_distribucion.png"
    grafico_nivel_estudios_distribucion.save(str(ruta_nivel_dist), dpi=300, verbose=False)
    print(f"✅ Guardado: {ruta_nivel_dist}")

    print(f"\n📊 Gráficos de resumen guardados en: {output_dir}")

    return {
        'distribucion': str(ruta_distribucion),
        'tendencia': str(ruta_tendencia),
        'ingresos_por_isla': str(ruta_islas),
        'nivel_estudios': str(ruta_nivel_dist)
    }


# CAPA IA: Generación de gráficos con LLM

@asset
def islas_raw(dataset_renta_limpio: pd.DataFrame) -> pd.DataFrame:
    """
    Agrega el dataset de renta limpio por isla y año, calculando el valor medio de OBS_VALUE.
    Produce un DataFrame con columnas ['isla', 'año', 'valor'] para uso en el pipeline de IA.
    """
    df = dataset_renta_limpio.copy()

    df_islas = (
        df.groupby(['ISLA_FINAL', 'TIME_PERIOD#es'])['OBS_VALUE']
        .mean()
        .reset_index()
    )

    df_islas = df_islas.rename(columns={
        'ISLA_FINAL': 'isla',
        'TIME_PERIOD#es': 'año',
        'OBS_VALUE': 'valor',
    })

    print(f"\n Dataset islas_raw: {df_islas.shape[0]} registros")
    print(f"Islas: {sorted(df_islas['isla'].unique())}")
    print(f"Años: {sorted(df_islas['año'].unique())}")

    return df_islas


@asset
def template_ia(islas_raw: pd.DataFrame) -> dict:
    """
    Construye el prompt para un LLM siguiendo la gramática de gráficos de Wickham.
    El prompt describe un gráfico de líneas del valor de renta por isla y año,
    destacando Tenerife en naranja y el resto en gris claro mediante scale_color_manual.
    Devuelve un diccionario con el modelo y los mensajes de sistema y usuario.
    """
    # Construir color_map dinámicamente: todas las islas en gris, Tenerife en naranja
    islas = sorted(islas_raw['isla'].dropna().unique())
    color_map = {isla: '#D3D3D3' for isla in islas}
    color_map['Tenerife'] = '#FF8C00'

    color_map_str = repr(color_map)

    system_message = (
        "Eres un experto en visualización de datos con Python y la librería plotnine. "
        "Tu única tarea es generar código Python ejecutable. "
        "Responde ÚNICAMENTE con código Python puro. "
        "No incluyas bloques de markdown (``` o similares), no incluyas texto explicativo "
        "ni descripciones fuera del código. "
        "El código debe poder ejecutarse directamente sin ninguna modificación. "
        "REGLAS ESTRICTAS que debes cumplir sin excepción:\n"
        "1. NUNCA crees un DataFrame, ni con pd.DataFrame(), ni con diccionarios, ni con ningún otro método. "
        "El DataFrame ya existe en la variable `df` y debes usarlo directamente.\n"
        "2. La variable del gráfico DEBE llamarse exactamente `grafico` (sin tilde, sin acento). "
        "Está terminantemente prohibido usar `gráfico` u otro nombre."
    )

    # Incrustar muestra real del DataFrame para que el LLM no genere sus propios datos
    muestra_df = islas_raw.head(10).to_string(index=False)
    islas_disponibles = sorted(islas_raw['isla'].dropna().unique().tolist())
    años_disponibles = sorted(islas_raw['año'].dropna().unique().tolist())

    user_message = (
        "Genera código Python con plotnine que produzca el siguiente gráfico "
        "siguiendo la gramática de gráficos de Wickham:\n\n"
        "DATOS:\n"
        "  - Dispones de un DataFrame ya cargado llamado `df` con las columnas:\n"
        "      * `isla`  (str)   — nombre de la isla\n"
        "      * `año`   (int)   — año de la observación\n"
        "      * `valor` (float) — valor medio de renta (€)\n"
        "  - NO debes crear el DataFrame ni leer ningún archivo. Ya viene dado.\n"
        f"  - Islas presentes: {islas_disponibles}\n"
        f"  - Años presentes:  {años_disponibles}\n"
        f"  - Muestra de los datos reales:\n{muestra_df}\n\n"
        "PROHIBICIONES (incumplirlas hará que el código falle):\n"
        "  - NO crees un DataFrame. No uses pd.DataFrame(), pd.read_csv() ni ningún equivalente.\n"
        "  - USA SIEMPRE la variable `df` que ya existe con los datos reales mostrados arriba.\n"
        "  - El nombre de la variable del gráfico debe ser exactamente `grafico` (sin tilde, sin acento).\n\n"
        "ESTÉTICAS (aes):\n"
        "  - Eje X → `año`\n"
        "  - Eje Y → `valor`\n"
        "  - Color → `isla`\n\n"
        "GEOMETRÍA:\n"
        "  - geom_line()\n\n"
        "ESCALA DE COLOR:\n"
        f"  - scale_color_manual con el siguiente diccionario de valores: {color_map_str}\n"
        "  - Esto resalta Tenerife en naranja (#FF8C00) y deja el resto en gris claro (#D3D3D3)\n\n"
        "TEMA:\n"
        "  - theme_minimal()\n\n"
        "ETIQUETAS:\n"
        "  - Título: 'Distribución de Renta por Isla - Canarias'\n"
        "  - Eje X: 'Año'\n"
        "  - Eje Y: 'Valor (€)'\n\n"
        "Importa las funciones necesarias de plotnine y asigna el gráfico a una variable llamada `grafico`."
    )

    return {
        'model': 'ollama/llama3.1:8b',
        'messages': [
            {'role': 'system', 'content': system_message},
            {'role': 'user',   'content': user_message},
        ],
    }

@asset
def codigo_generado_ia(template_ia: dict) -> str:
    """
    Envía el prompt construido por template_ia al LLM mediante litellm.
    Devuelve el contenido de la respuesta como string sin procesar.
    """
    response = litellm.completion(
        model=template_ia['model'],
        messages=template_ia['messages'],
    )
    codigo = response.choices[0].message.content
    print(f" ========================= Respuesta recibida del LLM  ==============================")
    print(f"\n{codigo}\n")
    print(f" ====================================================================================")
    return codigo

@asset
def codigo_limpio_ia(context: AssetExecutionContext, codigo_generado_ia: str) -> Output[str]:
    """
    Limpia la respuesta del LLM eliminando bloques de markdown si los hay.
    Envuelve el código en una función generar_plot(df) para su posterior ejecución.
    El código resultante puede ejecutarse directamente con exec().
    Devuelve un Output con metadatos que indican si se usó el fallback,
    haciéndolo visible en el lineage de Dagster.
    """
    codigo = codigo_generado_ia

    # Extraer código de bloque markdown si existe (```python...``` o ```...```)
    match = re.search(r'```(?:python)?\n?(.*?)```', codigo, re.DOTALL)
    if match:
        codigo = match.group(1)
        print(" Bloque markdown detectado y extraído")

    codigo = codigo.strip()

    # Normalizar comillas tipográficas (‘’“”) a comillas ASCII estándar
    # Los LLMs las generan habitualmente y causan SyntaxError en Python
    TABLA_COMILLAS = str.maketrans({
        '\u2018': "'",  # LEFT SINGLE QUOTATION MARK
        '\u2019': "'",  # RIGHT SINGLE QUOTATION MARK
        '\u201c': '"',  # LEFT DOUBLE QUOTATION MARK
        '\u201d': '"',  # RIGHT DOUBLE QUOTATION MARK
        '\u2032': "'",  # PRIME
        '\u2033': '"',  # DOUBLE PRIME
    })
    codigo_normalizado = codigo.translate(TABLA_COMILLAS)
    if codigo_normalizado != codigo:
        print(" Comillas tipográficas normalizadas a ASCII")
    codigo = codigo_normalizado
    # Eliminar imports con wildcard (from X import *) — no están permitidos dentro
    # de una función y son redundantes porque el entorno ya tiene todos los símbolos
    lineas_filtradas = [
        linea for linea in codigo.splitlines()
        if not re.match(r'^\s*from\s+\S+\s+import\s+\*', linea)
    ]
    n_eliminadas = len(codigo.splitlines()) - len(lineas_filtradas)
    if n_eliminadas:
        print(f" {n_eliminadas} línea(s) 'import *' eliminadas")
    codigo = "\n".join(lineas_filtradas)

    # Normalizar nombre de variable: reemplazar `gráfico` (con tilde) → `grafico`
    # El LLM a veces ignora la instrucción y usa el nombre con acento
    if 'gráfico' in codigo:
        codigo = codigo.replace('gráfico', 'grafico')
        print(" Variable 'gráfico' renombrada a 'grafico' (sin tilde)")

    # Eliminar cualquier línea que reasigne `df` desde cero (pd.DataFrame, pd.read_csv,
    # literales Ellipsis `...`, dicts, listas, etc.) ignorando solo las transformaciones
    # legítimas sobre el propio df, como `df = df[condicion]` o `df = df.copy()`.
    PATRON_CREACION_DF = re.compile(
        r'^\s*df\s*=\s*(?!df\b)',
        re.IGNORECASE,
    )
    lineas_sin_df = [l for l in codigo.splitlines() if not PATRON_CREACION_DF.match(l)]
    n_df_eliminadas = len(codigo.splitlines()) - len(lineas_sin_df)
    if n_df_eliminadas:
        print(f" {n_df_eliminadas} línea(s) de creación de DataFrame eliminadas")
    codigo = "\n".join(lineas_sin_df)

    print(f" ========================= Código generado por la IA (limpio) ==============================")
    print(f"\n{codigo}\n")
    print(f" ===========================================================================================")

    # Validar sintaxis antes de envolver — si falla, usar implementación de fallback
    try:
        compile(codigo, '<codigo_ia>', 'exec')
    except SyntaxError as e:
        motivo = str(e)
        context.log.warning(
            f"[codigo_limpio_ia] SyntaxError en el código del LLM: {motivo}. "
            "Se ha activado el fallback — el gráfico generado NO proviene de la IA."
        )
        # Fallback: implementación hardcodeada que sabemos que funciona.
        # Usa búsqueda case-insensitive para 'tenerife' evitando fallos por variantes de nombre.
        codigo_envuelto = (
            "def generar_plot(df):\n"
            "    from plotnine import ggplot, aes, geom_line, theme_minimal, labs, scale_color_manual\n"
            "    islas = sorted(df['isla'].dropna().unique())\n"
            "    color_map = {isla: '#D3D3D3' for isla in islas}\n"
            "    tenerife = next((i for i in color_map if 'tenerife' in i.lower()), None)\n"
            "    if tenerife:\n"
            "        color_map[tenerife] = '#FF8C00'\n"
            "    grafico = (\n"
            "        ggplot(df, aes(x='año', y='valor', color='isla'))\n"
            "        + geom_line()\n"
            "        + scale_color_manual(values=color_map)\n"
            "        + theme_minimal()\n"
            "        + labs(\n"
            "            title='Distribución de Renta por Isla - Canarias',\n"
            "            x='Año',\n"
            "            y='Valor (€)',\n"
            "        )\n"
            "    )\n"
            "    return grafico"
        )
        return Output(
            value=codigo_envuelto,
            metadata={
                "fallback_activado": MetadataValue.bool(True),
                "motivo_fallback": MetadataValue.text(motivo),
                "longitud_codigo": MetadataValue.int(len(codigo_envuelto)),
            },
        )

    # Corregir antipatrón REPL: "grafico = _"
    # El LLM usa _ para capturar el último resultado de la consola interactiva,
    # pero dentro de exec() la variable _ no existe. Se busca la línea con ggplot
    # y se envuelve toda la expresión en "grafico = (...)".
    if re.search(r'^\s*grafico\s*=\s*_\s*$', codigo, re.MULTILINE):
        lineas = codigo.splitlines()
        idx_repl = next(
            i for i, l in enumerate(lineas)
            if re.match(r'^\s*grafico\s*=\s*_\s*$', l)
        )
        # Localizar la primera línea del bloque ggplot antes de "grafico = _"
        j = idx_repl - 1
        while j > 0 and not re.search(r'\bggplot\b', lineas[j]):
            j -= 1
        # Reconstruir como asignación directa (la continuación explícita \ ya no es necesaria
        # porque todo queda dentro de paréntesis implícitos de la asignación)
        expr = [l.rstrip().rstrip('\\') for l in lineas[j:idx_repl]]
        codigo = '\n'.join(lineas[:j] + ['grafico = ('] + expr + [')'] + lineas[idx_repl + 1:])
        print(" Patrón REPL 'grafico = _' reescrito como asignación directa")

    # Envolver en función generar_plot(df) indentando cada línea
    lineas_indentadas = "\n".join("    " + linea for linea in codigo.splitlines())
    codigo_envuelto = f"def generar_plot(df):\n{lineas_indentadas}\n    return grafico"

    context.log.info(f"[codigo_limpio_ia] Código limpio listo ({len(codigo_envuelto)} caracteres)")
    return Output(
        value=codigo_envuelto,
        metadata={
            "fallback_activado": MetadataValue.bool(False),
            "longitud_codigo": MetadataValue.int(len(codigo_envuelto)),
        },
    )

@asset
def visualizacion_png(codigo_limpio_ia: str, islas_raw: pd.DataFrame) -> Output:
    """
    Ejecuta el código Python generado por la IA en un entorno controlado.
    Llama a generar_plot(islas_raw) para obtener el gráfico y lo guarda como PNG.
    Devuelve un Output de Dagster con la ruta del archivo generado como metadato.
    """
    # Preparar entorno de ejecución con plotnine y pandas disponibles
    entorno_ejecucion = globals().copy()
    for nombre in dir(plotnine):
        entorno_ejecucion[nombre] = getattr(plotnine, nombre)
    entorno_ejecucion['pd'] = pd
    entorno_ejecucion['p9'] = plotnine  # alias usado habitualmente por los LLMs

    # Ejecutar el código generado por la IA
    exec(codigo_limpio_ia, entorno_ejecucion)  # nosec B102

    # Llamar a la función generada
    grafico = entorno_ejecucion['generar_plot'](islas_raw)

    # Guardar el gráfico como PNG
    output_dir = Path(__file__).parent.parent / "outputs" / "pipeline"
    output_dir.mkdir(exist_ok=True)
    ruta = output_dir / "visualizacion_ia.png"
    grafico.save(str(ruta), dpi=300, verbose=False)

    print(f"\n Gráfico IA guardado en: {ruta}")

    return Output(
        value=str(ruta),
        metadata={
            'ruta': MetadataValue.path(str(ruta)),
        },
    )


# CAPA 5: Publicación en GitHub Pages

@asset
def publicar_en_ghpages(context: AssetExecutionContext, visualizacion_png: str) -> Output:
    """
    Sube visualizacion_ia.png a la rama gh-pages del repositorio.
    Genera un index.html mínimo que muestra el gráfico y lo publica en GitHub Pages.
    Si el push falla, emite un warning sin interrumpir el pipeline.
    URL pública: https://MacyFonseca.github.io/practica_dataops/
    """
    REPO_ROOT = Path(__file__).parent
    GHPAGES_URL = "https://MacyFonseca.github.io/practica_dataops/"
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    def _run(cmd: list, cwd: str) -> None:
        subprocess.run(cmd, cwd=cwd, check=True, capture_output=True, text=True)  # nosec B603

    with tempfile.TemporaryDirectory() as tmpdir:
        try:
            # Comprobar si la rama gh-pages ya existe en el remoto
            result = subprocess.run(
                ["git", "ls-remote", "--exit-code", "--heads", "origin", "gh-pages"],
                cwd=str(REPO_ROOT),
                capture_output=True,
            )
            rama_existe = result.returncode == 0

            if rama_existe:
                _run(["git", "worktree", "add", tmpdir, "gh-pages"], cwd=str(REPO_ROOT))
            else:
                # Crear rama orphan la primera vez
                _run(
                    ["git", "worktree", "add", "--orphan", "-b", "gh-pages", tmpdir],
                    cwd=str(REPO_ROOT),
                )

            # Copiar solo visualizacion_ia.png al worktree
            shutil.copy(visualizacion_png, str(Path(tmpdir) / "visualizacion_ia.png"))

            # Generar index.html mínimo
            html = f"""<!DOCTYPE html>
<html lang="es">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>Distribución de Renta por Isla - Canarias</title>
  <style>
    body {{ font-family: sans-serif; max-width: 960px; margin: 40px auto; padding: 0 20px; background: #f8f9fa; }}
    h1 {{ color: #333; }}
    img {{ max-width: 100%; border: 1px solid #ddd; border-radius: 4px; }}
    p.ts {{ color: #888; font-size: 0.85em; margin-top: 8px; }}
  </style>
</head>
<body>
  <h1>Distribución de Renta por Isla — Canarias</h1>
  <p>Gráfico generado automáticamente por el pipeline DataOps con IA (Dagster + LLM).</p>
  <img src="visualizacion_ia.png" alt="Distribución de Renta por Isla - Canarias">
  <p class="ts">Última actualización: {timestamp}</p>
</body>
</html>
"""
            (Path(tmpdir) / "index.html").write_text(html, encoding="utf-8")

            # Configurar identidad git mínima en el worktree si no hay una global
            _run(["git", "add", "visualizacion_ia.png", "index.html"], cwd=tmpdir)
            _run(
                ["git", "commit", "-m", f"chore: actualizar gráfico IA [{timestamp}]"],
                cwd=tmpdir,
            )
            _run(["git", "push", "origin", "gh-pages"], cwd=tmpdir)

            context.log.info(f"[publicar_en_ghpages] Gráfico publicado en {GHPAGES_URL}")
            print(f"\n✅ Gráfico publicado en: {GHPAGES_URL}")

        except subprocess.CalledProcessError as e:
            msg = (
                f"[publicar_en_ghpages] El push a gh-pages falló: "
                f"{e.stderr or e.stdout or str(e)}"
            )
            context.log.warning(msg)
            print(f"\n⚠️  {msg}")
        finally:
            # Limpiar el worktree aunque haya fallado el push
            subprocess.run(
                ["git", "worktree", "remove", "--force", tmpdir],
                cwd=str(REPO_ROOT),
                capture_output=True,
            )

    return Output(
        value=GHPAGES_URL,
        metadata={
            "url": MetadataValue.url(GHPAGES_URL),
            "imagen": MetadataValue.path(visualizacion_png),
            "timestamp": MetadataValue.text(timestamp),
        },
    )
