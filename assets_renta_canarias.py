"""
Assets para visualizar la distribución de rentas en Canarias siguiendo principios DataOps con Dagster.
Utiliza plotnine para crear gráficos de forma iterativa.
Integra información de municipios desde codislas.csv para enriquecer las visualizaciones.
"""

import pandas as pd
import os
from pathlib import Path
from dagster import asset
from plotnine import (
    ggplot, aes, geom_bar, geom_line, geom_point, 
    scale_x_continuous, scale_y_continuous,
    theme_minimal, labs, theme, element_text, facet_wrap
)

# CAPA 1: Extracción de Datos

@asset
def cargar_dataset_renta() -> pd.DataFrame:
    """
    Carga el dataset de distribución de rentas en Canarias desde el CSV.
    Este asset es la fuente de verdad para todos los análisis posteriores.
    """
    csv_path = Path(__file__).parent / "distribucion-renta-canarias.csv"
    
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
    """
    csv_path = Path(__file__).parent / "codislas.csv"
    
    df = pd.read_csv(csv_path, sep=';', encoding='latin-1')
    
    # Normalizar nombres de columnas
    df.columns = df.columns.str.strip()
    
    # Limpiar y normalizar nombres de municipios para coincidir con el dataset de rentas
    df['NOMBRE_NORMALIZADO'] = df['NOMBRE'].str.strip()
    
    print(f"\n Códigos de municipios cargados: {df.shape[0]} registros")
    print(f"Islas disponibles: {df['ISLA'].unique().tolist()}")
    
    return df


@asset
def dataset_renta_con_municipios(
    cargar_dataset_renta: pd.DataFrame,
    cargar_codigos_municipios: pd.DataFrame
) -> pd.DataFrame:
    """
    Integra la información de municipios del dataset de cargos con el dataset de rentas.
    Enriquece los datos con códigos de municipio, isla y normaliza nombres para mejor visualización.
    """
    df_renta = cargar_dataset_renta.copy()
    df_municipios = cargar_codigos_municipios.copy()
    
    # Crear diccionario de mapeo de nombres de municipios (caso insensible)
    mapeo_municipios = {}
    for idx, row in df_municipios.iterrows():
        nombre_limpio = row['NOMBRE_NORMALIZADO'].lower().strip()
        mapeo_municipios[nombre_limpio] = {
            'ISLA': row['ISLA'].strip(),
            'CMUN': row['CMUN'],
            'CISLA': row['CISLA'],
            'NOMBRE_MUNICIPIO': row['NOMBRE_NORMALIZADO']
        }
    
    # Normalizar nombres de territorio en el dataset de rentas
    df_renta['TERRITORIO_NORMALIZADO'] = df_renta['TERRITORIO#es'].str.lower().strip()
    
    # Realizar merge a través del nombre normalizado
    df_enriquecido = df_renta.merge(
        df_municipios[['NOMBRE_NORMALIZADO', 'ISLA', 'CMUN', 'CISLA']].rename(
            columns={'NOMBRE_NORMALIZADO': 'TERRITORIO_NORMALIZADO'}
        ),
        on='TERRITORIO_NORMALIZADO',
        how='left'
    )
    
    print(f"\n Dataset enriquecido: {df_enriquecido.shape[0]} registros")
    print(f"Municipios con información de isla: {df_enriquecido['ISLA'].notna().sum()}")
    
    return df_enriquecido



# CAPA 2: Transformación y Preparación de Datos

@asset
def dataset_renta_limpio(dataset_renta_con_municipios: pd.DataFrame) -> pd.DataFrame:
    """
    Limpia y prepara el dataset para el análisis.
    - Filtra valores nulos
    - Convierte tipos de datos
    - Crea columnas adicionales para el análisis
    - Utiliza datos enriquecidos con información de municipios
    """
    df = dataset_renta_con_municipios.copy()
    
    # Eliminar registros con valores nulos o confidenciales
    df = df.dropna(subset=['OBS_VALUE'])
    df = df[df['CONFIDENCIALIDAD_OBSERVACION#es'].isna()]
    
    # Convertir columnas a tipos apropiados
    df['OBS_VALUE'] = pd.to_numeric(df['OBS_VALUE'], errors='coerce')
    df['TIME_PERIOD#es'] = df['TIME_PERIOD#es'].astype(int)
    
    # Crear etiqueta de municipio enriquecida (con isla si está disponible)
    df['MUNICIPIO_ISLA'] = df.apply(
        lambda row: f"{row['TERRITORIO#es']} ({row['ISLA']})" if pd.notna(row['ISLA']) 
        else row['TERRITORIO#es'],
        axis=1
    )
    
    print(f"\n Dataset limpio: {df.shape[0]} registros válidos")
    print(f"Registros con información de isla: {df['ISLA'].notna().sum()}")
    
    return df


@asset
def datos_por_medida(dataset_renta_limpio: pd.DataFrame) -> dict:
    """
    Prepara datos agrupados por tipo de medida para procesamiento iterativo.
    Retorna un diccionario donde cada clave es una medida y el valor es su DataFrame.
    """
    medidas = dataset_renta_limpio['MEDIDAS#es'].unique()
    datos_agrupados = {}
    
    for medida in medidas:
        df_medida = dataset_renta_limpio[dataset_renta_limpio['MEDIDAS#es'] == medida].copy()
        datos_agrupados[medida] = df_medida.sort_values('TIME_PERIOD#es')
    
    print(f"\n Datos organizados por {len(medidas)} medidas diferentes")
    
    return datos_agrupados


# CAPA 3: Visualizaciones Iterativas

@asset
def generar_graficos_por_medida(datos_por_medida: dict):
    """
    Genera gráficos de forma iterativa para cada tipo de medida.
    Retorna una lista de diccionarios con medida, gráfico y nombre de archivo.
    """
    graficos_generados = []
    
    for medida, df_medida in datos_por_medida.items():
        # Generar nombre válido para archivo
        nombre_archivo = medida.lower().replace(" ", "_").replace("á", "a")
        
        # Crear el gráfico
        grafico = (
            ggplot(df_medida, aes(x='TIME_PERIOD#es', y='OBS_VALUE')) +
            geom_line(color='#1f77b4', size=1) +
            geom_point(color='#1f77b4', size=3) +
            labs(
                title=f'Distribución de {medida} (Canarias 2015-2023)',
                x='Año',
                y='Ingresso',
                caption='Fuente: Estadísticas de Ingresos en Canarias'
            ) +
            theme_minimal() +
            theme(
                figure_size=(10, 6),
                plot_title=element_text(size=14, weight='bold'),
                axis_title_x=element_text(size=11),
                axis_title_y=element_text(size=11),
            )
        )
        
        graficos_generados.append({
            'medida': medida,
            'grafico': grafico,
            'nombre_archivo': nombre_archivo
        })
    
    print(f"\n Se generaron {len(graficos_generados)} gráficos dinámicamente")
    
    return graficos_generados


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
    df_islas = dataset_renta_limpio[dataset_renta_limpio['ISLA'].notna()].copy()
    
    # Agrupar por isla y medida
    df_isla_medida = df_islas.groupby(['ISLA', 'MEDIDAS#es'])['OBS_VALUE'].sum().reset_index()
    
    grafico = (
        ggplot(df_isla_medida, aes(x='ISLA', y='OBS_VALUE', fill='MEDIDAS#es')) +
        geom_bar(stat='identity') +
        labs(
            title='Distribución de Fuentes de Ingreso por Isla (Canarias)',
            x='Isla',
            y='Ingreso Total',
            fill='Tipo de Ingreso',
            caption='Fuente: Estadísticas de Ingresos en Canarias\nDatos desglosados por municipios agrupados por isla'
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


# CAPA 4: Guardar Resultados

@asset
def guardar_graficos_dinamicos(generar_graficos_por_medida):
    """
    Guarda cada gráfico dinámico generado en archivos PNG.
    Este asset depende de los gráficos generados dinámicamente.
    """
    output_dir = Path(__file__).parent / "graficos_salida_pipeline"
    output_dir.mkdir(exist_ok=True)
    
    rutas_guardadas = []
    
    for grafico_data in generar_graficos_por_medida:
        try:
            ruta_archivo = output_dir / f"grafico_{grafico_data['nombre_archivo']}.png"
            grafico_data['grafico'].save(str(ruta_archivo), dpi=300, verbose=False)
            rutas_guardadas.append(str(ruta_archivo))
            print(f"✅ Guardado: {ruta_archivo}")
        except Exception as e:
            print(f"❌ Error guardando gráfico {grafico_data['medida']}: {e}")
    
    print(f"\n Gráficos dinámicos guardados en: {output_dir}")
    
    return rutas_guardadas

@asset
def guardar_graficos_resumen(grafico_distribucion_ingressos, grafico_tendencia_total, grafico_ingresos_por_isla):
    """
    Guarda los gráficos de resumen (distribución, tendencia e ingresos por isla).
    Ahora incluye visualizaciones enriquecidas con información de municipios.
    """
    output_dir = Path(__file__).parent / "graficos_salida_pipeline"
    output_dir.mkdir(exist_ok=True)
    
    # Guardar gráfico de distribución
    ruta_distribucion = output_dir / "01_distribucion_porcentajes.png"
    grafico_distribucion_ingressos.save(str(ruta_distribucion), dpi=300, verbose=False)
    print(f"✅ Guardado: {ruta_distribucion}")
    
    # Guardar gráfico de tendencia
    ruta_tendencia = output_dir / "02_tendencia_total.png"
    grafico_tendencia_total.save(str(ruta_tendencia), dpi=300, verbose=False)
    print(f"✅ Guardado: {ruta_tendencia}")
    
    # Guardar gráfico de ingresos por isla
    ruta_islas = output_dir / "03_ingresos_por_isla.png"
    grafico_ingresos_por_isla.save(str(ruta_islas), dpi=300, verbose=False)
    print(f"✅ Guardado: {ruta_islas}")
    
    print(f"\n Gráficos de resumen guardados en: {output_dir}")
    
    return {
        'distribucion': str(ruta_distribucion),
        'tendencia': str(ruta_tendencia),
        'ingresos_por_isla': str(ruta_islas)
    }
