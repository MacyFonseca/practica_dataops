"""
Laboratorio iterativo para diseñar gráficos de distribución de rentas en Canarias.
Exploración sin automatización, script simple y directo para experimentar con visualizaciones.
"""

import pandas as pd
from pathlib import Path
from plotnine import (
    ggplot, aes, geom_bar, geom_line, geom_point, geom_area,
    facet_wrap, scale_x_continuous,
    theme_minimal, labs, theme, element_text
)

# PASO 1: CARGAR Y EXPLORAR DATOS

print("\n" + "="*70)
print("LAB-RENTA: Exploración Iterativa de Rentas en Canarias")
print("="*70)

# Cargar dataset
csv_path = Path(__file__).parent / "distribucion-renta-canarias.csv"
df = pd.read_csv(csv_path)

# Normalizar columnas
df.columns = df.columns.str.strip()

print("\n📊 Información del Dataset")
print("-" * 70)
print(f"Forma: {df.shape[0]} registros × {df.shape[1]} columnas")

print(f"\n¿Hay valores faltantes?")
print(df.isnull().sum())

print(f"\nAños disponibles: {sorted(df['TIME_PERIOD#es'].unique())}")
print(f"\nMedidas de ingreso:")
for medida in df['MEDIDAS#es'].unique():
    print(f"  • {medida}")


# PASO 2: LIMPIAR DATOS

print("\n" + "="*70)
print("Limpieza de Datos")
print("="*70)

# Eliminar nulos y confidenciales
df_limpio = df.dropna(subset=['OBS_VALUE']).copy()
df_limpio = df_limpio[df_limpio['CONFIDENCIALIDAD_OBSERVACION#es'].isna()]

# Convertir valores a numérico
df_limpio['OBS_VALUE'] = pd.to_numeric(df_limpio['OBS_VALUE'], errors='coerce')
df_limpio['TIME_PERIOD#es'] = df_limpio['TIME_PERIOD#es'].astype(int)

# Eliminar filas con valores no numéricos
df_limpio = df_limpio.dropna(subset=['OBS_VALUE'])

print(f"✓ Registros originales: {df.shape[0]}")
print(f"✓ Registros limpios: {df_limpio.shape[0]}")
print(f"✓ Registros eliminados: {df.shape[0] - df_limpio.shape[0]}")


# PASO 3: GRÁFICO 1 - Distribuición Actual (Última año)

print("\n" + "="*70)
print("GRÁFICO 1: Distribución Actual de Fuentes de Ingreso (Último Año)")
print("="*70)

ultimo_año = df_limpio['TIME_PERIOD#es'].max()
print(f"\nAño seleccionado: {ultimo_año}")

df_ultimo = df_limpio[df_limpio['TIME_PERIOD#es'] == ultimo_año].copy()

print("\nDatos para el gráfico:")
print(df_ultimo[['MEDIDAS#es', 'OBS_VALUE']])

# Crear gráfico
grafico_distribucion = (
    ggplot(df_ultimo, aes(x='MEDIDAS#es', y='OBS_VALUE', fill='MEDIDAS#es')) +
    geom_bar(stat='identity', show_legend=False) +
    labs(
        title=f'Distribución de Fuentes de Ingreso en Canarias - {ultimo_año}',
        x='Tipo de Ingreso',
        y='Ingresso Total',
        caption='Fuente: Estadísticas de Ingresos en Canarias'
    ) +
    theme_minimal() +
    theme(
        figure_size=(12, 6),
        plot_title=element_text(size=14, weight='bold'),
        axis_title=element_text(size=11),
        axis_text_x=element_text(angle=45, hjust=1),
    )
)

print("\n✓ Gráfico creado exitosamente")
grafico_distribucion.show()


# PASO 4: GRÁFICO 2 - Tendencia Temporal 

print("\n" + "="*70)
print("GRÁFICO 2: Tendencia Temporal de Ingresos Totales")
print("="*70)

df_tendencia = df_limpio.groupby('TIME_PERIOD#es')['OBS_VALUE'].sum().reset_index()
df_tendencia.columns = ['Año', 'Total_Ingresos']

print("\nDatos para el gráfico:")
print(df_tendencia)

grafico_tendencia = (
    ggplot(df_tendencia, aes(x='Año', y='Total_Ingresos')) +
    geom_line(color='#2ca02c', size=1.2) +
    geom_point(color='#2ca02c', size=3) +
    scale_x_continuous(breaks=range(2015, 2024, 1)) +
    labs(
        title='Tendencia Total de Ingresos en Canarias (2015-2023)',
        x='Año',
        y='SIngresso Total',
        caption='Fuente: Estadísticas de Ingresos en Canarias'
    ) +
    theme_minimal() +
    theme(
        figure_size=(12, 6),
        plot_title=element_text(size=14, weight='bold'),
        axis_title=element_text(size=11),
        axis_text_x=element_text(angle=45, hjust=1),
    )
)

print("\n✓ Gráfico creado exitosamente")
grafico_tendencia.show()


# PASO 5: GRÁFICO 3 - Evolución por Medida

print("\n" + "="*70)
print("GRÁFICO 3: Evolución Individual de Cada Medida")
print("="*70)

medidas = df_limpio['MEDIDAS#es'].unique()
print(f"\nMedidas a visualizar: {len(medidas)}")
for medida in medidas:
    print(f"  • {medida}")

grafico_evolucion = (
    ggplot(df_limpio, aes(x='TIME_PERIOD#es', y='OBS_VALUE', color='MEDIDAS#es')) +
    geom_line(size=1) +
    geom_point(size=2.5) +
    scale_x_continuous(breaks=range(2015, 2024, 1)) +
    labs(
        title='Evolución de Fuentes de Ingreso en Canarias (2015-2023)',
        x='Año',
        y='Ingresso',
        color='Tipo de Ingreso',
        caption='Fuente: Estadísticas de Ingresos en Canarias'
    ) +
    theme_minimal() +
    theme(
        figure_size=(13, 6),
        plot_title=element_text(size=14, weight='bold'),
        axis_title=element_text(size=11),
        legend_position='bottom',
    )
)

print("\n✓ Gráfico creado exitosamente")
grafico_evolucion.show()


# PASO 6: GRÁFICOS 4 - Individuales por Medida (Iterativo)

print("\n" + "="*70)
print("GRÁFICO 4: Gráficos Individuales por Medida (Iterativo)")
print("="*70)

graficos_por_medida = {}

for medida in df_limpio['MEDIDAS#es'].unique():
    df_medida = df_limpio[df_limpio['MEDIDAS#es'] == medida].copy()
    
    grafico = (
        ggplot(df_medida, aes(x='TIME_PERIOD#es', y='OBS_VALUE')) +
        geom_line(color='#1f77b4', size=1) +
        geom_point(color='#1f77b4', size=2.5) +
        scale_x_continuous(breaks=range(2015, 2024, 2)) +
        labs(
            title=f'Evolución: {medida}',
            x='Año',
            y='Ingresso',
            caption='Fuente: Estadísticas de Ingresos en Canarias'
        ) +
        theme_minimal() +
        theme(
            figure_size=(10, 5),
            plot_title=element_text(size=12, weight='bold'),
            axis_title=element_text(size=10),
            axis_text_x=element_text(angle=45, hjust=1),
        )
    )
    
    graficos_por_medida[medida] = grafico

print(f"\n✓ {len(graficos_por_medida)} gráficos individuales creados")
for medida in graficos_por_medida.keys():
    print(f"  • {medida}")


# PASO 7: GRÁFICO 5 - Área Acumulativa

print("\n" + "="*70)
print("GRÁFICO 5: Composición Acumulativa de Ingresos")
print("="*70)

grafico_area = (
    ggplot(df_limpio, aes(x='TIME_PERIOD#es', y='OBS_VALUE', fill='MEDIDAS#es')) +
    geom_area(position='stack', alpha=0.7) +
    scale_x_continuous(breaks=range(2015, 2024, 1)) +
    labs(
        title='Composición Acumulativa de Ingresos en Canarias',
        x='Año',
        y='Ingresso Acumulado',
        fill='Tipo de Ingreso',
        caption='Fuente: Estadísticas de Ingresos en Canarias'
    ) +
    theme_minimal() +
    theme(
        figure_size=(12, 6),
        plot_title=element_text(size=14, weight='bold'),
        axis_title=element_text(size=11),
        legend_position='bottom',
    )
)

print("\n✓ Gráfico creado exitosamente")
grafico_area.show()


# PASO 8: ESTADÍSTICAS

print("\n" + "="*70)
print("Estadisticas por medida")
print("="*70)

stats = df_limpio.groupby('MEDIDAS#es')['OBS_VALUE'].agg([
    ('Promedio', 'mean'),
    ('Mínimo', 'min'),
    ('Máximo', 'max'),
    ('Desv.Est', 'std'),
    ('Coef.Var', lambda x: (x.std() / x.mean()) * 100 if x.mean() != 0 else 0)
]).round(2)

print("\n")
print(stats.to_string())

print("\n" + "="*70)
print("Analisis de cambio por anõ")
print("="*70)

for medida in df_limpio['MEDIDAS#es'].unique():
    df_medida = df_limpio[df_limpio['MEDIDAS#es'] == medida].sort_values('TIME_PERIOD#es')
    
    primer_año = df_medida['TIME_PERIOD#es'].min()
    ultimo_año = df_medida['TIME_PERIOD#es'].max()
    
    valor_inicio = df_medida[df_medida['TIME_PERIOD#es'] == primer_año]['OBS_VALUE'].values[0]
    valor_fin = df_medida[df_medida['TIME_PERIOD#es'] == ultimo_año]['OBS_VALUE'].values[0]
    cambio = valor_fin - valor_inicio
    cambio_pct = (cambio / valor_inicio) * 100 if valor_inicio != 0 else 0
    
    print(f"\n{medida}")
    print(f"  {primer_año}: {valor_inicio:.2f}%")
    print(f"  {ultimo_año}: {valor_fin:.2f}%")
    print(f"  Cambio: {cambio:+.2f}% ({cambio_pct:+.1f}%)")


# PASO 9: Exportar gráficos

print("\n" + "="*70)
print("Exportando gráficos")
print("="*70)

output_dir = Path(__file__).parent / "graficos_salida_lab"
output_dir.mkdir(exist_ok=True)

print(f"\nDirectorio de salida: {output_dir}")

try:
    grafico_distribucion.save(str(output_dir / "01_distribucion.png"), dpi=300, verbose=False)
    print("✓ Guardado: 01_distribucion.png")
except Exception as e:
    print(f"✗ Error guardando distribucion: {e}")

try:
    grafico_tendencia.save(str(output_dir / "02_tendencia.png"), dpi=300, verbose=False)
    print("✓ Guardado: 02_tendencia.png")
except Exception as e:
    print(f"✗ Error guardando tendencia: {e}")

try:
    grafico_evolucion.save(str(output_dir / "03_evolucion.png"), dpi=300, verbose=False)
    print("✓ Guardado: 03_evolucion.png")
except Exception as e:
    print(f"✗ Error guardando evolución: {e}")

for medida, grafico in graficos_por_medida.items():
    nombre_archivo = medida.lower().replace(" ", "_").replace("á", "a")
    try:
        grafico.save(str(output_dir / f"04_medida_{nombre_archivo}.png"), dpi=300, verbose=False)
        print(f"✓ Guardado: 04_medida_{nombre_archivo}.png")
    except Exception as e:
        print(f"✗ Error guardando {medida}: {e}")

try:
    grafico_area.save(str(output_dir / "05_area.png"), dpi=300, verbose=False)
    print("✓ Guardado: 05_area.png")
except Exception as e:
    print(f"✗ Error guardando área: {e}")

