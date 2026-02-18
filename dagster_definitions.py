"""
Script para ejecutar el pipeline de Dagster directamente.
Alterna a usar dagster dev -f assets_renta_canarias.py desde la terminal.
"""

from dagster import Definitions, define_asset_job, in_process_executor
from assets_renta_canarias import (
    cargar_dataset_renta,
    dataset_renta_limpio,
    datos_por_medida,
    generar_graficos_por_medida,
    grafico_distribucion_ingressos,
    grafico_tendencia_total,
    guardar_graficos_dinamicos,
    guardar_graficos_resumen
)


# Definir el job que ejecuta todos los assets en orden
renta_canarias_job = define_asset_job(
    name="pipeline_renta_canarias",
    description="Pipeline completo de análisis de rentas en Canarias con visualizaciones",
    executor_def=in_process_executor,
    tags={
        "owner": "DataOps",
        "domain": "Visualización",
        "dataset": "Distribución de Rentas Canarias"
    }
)


# Definir todas las definiciones para Dagster
defs = Definitions(
    assets=[
        cargar_dataset_renta,
        dataset_renta_limpio,
        datos_por_medida,
        generar_graficos_por_medida,
        grafico_distribucion_ingressos,
        grafico_tendencia_total,
        guardar_graficos_dinamicos,
        guardar_graficos_resumen
    ],
    jobs=[renta_canarias_job],
)


if __name__ == "__main__":
    print("\n" + "="*70)
    print("Pipeline Renta Canarias - Ejecución Manual")
    print("\n✓ Para ejecutar este pipeline desde Dagster UI, usa:")
    print("  → dagster dev -f assets_renta_canarias.py")
    print("\n✓ Luego abre http://localhost:3000 en tu navegador")
    print("\n✓ Selecciona el job 'pipeline_renta_canarias' y haz clic en 'Launch Execution'")
    print("\n" + "="*70)