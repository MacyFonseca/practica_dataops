"""
Script para ejecutar el pipeline de Dagster directamente.
Alterna a usar dagster dev -f assets_renta_canarias.py desde la terminal.
"""

import json
import os
from pathlib import Path

from dagster import (
    Definitions,
    RunRequest,
    SkipReason,
    define_asset_job,
    in_process_executor,
    sensor,
)
from src.assets_renta_canarias import (
    cargar_dataset_renta,
    cargar_codigos_municipios,
    cargar_nivel_estudios,
    dataset_renta_con_municipios,
    dataset_renta_con_estudios,
    dataset_estudios_limpio,
    dataset_renta_limpio,
    grafico_distribucion_ingressos,
    grafico_tendencia_total,
    grafico_ingresos_por_isla,
    grafico_nivel_estudios_distribucion,
    guardar_graficos_resumen,
    islas_raw,
    template_ia,
    codigo_generado_ia,
    codigo_limpio_ia,
    visualizacion_png,
    publicar_en_ghpages,
)
from src.checks_pipeline import all_checks


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


# Sensor: relanzar el pipeline cuando cambien los archivos de datos
_DATOS_DIR = Path(__file__).parent
_ARCHIVOS_VIGILADOS = [
    _DATOS_DIR / "distribucion-renta-canarias.csv",
    _DATOS_DIR / "codislas.csv",
    _DATOS_DIR / "nivelestudios.xlsx",
]


@sensor(job=renta_canarias_job, minimum_interval_seconds=60)
def sensor_cambio_datos(context):
    """
    Evalúa cada 30 segundos si alguno de los archivos de datos ha sido modificado.
    Si detecta un cambio, lanza automáticamente pipeline_renta_canarias.
    El cursor almacena el último mtime conocido de cada archivo.
    """
    cursor_actual = json.loads(context.cursor or "{}")
    nuevo_cursor = {}
    hay_cambios = False

    for ruta in _ARCHIVOS_VIGILADOS:
        clave = str(ruta)
        if ruta.exists():
            mtime = os.path.getmtime(ruta)
            nuevo_cursor[clave] = mtime
            if cursor_actual.get(clave) != mtime:
                hay_cambios = True
        else:
            context.log.warning(f"[sensor_cambio_datos] Archivo no encontrado: {ruta}")

    if hay_cambios:
        context.update_cursor(json.dumps(nuevo_cursor))
        archivos_cambiados = [
            Path(k).name
            for k, v in nuevo_cursor.items()
            if cursor_actual.get(k) != v
        ]
        context.log.info(f"[sensor_cambio_datos] Cambios detectados en: {archivos_cambiados}")
        yield RunRequest(
            run_key=json.dumps(nuevo_cursor),
            tags={"trigger": "sensor_cambio_datos", "archivos": str(archivos_cambiados)},
        )
    else:
        yield SkipReason("Sin cambios en los archivos de datos")


# Definir todas las definiciones para Dagster
defs = Definitions(
    assets=[
        cargar_dataset_renta,
        cargar_codigos_municipios,
        cargar_nivel_estudios,
        dataset_renta_con_municipios,
        dataset_renta_con_estudios,
        dataset_estudios_limpio,
        dataset_renta_limpio,
        grafico_distribucion_ingressos,
        grafico_tendencia_total,
        grafico_ingresos_por_isla,
        grafico_nivel_estudios_distribucion,
        guardar_graficos_resumen,
        islas_raw,
        template_ia,
        codigo_generado_ia,
        codigo_limpio_ia,
        visualizacion_png,
        publicar_en_ghpages,
    ],
    asset_checks=all_checks,
    jobs=[renta_canarias_job],
    sensors=[sensor_cambio_datos],
)


if __name__ == "__main__":
    print("\n" + "="*70)
    print("Pipeline Renta Canarias - Ejecución Manual")
    print("\n✓ Para ejecutar este pipeline desde Dagster UI, usa:")
    print("  → dagster dev -f assets_renta_canarias.py")
    print("\n✓ Luego abre http://localhost:3000 en tu navegador")
    print("\n✓ Selecciona el job 'pipeline_renta_canarias' y haz clic en 'Launch Execution'")
    print("\n" + "="*70)
