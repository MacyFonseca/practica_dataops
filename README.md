# Practica DataOps


## Master en Ciberseguridad e Inteligencia de Datos


### Assignatura: Visualización


#### Objetivo:
- Visualización de la distribución de rentas en Canarias siguiendo los principios de **DataOps** mediante la herramienta **Dagster** y la librería **plotnine**.


#### Ejecutar codigo:
- Crear entorno virtual Python: python3 -m venv venv
- Activar el entorno: source venv/bin/activate
- Instalar dependencias: pip install dagster; pip install dagster-webserver; pip install pandas; pip install plotnine
- Test de comunicación (verificar assets en la interfaz de Dagster): dagster dev -f  <ruta_fichero_assets>