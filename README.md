# Practica DataOps


## Master en Ciberseguridad e Inteligencia de Datos


### Assignatura: Visualización


#### Objetivo:
- Visualización de la distribución de rentas en Canarias siguiendo los principios de **DataOps** mediante la herramienta **Dagster** y la librería **plotnine**.


#### Ejecutar codigo:
- Crear entorno virtual Python: python3 -m venv venv
- Activar el entorno: source venv/bin/activate
- Instalar dependencias: pip install -r requirements.txt

**Pipeline completo (Dagster UI):**
```bash
dagster dev -f dagster_definitions.py --port 3000
# → http://127.0.0.1:3000/
```

**Solo los checks desde terminal:**
```bash
dagster asset check --select "*"
```

**Tests negativos con pytest:**
```bash
python -m pytest test_checks_pipeline.py -v
```