#!/bin/bash

# Script de configuración del proyecto - Pipeline DataOps Rentas Canarias
# Uso: bash setup.sh

set -e

echo "╔════════════════════════════════════════════════════════════════╗"
echo "║     SETUP - Pipeline DataOps: Rentas en Canarias               ║"
echo "╚════════════════════════════════════════════════════════════════╝"
echo ""

# Colores
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Comprobar Python
echo -e "${BLUE}[1/4]${NC} Verificando Python..."
if ! command -v python3 &> /dev/null; then
    echo -e "${RED}✗ Python 3 no encontrado${NC}"
    exit 1
fi
PYTHON_VERSION=$(python3 --version | awk '{print $2}')
echo -e "${GREEN}✓ Python ${PYTHON_VERSION} encontrado${NC}"

# Crear entorno virtual
echo ""
echo -e "${BLUE}[2/4]${NC} Creando entorno virtual..."
if [ ! -d "venv" ]; then
    python3 -m venv venv
    echo -e "${GREEN}✓ Entorno virtual creado${NC}"
else
    echo -e "${YELLOW}⚠ Entorno virtual ya existe${NC}"
fi

# Activar entorno
echo ""
echo -e "${BLUE}[3/4]${NC} Activando entorno virtual y instalando dependencias..."
source venv/bin/activate

echo -e "${YELLOW}  Instalando: dagster${NC}"
pip install -q dagster

echo -e "${YELLOW}  Instalando: dagster-webserver${NC}"
pip install -q dagster-webserver

echo -e "${YELLOW}  Instalando: pandas${NC}"
pip install -q pandas

echo -e "${YELLOW}  Instalando: plotnine${NC}"
pip install -q plotnine

echo -e "${GREEN}✓ Todas las dependencias instaladas${NC}"

# Crear directorio de salida
echo ""
echo -e "${BLUE}[4/4]${NC} Configurando directorios..."
mkdir -p graficos_salida
echo -e "${GREEN}✓ Directorio 'graficos_salida' creado${NC}"

# Verificar archivos necesarios
echo ""
echo -e "${BLUE}Verificando archivos necesarios:${NC}"
FILES=("distribucion-renta-canarias.csv" "assets_renta_canarias.py")
for file in "${FILES[@]}"; do
    if [ -f "$file" ]; then
        echo -e "${GREEN}  ✓ $file${NC}"
    else
        echo -e "${RED}  ✗ $file (FALTA)${NC}"
    fi
done
