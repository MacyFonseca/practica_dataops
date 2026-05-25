"""
Assets para visualizar la distribución de renta, ocupación y actividad económica
en las secciones censales de Tenerife (2021–2023).

Historia de datos: "¿Dónde viven los ricos y los pobres de Tenerife?"
  - Mapa 1: Renta bruta media por persona por sección (evolución 2021–2023)
  - Mapa 2: Fuente de ingreso dominante por sección (sueldos, pensiones, prestaciones…)
  - Mapa 3: Categoría de ocupación dominante por sección
  - Mapa 4: Actividad económica dominante por sección

Principios DataOps: assets independientes por capa, checks de calidad, rutas reproducibles.
Gramática de gráficos: ggplot + geom_map + escalas + facetas + tema.
"""

import geopandas as gpd
import pandas as pd
from pathlib import Path
from dagster import asset, Output, MetadataValue
from plotnine import (
    ggplot, aes, geom_map,
    scale_fill_cmap, scale_fill_hue,
    facet_wrap, labs, theme_void, theme, element_text,
)

_DATA_DIR = Path(__file__).parent.parent / "data"
_CARTO_DIR = _DATA_DIR / "cartografia-secciones"

# Años de los GeoJSON disponibles
_ANIOS_MAPA = [2021, 2022, 2023, 2024]


# ===========================================================================
# CAPA 1: EXTRACCIÓN
# ===========================================================================

@asset(
    group_name="secciones_tenerife",
    description="Carga renta bruta media por sección censal de Tenerife (2021–2023). "
                "TERRITORIO_CODE apunta al año del mapa siguiente (2021→2022, …).",
)
def cargar_renta_media_sc() -> pd.DataFrame:
    path = _DATA_DIR / "rentamedia-sc-3.csv"
    df = pd.read_csv(path)
    df.columns = df.columns.str.strip()
    df["OBS_VALUE"] = pd.to_numeric(df["OBS_VALUE"], errors="coerce")
    df["año"] = pd.to_numeric(df["año"], errors="coerce").astype("Int64")
    print(f"\n Renta media secciones: {df.shape[0]} registros")
    print(f"  Años: {sorted(df['año'].dropna().unique().tolist())}")
    print(f"  Medidas: {df['MEDIDAS_CODE'].unique().tolist()}")
    return df


@asset(
    group_name="secciones_tenerife",
    description="Carga distribución de ingresos por fuente (sueldos, pensiones, prestaciones…) "
                "a nivel de sección censal. OBS_VALUE usa coma decimal.",
)
def cargar_distribucion_renta_sc() -> pd.DataFrame:
    path = _DATA_DIR / "distribucion-renta-ingresos.csv"
    df = pd.read_csv(path)
    df.columns = df.columns.str.strip()
    # OBS_VALUE usa coma como separador decimal
    df["OBS_VALUE"] = (
        df["OBS_VALUE"]
        .astype(str)
        .str.replace(",", ".", regex=False)
        .pipe(pd.to_numeric, errors="coerce")
    )
    df["año"] = pd.to_numeric(df["año"], errors="coerce").astype("Int64")
    print(f"\n Distribución renta secciones: {df.shape[0]} registros")
    print(f"  Medidas: {df['MEDIDAS#es'].unique().tolist()}")
    return df


@asset(
    group_name="secciones_tenerife",
    description="Carga sector de ocupación por sección censal (2021–2023). "
                "geocode usa el año de datos (20210101_…, 20220101_…, 20230101_…).",
)
def cargar_ocupacion_sc() -> pd.DataFrame:
    path = _DATA_DIR / "ocupacion-sc-3.csv"
    df = pd.read_csv(path)
    df.columns = df.columns.str.strip()
    df["num_casos"] = pd.to_numeric(df["num_casos"], errors="coerce").fillna(0)
    df["año"] = pd.to_numeric(df["año"], errors="coerce").astype("Int64")
    df = df.dropna(subset=["geocode"])
    print(f"\n Ocupación secciones: {df.shape[0]} registros")
    print(f"  Años: {sorted(df['año'].dropna().unique().tolist())}")
    print(f"  Categorías de ocupación: {df['ocupacion'].nunique()}")
    return df


@asset(
    group_name="secciones_tenerife",
    description="Carga actividad económica por sección censal (2021–2023). "
                "geocode usa el año de datos; año en columna 'Periodo'.",
)
def cargar_actividad_sc() -> pd.DataFrame:
    path = _DATA_DIR / "actividad-sc-3.csv"
    df = pd.read_csv(path)
    df.columns = df.columns.str.strip()
    df["num_casos"] = pd.to_numeric(df["num_casos"], errors="coerce").fillna(0)
    df["Periodo"] = pd.to_numeric(df["Periodo"], errors="coerce").astype("Int64")
    df = df.dropna(subset=["geocode"])
    print(f"\n Actividad económica secciones: {df.shape[0]} registros")
    print(f"  Periodos: {sorted(df['Periodo'].dropna().unique().tolist())}")
    print(f"  Categorías actividad: {df['Actividad económica'].nunique()}")
    return df


@asset(
    group_name="secciones_tenerife",
    description="Carga y combina los 4 GeoJSON de secciones censales de Tenerife "
                "(2021–2024) en un único GeoDataFrame con columna 'año_mapa'.",
)
def cargar_cartografia() -> gpd.GeoDataFrame:
    gdfs = []
    for year in _ANIOS_MAPA:
        path = _CARTO_DIR / f"secciones_{year}0101_tenerife.json"
        gdf = gpd.read_file(str(path))
        gdf["año_mapa"] = year
        gdfs.append(gdf)
    carto = gpd.GeoDataFrame(
        pd.concat(gdfs, ignore_index=True),
        geometry="geometry",
        crs=gdfs[0].crs,
    )
    print(f"\n Cartografía cargada: {len(carto)} filas ({len(_ANIOS_MAPA)} años × secciones)")
    print(f"  CRS: {carto.crs}")
    return carto


# ===========================================================================
# CAPA 2: TRANSFORMACIÓN
# ===========================================================================

@asset(
    group_name="secciones_tenerife",
    description="Merge renta bruta media × cartografía. "
                "Filtra RENTA_BRUTA_MEDIA_PERSONA. Join por TERRITORIO_CODE == geocode.",
)
def geodata_renta_media(
    cargar_renta_media_sc: pd.DataFrame,
    cargar_cartografia: gpd.GeoDataFrame,
) -> gpd.GeoDataFrame:
    df = cargar_renta_media_sc[
        cargar_renta_media_sc["MEDIDAS_CODE"] == "RENTA_BRUTA_MEDIA_PERSONA"
    ].copy()

    gdf = cargar_cartografia.merge(
        df[["TERRITORIO_CODE", "año", "OBS_VALUE", "municipio"]],
        left_on="geocode",
        right_on="TERRITORIO_CODE",
        how="left",
    )
    gdf = gpd.GeoDataFrame(gdf, geometry="geometry", crs=cargar_cartografia.crs)

    cobertura = gdf["OBS_VALUE"].notna().mean()
    print(f"\n Geodata renta media: {len(gdf)} filas, cobertura datos {cobertura:.1%}")
    return gdf


@asset(
    group_name="secciones_tenerife",
    description="Fuente de ingreso dominante por sección: la medida con mayor OBS_VALUE. "
                "Usa el último año disponible. Join por TERRITORIO_CODE == geocode.",
)
def geodata_distribucion_renta(
    cargar_distribucion_renta_sc: pd.DataFrame,
    cargar_cartografia: gpd.GeoDataFrame,
) -> gpd.GeoDataFrame:
    ultimo_anio = int(cargar_distribucion_renta_sc["año"].max())
    df = cargar_distribucion_renta_sc[
        cargar_distribucion_renta_sc["año"] == ultimo_anio
    ].copy()

    # Medida dominante = mayor OBS_VALUE por sección
    # Filtrar filas con OBS_VALUE nulo antes del groupby para evitar grupos all-NA
    df_valid = df[df["OBS_VALUE"].notna()].copy()
    idx = df_valid.groupby("TERRITORIO_CODE")["OBS_VALUE"].idxmax()
    df_dom = (
        df_valid.loc[idx, ["TERRITORIO_CODE", "MEDIDAS#es", "OBS_VALUE"]]
        .rename(columns={"MEDIDAS#es": "medida_dominante", "OBS_VALUE": "valor_dominante"})
    )

    gdf = cargar_cartografia.merge(
        df_dom,
        left_on="geocode",
        right_on="TERRITORIO_CODE",
        how="left",
    )
    gdf = gpd.GeoDataFrame(gdf, geometry="geometry", crs=cargar_cartografia.crs)

    cobertura = gdf["medida_dominante"].notna().mean()
    print(f"\n Geodata distribución renta (año {ultimo_anio}): {len(gdf)} filas, cobertura {cobertura:.1%}")
    return gdf


@asset(
    group_name="secciones_tenerife",
    description="Ocupación dominante por sección: categoría con más casos. "
                "Agrega por (geocode, año, ocupacion) sumando ambos sexos. Join por geocode.",
)
def geodata_ocupacion(
    cargar_ocupacion_sc: pd.DataFrame,
    cargar_cartografia: gpd.GeoDataFrame,
) -> gpd.GeoDataFrame:
    df = cargar_ocupacion_sc.copy()

    # Suma de casos por sección, año y categoría de ocupación (ambos sexos)
    df_agg = (
        df.groupby(["geocode", "año", "ocupacion"])["num_casos"]
        .sum()
        .reset_index()
    )

    # Ocupación dominante por (geocode, año)
    idx = df_agg.groupby(["geocode", "año"])["num_casos"].idxmax()
    df_dom = (
        df_agg.loc[idx.dropna(), ["geocode", "año", "ocupacion", "num_casos"]]
        .rename(columns={"ocupacion": "ocupacion_dominante", "num_casos": "n_casos_dom"})
    )

    gdf = cargar_cartografia.merge(df_dom, on="geocode", how="left")
    gdf = gpd.GeoDataFrame(gdf, geometry="geometry", crs=cargar_cartografia.crs)

    cobertura = gdf["ocupacion_dominante"].notna().mean()
    print(f"\n Geodata ocupación: {len(gdf)} filas, cobertura {cobertura:.1%}")
    return gdf


@asset(
    group_name="secciones_tenerife",
    description="Actividad económica dominante por sección: categoría con más casos. "
                "Agrega por (geocode, Periodo, Actividad económica). Join por geocode.",
)
def geodata_actividad(
    cargar_actividad_sc: pd.DataFrame,
    cargar_cartografia: gpd.GeoDataFrame,
) -> gpd.GeoDataFrame:
    df = cargar_actividad_sc.copy()

    # Suma de casos por sección, período y tipo de actividad (ambos sexos)
    df_agg = (
        df.groupby(["geocode", "Periodo", "Actividad económica"])["num_casos"]
        .sum()
        .reset_index()
    )

    # Actividad dominante por (geocode, Periodo)
    idx = df_agg.groupby(["geocode", "Periodo"])["num_casos"].idxmax()
    df_dom = (
        df_agg.loc[idx.dropna(), ["geocode", "Periodo", "Actividad económica", "num_casos"]]
        .rename(columns={
            "Actividad económica": "actividad_dominante",
            "num_casos": "n_casos_dom",
            "Periodo": "año",
        })
    )

    gdf = cargar_cartografia.merge(df_dom, on="geocode", how="left")
    gdf = gpd.GeoDataFrame(gdf, geometry="geometry", crs=cargar_cartografia.crs)

    cobertura = gdf["actividad_dominante"].notna().mean()
    print(f"\n Geodata actividad: {len(gdf)} filas, cobertura {cobertura:.1%}")
    return gdf


# ===========================================================================
# CAPA 3: VISUALIZACIÓN
# ===========================================================================

@asset(
    group_name="secciones_tenerife",
    description="Mapa coroplético de renta bruta media por persona, facetado por año (2021–2023). "
                "Permite ver la evolución temporal de la desigualdad territorial.",
)
def mapa_renta_media(geodata_renta_media: gpd.GeoDataFrame):
    """
    Gramática de gráficos:
      Dataset:   GeoDataFrame por sección censal, filtrado a RENTA_BRUTA_MEDIA_PERSONA.
      Estética:  fill = OBS_VALUE (renta €/persona).
      Geometría: geom_map() — polígonos de secciones censales.
      Escala:    scale_fill_gradient2 divergente: rojo (baja renta) → blanco (mediana) → azul (alta renta).
      Faceta:    año de los datos (2021, 2022, 2023).
      Tema:      theme_void() — elimina ejes, fondo y cuadrícula para un mapa limpio.
    """
    gdf = geodata_renta_media[geodata_renta_media["OBS_VALUE"].notna()].copy()
    gdf["año_label"] = "Datos " + gdf["año"].astype(int).astype(str)

    return (
        ggplot(gdf)
        + aes(fill="OBS_VALUE")
        + geom_map(color="white", size=0.05)
        + scale_fill_cmap(
            "RdBu_r",
            name="€ / persona",
        )
        + facet_wrap("~ año_label", ncol=3)
        + labs(
            title="Renta bruta media por persona en Tenerife (2021–2023)",
            subtitle="Por sección censal. Escala divergente: rojo = baja renta, azul = alta renta.",
            caption="Fuente: Instituto Canario de Estadística (ISTAC)",
        )
        + theme_void()
        + theme(
            figure_size=(18, 7),
            plot_title=element_text(size=14, weight="bold"),
            plot_subtitle=element_text(size=10),
            strip_text=element_text(size=10, weight="bold"),
            legend_position="right",
        )
    )


@asset(
    group_name="secciones_tenerife",
    description="Mapa coroplético de la fuente de ingreso dominante por sección censal. "
                "¿Viven de sueldos, pensiones u otras prestaciones?",
)
def mapa_fuentes_ingreso(geodata_distribucion_renta: gpd.GeoDataFrame):
    """
    Gramática de gráficos:
      Dataset:   GeoDataFrame con medida_dominante por sección (último año).
      Estética:  fill = medida_dominante (categoría de ingreso).
      Geometría: geom_map() — polígonos de secciones censales.
      Escala:    scale_fill_manual con paleta cualitativa diferenciada.
      Tema:      theme_void() para un mapa sin distracciones visuales.
    Recomendación de diseño: paleta cualitativa para variables nominales,
    colores suficientemente distintos entre categorías.
    """
    gdf = geodata_distribucion_renta[
        geodata_distribucion_renta["medida_dominante"].notna()
    ].copy()

    return (
        ggplot(gdf)
        + aes(fill="medida_dominante")
        + geom_map(color="white", size=0.05)
        + scale_fill_hue(name="Fuente de ingreso\ndominante")
        + labs(
            title="Fuente de ingreso dominante en Tenerife por sección censal",
            subtitle="Sueldo, pensión u otra prestación con mayor peso en cada zona.",
            caption="Fuente: ISTAC",
        )
        + theme_void()
        + theme(
            figure_size=(14, 10),
            plot_title=element_text(size=14, weight="bold"),
            plot_subtitle=element_text(size=10),
            legend_position="right",
            legend_text=element_text(size=9),
        )
    )


@asset(
    group_name="secciones_tenerife",
    description="Mapa coroplético de la categoría de ocupación dominante por sección censal "
                "(último año disponible). ¿Qué tipo de trabajo predomina en cada zona?",
)
def mapa_ocupacion(geodata_ocupacion: gpd.GeoDataFrame):
    """
    Gramática de gráficos:
      Dataset:   GeoDataFrame con ocupacion_dominante por sección, filtrado al año más reciente.
      Estética:  fill = ocupacion_dominante (categoría de ocupación).
      Geometría: geom_map() — polígonos de secciones censales.
      Escala:    scale_fill_manual con paleta cualitativa.
      Tema:      theme_void() para focalizar en la distribución geográfica.
    Recomendación de diseño: etiquetas abreviadas para evitar leyenda ilegible;
    colores categóricos diferenciados.
    """
    ultimo_anio = int(geodata_ocupacion["año"].dropna().max())
    gdf = geodata_ocupacion[geodata_ocupacion["año"] == ultimo_anio].copy()
    gdf = gdf[gdf["ocupacion_dominante"].notna()].copy()

    # Abreviar etiquetas largas para la leyenda
    gdf["ocup_corta"] = gdf["ocupacion_dominante"].str[:50]

    return (
        ggplot(gdf)
        + aes(fill="ocup_corta")
        + geom_map(color="white", size=0.05)
        + scale_fill_hue(name="Ocupación dominante")
        + labs(
            title=f"Ocupación dominante por sección censal en Tenerife ({ultimo_anio})",
            subtitle="Categoría ocupacional con mayor número de casos por sección.",
            caption="Fuente: INE — Censo anual de población",
        )
        + theme_void()
        + theme(
            figure_size=(15, 10),
            plot_title=element_text(size=14, weight="bold"),
            plot_subtitle=element_text(size=10),
            legend_position="right",
            legend_text=element_text(size=8),
        )
    )


@asset(
    group_name="secciones_tenerife",
    description="Mapa coroplético de la actividad económica dominante por sección censal "
                "(último año disponible). ¿En qué sector trabaja más gente en cada zona?",
)
def mapa_actividad(geodata_actividad: gpd.GeoDataFrame):
    """
    Gramática de gráficos:
      Dataset:   GeoDataFrame con actividad_dominante por sección, filtrado al año más reciente.
      Estética:  fill = actividad_dominante (sector o categoría de actividad).
      Geometría: geom_map() — polígonos de secciones censales.
      Escala:    scale_fill_manual con paleta cualitativa.
      Tema:      theme_void() sin ejes ni fondo para maximizar el área del mapa.
    """
    ultimo_anio = int(geodata_actividad["año"].dropna().max())
    gdf = geodata_actividad[geodata_actividad["año"] == ultimo_anio].copy()
    gdf = gdf[gdf["actividad_dominante"].notna()].copy()

    # Abreviar etiquetas largas
    gdf["act_corta"] = gdf["actividad_dominante"].str[:50]

    return (
        ggplot(gdf)
        + aes(fill="act_corta")
        + geom_map(color="white", size=0.05)
        + scale_fill_hue(name="Actividad dominante")
        + labs(
            title=f"Actividad económica dominante por sección censal en Tenerife ({ultimo_anio})",
            subtitle="Sector con mayor número de casos por sección.",
            caption="Fuente: INE — Censo anual de población",
        )
        + theme_void()
        + theme(
            figure_size=(15, 10),
            plot_title=element_text(size=14, weight="bold"),
            plot_subtitle=element_text(size=10),
            legend_position="right",
            legend_text=element_text(size=8),
        )
    )


# ===========================================================================
# CAPA 4: GUARDADO
# ===========================================================================

@asset(
    group_name="secciones_tenerife",
    description="Guarda los 4 mapas de secciones censales como PNG en outputs/pipeline/.",
)
def guardar_mapas_secciones(
    mapa_renta_media,
    mapa_fuentes_ingreso,
    mapa_ocupacion,
    mapa_actividad,
) -> Output:
    output_dir = Path(__file__).parent.parent / "outputs" / "pipeline" / "mapas_secciones"
    output_dir.mkdir(parents=True, exist_ok=True)

    mapas = {
        "mapa_renta_media.png": mapa_renta_media,
        "mapa_fuentes_ingreso.png": mapa_fuentes_ingreso,
        "mapa_ocupacion.png": mapa_ocupacion,
        "mapa_actividad.png": mapa_actividad,
    }

    rutas_guardadas = []
    for nombre, grafico in mapas.items():
        ruta = output_dir / nombre
        grafico.save(str(ruta), dpi=200, verbose=False)
        print(f"✅ Guardado: {ruta}")
        rutas_guardadas.append(str(ruta))

    return Output(
        value=rutas_guardadas,
        metadata={
            "n_mapas": len(rutas_guardadas),
            "rutas": MetadataValue.text("\n".join(rutas_guardadas)),
        },
    )
