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
from shapely.geometry import box as shapely_box
from dagster import asset, Output, MetadataValue
from plotnine import (
    ggplot, aes, geom_map,
    scale_fill_cmap, scale_fill_hue,
    facet_wrap, facet_grid, labs, theme_void, theme, element_text, element_rect,
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
    # Filtrar a secciones de Tenerife isla (excluye La Gomera, La Palma, El Hierro)
    tenerife_bbox = shapely_box(-16.92, 27.97, -16.05, 28.60)
    n_antes = len(carto)
    carto_wgs = carto.to_crs("EPSG:4326")
    mask = carto_wgs.geometry.centroid.within(tenerife_bbox)
    carto = carto[mask].copy()
    carto = gpd.GeoDataFrame(carto, geometry="geometry", crs=gdfs[0].crs)
    print(f"\n Cartografía cargada: {len(carto)} filas Tenerife ({len(_ANIOS_MAPA)} años × secciones)")
    print(f"  Descartadas fuera de Tenerife: {n_antes - len(carto)}")
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
    description="Distribución de ingresos por fuente de ingreso y sección censal (último año). "
                "Mantiene todas las fuentes. Join por TERRITORIO_CODE == geocode.",
)
def geodata_distribucion_renta(
    cargar_distribucion_renta_sc: pd.DataFrame,
    cargar_cartografia: gpd.GeoDataFrame,
) -> gpd.GeoDataFrame:
    ultimo_anio = int(cargar_distribucion_renta_sc["año"].max())
    df = cargar_distribucion_renta_sc[
        cargar_distribucion_renta_sc["año"] == ultimo_anio
    ].copy()

    # Todas las fuentes de ingreso por sección (sin filtro de dominante)
    df_valid = df[df["OBS_VALUE"].notna()].copy()
    df_valid = df_valid.rename(columns={"MEDIDAS#es": "fuente_ingreso"})

    gdf = cargar_cartografia.merge(
        df_valid[["TERRITORIO_CODE", "fuente_ingreso", "OBS_VALUE"]],
        left_on="geocode",
        right_on="TERRITORIO_CODE",
        how="left",
    )
    gdf = gpd.GeoDataFrame(gdf, geometry="geometry", crs=cargar_cartografia.crs)

    n_fuentes = gdf["fuente_ingreso"].nunique()
    cobertura = gdf["OBS_VALUE"].notna().mean()
    print(f"\n Geodata distribución renta (año {ultimo_anio}): {len(gdf)} filas, {n_fuentes} fuentes, cobertura {cobertura:.1%}")
    return gdf


@asset(
    group_name="secciones_tenerife",
    description="Distribución de ocupaciones por sección censal (todos los años). "
                "Proporción de cada categoría sobre el total de la sección × año.",
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

    # Proporción de cada ocupación dentro de la sección × año
    df_total = (
        df_agg.groupby(["geocode", "año"])["num_casos"]
        .sum().reset_index()
        .rename(columns={"num_casos": "total_seccion"})
    )
    df_agg = df_agg.merge(df_total, on=["geocode", "año"])
    df_agg["proporcion"] = df_agg["num_casos"] / df_agg["total_seccion"]

    gdf = cargar_cartografia.merge(df_agg, on="geocode", how="left")
    gdf = gpd.GeoDataFrame(gdf, geometry="geometry", crs=cargar_cartografia.crs)

    n_cats = gdf["ocupacion"].nunique()
    cobertura = gdf["proporcion"].notna().mean()
    print(f"\n Geodata ocupación: {len(gdf)} filas, {n_cats} categorías, cobertura {cobertura:.1%}")
    return gdf


@asset(
    group_name="secciones_tenerife",
    description="Distribución de actividades económicas por sección censal (todos los años). "
                "Proporción de cada categoría sobre el total de la sección × año.",
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

    # Renombrar para consistencia antes de calcular proporciones
    df_agg = df_agg.rename(columns={
        "Actividad económica": "actividad",
        "Periodo": "año",
    })

    # Proporción de cada actividad dentro de la sección × año
    df_total = (
        df_agg.groupby(["geocode", "año"])["num_casos"]
        .sum().reset_index()
        .rename(columns={"num_casos": "total_seccion"})
    )
    df_agg = df_agg.merge(df_total, on=["geocode", "año"])
    df_agg["proporcion"] = df_agg["num_casos"] / df_agg["total_seccion"]

    gdf = cargar_cartografia.merge(df_agg, on="geocode", how="left")
    gdf = gpd.GeoDataFrame(gdf, geometry="geometry", crs=cargar_cartografia.crs)

    n_cats = gdf["actividad"].nunique()
    cobertura = gdf["proporcion"].notna().mean()
    print(f"\n Geodata actividad: {len(gdf)} filas, {n_cats} categorías, cobertura {cobertura:.1%}")
    return gdf


@asset(
    group_name="secciones_tenerife",
    description="Distribución de ocupaciones por sección censal, desglosada por sexo (último año). "
                "Proporción de cada categoría sobre el total de la sección × sexo.",
)
def geodata_ocupacion_por_sexo(
    cargar_ocupacion_sc: pd.DataFrame,
    cargar_cartografia: gpd.GeoDataFrame,
) -> gpd.GeoDataFrame:
    df = cargar_ocupacion_sc.copy()
    ultimo_anio = int(df["año"].dropna().max())
    df = df[df["año"] == ultimo_anio].copy()

    df_agg = (
        df.groupby(["geocode", "sexo", "ocupacion"])["num_casos"]
        .sum().reset_index()
    )
    # Proporción de cada ocupación dentro de (sección, sexo)
    df_total = (
        df_agg.groupby(["geocode", "sexo"])["num_casos"]
        .sum().reset_index()
        .rename(columns={"num_casos": "total_seccion"})
    )
    df_agg = df_agg.merge(df_total, on=["geocode", "sexo"])
    df_agg["proporcion"] = df_agg["num_casos"] / df_agg["total_seccion"]

    carto_ultimo = cargar_cartografia[cargar_cartografia["año_mapa"] == ultimo_anio].copy()
    gdf = carto_ultimo.merge(df_agg, on="geocode", how="left")
    gdf = gpd.GeoDataFrame(gdf, geometry="geometry", crs=cargar_cartografia.crs)

    n_cats = gdf["ocupacion"].nunique()
    cobertura = gdf["proporcion"].notna().mean()
    print(f"\n Geodata ocupación por sexo ({ultimo_anio}): {len(gdf)} filas, {n_cats} categorías, cobertura {cobertura:.1%}")
    return gdf


@asset(
    group_name="secciones_tenerife",
    description="Distribución de actividades económicas por sección censal, desglosada por sexo (último año). "
                "Proporción de cada categoría sobre el total de la sección × sexo.",
)
def geodata_actividad_por_sexo(
    cargar_actividad_sc: pd.DataFrame,
    cargar_cartografia: gpd.GeoDataFrame,
) -> gpd.GeoDataFrame:
    df = cargar_actividad_sc.copy()
    ultimo_anio = int(df["Periodo"].dropna().max())
    df = df[df["Periodo"] == ultimo_anio].copy()
    df = df.rename(columns={"Actividad económica": "actividad", "Sexo": "sexo"})

    df_agg = (
        df.groupby(["geocode", "sexo", "actividad"])["num_casos"]
        .sum().reset_index()
    )
    # Proporción de cada actividad dentro de (sección, sexo)
    df_total = (
        df_agg.groupby(["geocode", "sexo"])["num_casos"]
        .sum().reset_index()
        .rename(columns={"num_casos": "total_seccion"})
    )
    df_agg = df_agg.merge(df_total, on=["geocode", "sexo"])
    df_agg["proporcion"] = df_agg["num_casos"] / df_agg["total_seccion"]

    carto_ultimo = cargar_cartografia[cargar_cartografia["año_mapa"] == ultimo_anio].copy()
    gdf = carto_ultimo.merge(df_agg, on="geocode", how="left")
    gdf = gpd.GeoDataFrame(gdf, geometry="geometry", crs=cargar_cartografia.crs)

    n_cats = gdf["actividad"].nunique()
    cobertura = gdf["proporcion"].notna().mean()
    print(f"\n Geodata actividad por sexo ({ultimo_anio}): {len(gdf)} filas, {n_cats} categorías, cobertura {cobertura:.1%}")
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
            plot_background=element_rect(fill="white"),
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
    gdf = geodata_distribucion_renta[geodata_distribucion_renta["OBS_VALUE"].notna()].copy()
    gdf["fuente_corta"] = gdf["fuente_ingreso"].str[:35]

    return (
        ggplot(gdf)
        + aes(fill="OBS_VALUE")
        + geom_map(color="none", size=0)
        + scale_fill_cmap("YlOrRd", name="€ / persona")
        + facet_wrap("~ fuente_corta", ncol=3)
        + labs(
            title="Distribución de las fuentes de ingreso por sección censal en Tenerife",
            subtitle="Valor medio (€/persona) de cada fuente: sueldos, pensiones, prestaciones…",
            caption="Fuente: ISTAC",
        )
        + theme_void()
        + theme(
            figure_size=(18, 10),
            plot_background=element_rect(fill="white"),
            plot_title=element_text(size=13, weight="bold"),
            plot_subtitle=element_text(size=10),
            strip_text=element_text(size=9, weight="bold"),
            legend_position="right",
        )
    )


@asset(
    group_name="secciones_tenerife",
    description="Mapas facetados de ocupación dominante por sección censal en Tenerife "
                "(2021, 2022, 2023). Permite observar cambios temporales en la estructura ocupacional.",
)
def mapa_ocupacion_por_anio(geodata_ocupacion: gpd.GeoDataFrame):
    """
    Gramática de gráficos:
      Dataset:   GeoDataFrame con ocupacion_dominante para todos los años disponibles.
      Estética:  fill = ocupacion_dominante (categoría de ocupación).
      Geometría: geom_map() — polígonos de secciones censales.
      Faceta:    año de datos (2021, 2022, 2023) en 3 paneles.
      Escala:    scale_fill_hue — paleta cualitativa consistente entre facetas.
      Tema:      theme_void() para maximizar el área del mapa.
    """
    gdf = geodata_ocupacion[geodata_ocupacion["proporcion"].notna()].copy()
    ultimo_anio = int(gdf["año"].dropna().max())
    gdf = gdf[gdf["año"] == ultimo_anio].copy()
    gdf["ocup_corta"] = gdf["ocupacion"].str[:30]

    return (
        ggplot(gdf)
        + aes(fill="proporcion")
        + geom_map(color="none", size=0)
        + scale_fill_cmap("YlOrRd", name="Proporción\ndel total")
        + facet_wrap("~ ocup_corta", ncol=4)
        + labs(
            title=f"Distribución de ocupaciones por sección censal en Tenerife ({ultimo_anio})",
            subtitle="Proporción de cada categoría sobre el total de ocupados en cada sección.",
            caption="Fuente: INE — Censo anual de población",
        )
        + theme_void()
        + theme(
            figure_size=(20, 12),
            plot_background=element_rect(fill="white"),
            plot_title=element_text(size=13, weight="bold"),
            plot_subtitle=element_text(size=10),
            strip_text=element_text(size=9, weight="bold"),
            legend_position="right",
        )
    )


@asset(
    group_name="secciones_tenerife",
    description="Mapas facetados de actividad económica dominante por sección censal en Tenerife "
                "(2021, 2022, 2023). Muestra la evolución del tejido económico territorial.",
)
def mapa_actividad_por_anio(geodata_actividad: gpd.GeoDataFrame):
    """
    Gramática de gráficos:
      Dataset:   GeoDataFrame con actividad_dominante para todos los años disponibles.
      Estética:  fill = actividad_dominante (sector económico).
      Geometría: geom_map() — polígonos de secciones censales.
      Faceta:    año de datos (2021, 2022, 2023) en 3 paneles.
      Escala:    scale_fill_hue — paleta cualitativa consistente entre facetas.
      Tema:      theme_void().
    """
    gdf = geodata_actividad[geodata_actividad["proporcion"].notna()].copy()
    ultimo_anio = int(gdf["año"].dropna().max())
    gdf = gdf[gdf["año"] == ultimo_anio].copy()
    gdf["act_corta"] = gdf["actividad"].str[:30]

    return (
        ggplot(gdf)
        + aes(fill="proporcion")
        + geom_map(color="none", size=0)
        + scale_fill_cmap("YlOrRd", name="Proporción\ndel total")
        + facet_wrap("~ act_corta", ncol=4)
        + labs(
            title=f"Distribución de actividades económicas por sección censal en Tenerife ({ultimo_anio})",
            subtitle="Proporción de cada sector sobre el total de activos en cada sección.",
            caption="Fuente: INE — Censo anual de población",
        )
        + theme_void()
        + theme(
            figure_size=(20, 12),
            plot_background=element_rect(fill="white"),
            plot_title=element_text(size=13, weight="bold"),
            plot_subtitle=element_text(size=10),
            strip_text=element_text(size=9, weight="bold"),
            legend_position="right",
        )
    )


@asset(
    group_name="secciones_tenerife",
    description="Mapa de ocupación dominante por sección censal en Tenerife, facetado por sexo "
                "(último año). Compara qué tipo de trabajo predomina entre hombres y mujeres.",
)
def mapa_ocupacion_por_sexo(geodata_ocupacion_por_sexo: gpd.GeoDataFrame):
    """
    Gramática de gráficos:
      Dataset:   GeoDataFrame con ocupacion_dominante × sexo (último año).
      Estética:  fill = ocupacion_dominante (categoría de ocupación).
      Geometría: geom_map() — polígonos de secciones censales.
      Faceta:    sexo (Hombres / Mujeres) en 2 paneles.
      Escala:    scale_fill_hue — paleta cualitativa, colores consistentes entre paneles.
      Tema:      theme_void().
    """
    gdf = geodata_ocupacion_por_sexo[geodata_ocupacion_por_sexo["proporcion"].notna()].copy()
    ultimo_anio = int(geodata_ocupacion_por_sexo["año_mapa"].dropna().max())
    gdf["ocup_corta"] = gdf["ocupacion"].str[:25]

    return (
        ggplot(gdf)
        + aes(fill="proporcion")
        + geom_map(color="none", size=0)
        + scale_fill_cmap("YlOrRd", name="Proporción")
        + facet_grid("sexo ~ ocup_corta")
        + labs(
            title=f"Distribución de ocupaciones por sección censal en Tenerife ({ultimo_anio}), por sexo",
            subtitle="Proporción de cada categoría sobre el total de ocupados, comparada entre hombres y mujeres.",
            caption="Fuente: INE — Censo anual de población",
        )
        + theme_void()
        + theme(
            figure_size=(22, 8),
            plot_background=element_rect(fill="white"),
            plot_title=element_text(size=12, weight="bold"),
            plot_subtitle=element_text(size=10),
            strip_text=element_text(size=8, weight="bold"),
            legend_position="right",
        )
    )


@asset(
    group_name="secciones_tenerife",
    description="Mapa de actividad económica dominante por sección censal en Tenerife, facetado por sexo "
                "(último año). ¿En qué sectores trabajan hombres y mujeres en cada zona?",
)
def mapa_actividad_por_sexo(geodata_actividad_por_sexo: gpd.GeoDataFrame):
    """
    Gramática de gráficos:
      Dataset:   GeoDataFrame con actividad_dominante × sexo (último año).
      Estética:  fill = actividad_dominante (sector económico).
      Geometría: geom_map() — polígonos de secciones censales.
      Faceta:    sexo (Hombres / Mujeres) en 2 paneles.
      Escala:    scale_fill_hue — paleta cualitativa.
      Tema:      theme_void().
    """
    gdf = geodata_actividad_por_sexo[geodata_actividad_por_sexo["proporcion"].notna()].copy()
    ultimo_anio = int(geodata_actividad_por_sexo["año_mapa"].dropna().max())
    gdf["act_corta"] = gdf["actividad"].str[:25]

    return (
        ggplot(gdf)
        + aes(fill="proporcion")
        + geom_map(color="none", size=0)
        + scale_fill_cmap("YlOrRd", name="Proporción")
        + facet_grid("sexo ~ act_corta")
        + labs(
            title=f"Distribución de actividades económicas por sección censal en Tenerife ({ultimo_anio}), por sexo",
            subtitle="Proporción de cada sector sobre el total de activos, comparada entre hombres y mujeres.",
            caption="Fuente: INE — Censo anual de población",
        )
        + theme_void()
        + theme(
            figure_size=(22, 8),
            plot_background=element_rect(fill="white"),
            plot_title=element_text(size=12, weight="bold"),
            plot_subtitle=element_text(size=10),
            strip_text=element_text(size=8, weight="bold"),
            legend_position="right",
        )
    )


# ===========================================================================
# CAPA 4: GUARDADO
# ===========================================================================

@asset(
    group_name="secciones_tenerife",
    description="Guarda los 6 mapas de secciones censales como PNG en outputs/pipeline/mapas_secciones/.",
)
def guardar_mapas_secciones(
    mapa_renta_media,
    mapa_fuentes_ingreso,
    mapa_ocupacion_por_anio,
    mapa_actividad_por_anio,
    mapa_ocupacion_por_sexo,
    mapa_actividad_por_sexo,
) -> Output:
    output_dir = Path(__file__).parent.parent / "outputs" / "pipeline" / "mapas_secciones"
    output_dir.mkdir(parents=True, exist_ok=True)

    mapas = {
        "mapa_renta_media.png": mapa_renta_media,
        "mapa_fuentes_ingreso.png": mapa_fuentes_ingreso,
        "mapa_ocupacion_por_anio.png": mapa_ocupacion_por_anio,
        "mapa_actividad_por_anio.png": mapa_actividad_por_anio,
        "mapa_ocupacion_por_sexo.png": mapa_ocupacion_por_sexo,
        "mapa_actividad_por_sexo.png": mapa_actividad_por_sexo,
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
