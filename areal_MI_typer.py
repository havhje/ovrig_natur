import marimo

__generated_with = "0.15.2"
app = marimo.App(width="columns")


@app.cell(column=0, hide_code=True)
def _(mo):
    mo.md(
        r"""
    # Nature Type Mapping Analysis - Notebook Summary


    ## Datasets

    ### 1. Nature Types (`naturtyper` table)
    - **Content**: MI-classified nature type polygons
    - **Key Fields**:
      - `geom`: Geometry column containing polygon/multipolygon data
      - `area_sqm`: Calculated area in square meters using spheroidal calculation
    - **Area Calculation**: Uses `ST_Area_Spheroid(ST_FlipCoordinates(geom))` 
      - Note: Coordinate flipping suggests data may be in a coordinate system requiring axis order correction
    - **Projection**: WGS84
    - Lastet ned fra: https://kartkatalog.geonorge.no/metadata/naturtyper-paa-land-nin/eb48dd19-03da-41e1-afd9-7ebc3079265c

    ### 2. Coverage Map (`dekningskart` table)
    - **Content**: Survey coverage areas for MI-mapping
    - **Key Fields**:
      - `geom`: Geometry column containing polygon/multipolygon data
      - `area_sqm_riktig`: Calculated area in square meters
    - **Area Calculation**: Uses standard `ST_Area(geom)`
    - **Projection**: ETRS89 / UTM zone 33N
    - Lastet ned fra (via ett python script): https://kart.miljodirektoratet.no/arcgis/rest/services/naturtyper_nin/MapServer/1

    ## Technical Notes

    ### Spatial Considerations
    1. **Different area calculation methods**: 
       - `naturtyper`: Spheroidal calculation (geographic coordinates)
       - `dekningskart`: Planar calculation (projected coordinates)
       - This suggests the datasets may be in different coordinate reference systems

    2. **Coordinate flipping**: The use of `ST_FlipCoordinates` for the naturtyper table indicates potential coordinate order issues (lat/lon vs lon/lat)

    ## Recommendations
    1. Consider harmonizing projections for more accurate area comparisons
    3. Investigate why coverage is low - could indicate data quality issues or genuine mapping gaps
    4. Add metadata fields to track mapping date, source, and quality indicators
    """
    )
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(
        r"""
    ## Om areal beregning i koordinat systemer


    UTM 33N: The Flat Map (Projected Coordinate System)
    Think of a UTM (Universal Transverse Mercator) coordinate system like a paper map of a specific region. To create this flat map, the curved surface of the Earth has been mathematically "unrolled" or projected onto a 2D plane.[1]
    It's Planar: Because the data is on a flat, Cartesian grid (like graph paper), simple Euclidean geometry applies.[2] You can use the Pythagorean theorem to calculate distances, and standard geometric formulas to calculate area. The units are consistent and linear, like meters.

    ST_Area Works Perfectly: The ST_Area function in DuckDB is designed for these planar calculations. It treats your polygon as if it were drawn on a flat sheet of paper and calculates the area using simple, fast, 2D geometry. The area it returns is in the units of the coordinate system itself (in the case of UTM 33N, that would be square meters).[3]
    The key takeaway is that for a projected coordinate system like UTM, the distortion caused by flattening the Earth is minimized over a small area (the UTM zone), making these simple calculations highly accurate for that specific region.[4]

    WGS84: The Globe (Geographic Coordinate System)

    WGS84, on the other hand, doesn't represent a flat map. It's a geographic coordinate system that models the Earth as a three-dimensional spheroid (a slightly squashed sphere).[5] Coordinates are not given in meters but in angular units: degrees of latitude and longitude.

    It's a Curved Surface: You can't use simple, flat-plane geometry on a globe. The distance between two longitude lines changes as you move from the equator to the poles. A "square" degree near the equator covers a much larger area than a "square" degree near the North Pole.

    Why ST_Area Fails: If you were to use the standard ST_Area function on WGS84 coordinates, it would treat the degrees of latitude and longitude as if they were on a flat grid. The result would be a meaningless number in "square degrees," which doesn't translate to a real-world area like square meters and would be wildly inaccurate.
    Enter ST_Area_Spheroidal: This is where the specialized function comes in. ST_Area_Spheroidal is designed to work with geographic coordinates (like WGS84 latitude and longitude).[6][7] It uses complex spherical or, more accurately, ellipsoidal mathematics to calculate the area on the curved surface of the Earth model.[8][9] This function takes the Earth's curvature into account to give you a highly accurate area measurement in square meters.[3]

    The "Flipping Coordinates" Issue

    You mentioned having to flip coordinates. This is a common point of confusion. Geospatial standards often specify a latitude, longitude (Y, X) order, but many systems default to a longitude, latitude (X, Y) order. DuckDB's ST_Area_Spheroid function specifically expects the latitude, longitude order.[10] If your data is stored in the more common longitude, latitude format, you need to flip them for the function to calculate correctly; otherwise, it can return incorrect results or NaN (Not a Number).[10]

    Analogy: Measuring a Soccer Field vs. the Whole Earth

    UTM (ST_Area): Calculating the area of a polygon in UTM is like measuring the area of a soccer field. The field is small enough that the Earth's curvature is negligible. You can use a tape measure and standard geometric formulas (length x width) and get a very accurate result.

    WGS84 (ST_Area_Spheroidal): Calculating the area of a polygon in WGS84 is like trying to calculate the area of North America. You can't just use a giant tape measure. You have to use calculations that account for the fact that the surface you're measuring is curved.

    In summary, you use different functions because you are working with fundamentally different models of the world: a flat projection (UTM) versus a curved spheroid (WGS84). DuckDB provides the right tool for each job to ensure your calculations are both efficient and, most importantly, accurate.
    """
    )
    return


@app.cell
def _():
    return


@app.cell(column=1)
def _():
    import marimo as mo
    import polars as pl
    import altair as alt
    import os
    import duckdb
    return duckdb, mo


@app.cell
def _(duckdb):
    # Setter opp connection til duckdb
    # Har følgende tables: naturtyper (MI-data) og dekningskart (for MI-kartlegging)



    # duckdb_conn = duckdb.connect("C:/Users/havh/OneDrive - Multiconsult/Dokumenter/Oppdrag/FoUI SVV/duck_db.naturtyper.db")
    duckdb_conn = duckdb.connect("/Users/havardhjermstad-sollerud/Documents/Kodeprosjekter/marimo/ovrig_natur/naturtyper")
    return (duckdb_conn,)


@app.cell
def _(duckdb_conn, mo):
    _df = mo.sql(
        f"""
        INSTALL spatial;
        LOAD spatial;
        """,
        engine=duckdb_conn
    )
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""### Kalkulasjoner""")
    return


@app.cell
def _(duckdb_conn, mo, naturtyper):
    MI_df = mo.sql(
        f"""
        SELECT * EXCLUDE (geom)
        FROM naturtyper
        """,
        engine=duckdb_conn
    )
    return (MI_df,)


@app.cell
def _(dekningskart, duckdb_conn, mo):
    dekning_df = mo.sql(
        f"""
        SELECT * EXCLUDE (geom)
        FROM dekningskart
        """,
        engine=duckdb_conn
    )
    return (dekning_df,)


@app.cell
def _(MI_df, dekning_df):
    statestikk_MI = MI_df.select("area_sqm").sum()
    statestikk_dekning = dekning_df.select("area_sqm_riktig").sum()
    return statestikk_MI, statestikk_dekning


@app.cell
def _(statestikk_MI, statestikk_dekning):
    # Dekning 
    Prosent_dekning = (statestikk_MI/statestikk_dekning)*100
    Prosent_dekning
    return (Prosent_dekning,)


@app.cell
def _(Prosent_dekning, mo, statestikk_MI, statestikk_dekning):
    # Let's examine the actual values and what they represent
    mo.vstack([
        mo.md("## Coverage Analysis"),
        mo.md(f"""
        ### What the numbers mean:
    
        - **Total MI-mapped area**: {statestikk_MI[0,0]:,.0f} m² ({statestikk_MI[0,0]/1000000:,.2f} km²)
        - **Total survey/coverage area**: {statestikk_dekning[0,0]:,.0f} m² ({statestikk_dekning[0,0]/1000000:,.2f} km²)
        - **Coverage percentage**: {Prosent_dekning[0,0]:.2f}%
    
        This means that **{Prosent_dekning[0,0]:.2f}%** of the total surveyed area has been classified with specific nature types (MI-types).
        """)
    ])
    return


@app.cell(column=2, hide_code=True)
def _(mo):
    mo.md(r"""## MI-typer""")
    return


@app.cell
def _(duckdb_conn, mo, naturtyper):
    _df = mo.sql(
        f"""
        -- Get 10 random rows (excluding geometry column)
        SELECT * EXCLUDE (geom)
        FROM naturtyper
        USING SAMPLE 10;
        """,
        engine=duckdb_conn
    )
    return


@app.cell
def _(duckdb_conn, mo, naturtyper):
    _ = mo.sql(
        f"""
        ALTER TABLE naturtyper 
        ADD COLUMN area_sqm DOUBLE;
        UPDATE naturtyper 
        SET area_sqm = ST_Area_Spheroid(ST_FlipCoordinates(geom))
        WHERE ST_GeometryType(geom) IN ('POLYGON', 'MULTIPOLYGON');
        """,
        output=False,
        engine=duckdb_conn
    )
    return


@app.cell(column=3, hide_code=True)
def _(mo):
    mo.md(r"""## Dekningskart MI-typer""")
    return


@app.cell
def _(dekningskart, duckdb_conn, mo):
    _df = mo.sql(
        f"""
        SELECT * EXCLUDE (geom)
        FROM dekningskart
        USING SAMPLE 10;
        """,
        engine=duckdb_conn
    )
    return


@app.cell
def _(dekningskart, duckdb_conn, mo):
    _df = mo.sql(
        f"""
        ALTER TABLE dekningskart 
        ADD COLUMN area_sqm_riktig DOUBLE;
        UPDATE dekningskart 
        SET area_sqm_riktig = ST_Area(geom)
        WHERE ST_GeometryType(geom) IN ('POLYGON', 'MULTIPOLYGON');
        """,
        engine=duckdb_conn
    )
    return


if __name__ == "__main__":
    app.run()
