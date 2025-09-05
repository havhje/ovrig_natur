import marimo

__generated_with = "0.15.0"
app = marimo.App(width="columns")


@app.cell(column=0)
def _():
    import marimo as mo
    import polars as pl
    import altair as alt
    import os
    import duckdb
    return duckdb, mo


@app.cell
def _(duckdb):
    duckdb_conn = duckdb.connect("C:/Users/havh/OneDrive - Multiconsult/Dokumenter/Oppdrag/FoUI SVV/duck_db.naturtyper.db")
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


@app.cell
def _(duckdb_conn, mo, naturtyper):
    _df = mo.sql(
        f"""
        -- See all columns and their data types
        DESCRIBE naturtyper;
        """,
        engine=duckdb_conn
    )
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


@app.cell(column=1)
def _():
    return


@app.cell
def _(duckdb_conn, mo, naturtyper):
    _df = mo.sql(
        f"""
        SELECT
            COUNT(*) as total_polygons,
            MIN(ST_Area_Spheroid(ST_FlipCoordinates(geom))) as min_area_sqm,
            MAX(ST_Area_Spheroid(ST_FlipCoordinates(geom))) as max_area_sqm,
            AVG(ST_Area_Spheroid(ST_FlipCoordinates(geom))) as avg_area_sqm
        FROM naturtyper
        WHERE ST_GeometryType(geom) IN ('POLYGON', 'MULTIPOLYGON');
        """,
        engine=duckdb_conn
    )
    return


@app.cell
def _(duckdb_conn, mo, naturtyper):
    _df = mo.sql(
        f"""
        SELECT
            *, -- Selects all columns from your table
            ST_Area_Spheroid(ST_FlipCoordinates(geom)) AS calculated_area_sqm
        FROM
            naturtyper
        WHERE
            ST_GeometryType(geom) IN ('POLYGON', 'MULTIPOLYGON')
        ORDER BY
            calculated_area_sqm DESC -- Orders the results by area, largest first
        LIMIT 1; -- Returns only the top row```
        """,
        engine=duckdb_conn
    )
    return


if __name__ == "__main__":
    app.run()
