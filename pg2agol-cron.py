"""Downloads postgis table, uploads to AGOL"""

import os
import shutil
import geopandas as gpd
from arcgis.gis import GIS, ItemProperties
from dotenv import load_dotenv
from sqlalchemy import create_engine

import tempfile
from pathlib import Path

# TODO handle cli args


def setup_config():
    load_dotenv()


def extract_from_pg() -> gpd.GeoDataFrame:
    print("Extracting from postgres..")
    pg_user = os.getenv("PG_USER")
    pg_pass = os.getenv("PG_PASS")
    pg_host = os.getenv("PG_HOST")
    pg_port = os.getenv("PG_PORT") or "5432"
    pg_db = os.getenv("PG_DB")
    db_connection_url = f"postgresql://{pg_user}:{pg_pass}@{pg_host}:{pg_port}/{pg_db}"

    conn = create_engine(db_connection_url)
    sql = "SELECT id, facility_name, geom FROM public.facilities;"
    print(f"Connecting to db: {pg_db} on host: {pg_host}..")
    df = gpd.GeoDataFrame.from_postgis(sql, conn)
    print("Connection successful!")

    return df


def get_attribute_csv() -> gpd.GeoDataFrame:
    # TODO extract from google sheets

    csv_path = os.getenv("CSV_PATH")
    df = gpd.GeoDataFrame.from_file(csv_path)
    return df


def join_data(geometry_df: gpd.GeoDataFrame, attributes_df: gpd.GeoDataFrame):
    # see https://gpd.org/en/stable/docs/user_guide/mergingdata.html#attribute-joins
    join_field = os.getenv("JOIN_FIELD")
    attributes_df.loc(
        attributes_df.index[attributes_df[join_field] != ''], axis=0, inplace=True)
    attributes_df['OID'] = attributes_df[join_field].astype(
        'int64', errors="ignore")
    attributes_df["OID"].dropna()
    join_df = geometry_df.merge(
        attributes_df, left_on='id', right_on='OID')
    return join_df


def convert_to_shp_zip(df: gpd.GeoDataFrame) -> Path:
    """Converts pg table to shapefile, saves as zip for upload"""
    print("Converting dataframe to shapefile..")
    tempdir = Path(tempfile.mkdtemp(prefix='arc_shp'))
    shp_file_name = os.getenv("SHP_FILE_NAME") or "dataframe.shp"
    abs_path = tempdir.joinpath(shp_file_name)
    df.to_file(abs_path)
    print(f"Converted to shapefile at {abs_path}")

    zip_name = shutil.make_archive(str(abs_path), 'zip', tempdir)
    print(f"Shapefile zipped to {zip_name}")

    return Path(zip_name)


def publish_to_agol(shp_path: Path):
    """Publishes shapefile zip to AGOL"""
    print("Publishing to AGOL..")
    gis = GIS(
        url=os.getenv("AGOL_URL"),
        username=os.getenv("AGOL_USER"),
        password=os.getenv("AGOL_PASS"),
    )

    agol_fs = gis.content.search(
        query=f'owner:{gis.properties.user.username} type:feature service')

    shp_props = {
        "type": "Shapefile",
        "title": os.getenv("SHP_PROP_TITLE"),
        "tags": os.getenv("SHP_PROP_TAGS"),
        "overwrite": True,
    }

    shp_item = gis.content.add(shp_props, data=str(shp_path))
    shp_service = shp_item.publish()
    shp_service


def cleanup():
    # TODO cleanup tempdir/zip
    # TODO cleanup csv
    pass


def main():
    setup_config()
    pg_df = extract_from_pg()
    attributes_df = get_attribute_csv()
    joined_df = join_data(pg_df, attributes_df)
    shp_path = convert_to_shp_zip(pg_df)
    publish_to_agol(shp_path)
    cleanup()


if __name__ == "__main__":
    # TODO argparse

    main()
