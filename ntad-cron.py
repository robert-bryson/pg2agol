"""Downloads postgis table, uploads to AGOL"""

import os
import shutil
import geopandas
from arcgis.gis import GIS, ItemProperties
from dotenv import load_dotenv
from sqlalchemy import create_engine

import tempfile
from pathlib import Path

# TODO handle cli args


def setup_config():
    load_dotenv()


def extract_from_pg() -> geopandas.GeoDataFrame:
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
    df = geopandas.GeoDataFrame.from_postgis(sql, conn)
    print("Connection successful!")

    return df


def extract_from_gsheet():
    pass


def join_data():
    # see https://geopandas.org/en/stable/docs/user_guide/mergingdata.html#attribute-joins
    pass


def convert_to_shp_zip(df: geopandas.GeoDataFrame) -> Path:
    print("Converting dataframe to shapefile..")
    tempdir = Path(tempfile.mkdtemp(prefix='arc_shp'))
    shp_file_name = os.getenv("SHP_FILE_NAME") or "dataframe.shp"
    abs_path = tempdir.joinpath(shp_file_name)
    df.to_file(abs_path)
    print(f"Sucessfully converted to shapefile at {abs_path}")

    zip_name = shutil.make_archive(shp_file_name, 'zip', tempdir)
    print(f"Shapefile zipped to {zip_name}")

    return Path(zip_name)


def publish_to_agol(shp_path: Path):
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
        "title": "NTAD Data",
        "tags": "ntad",
        "overwrite": True,
    }

    shp_item = gis.content.add(shp_props, data=str(shp_path))
    shp_service = shp_item.publish()
    shp_service

def cleanup():
    # TODO cleanup tempdir/zip
    pass


def main():
    setup_config()
    pg_df = extract_from_pg()
    # TODO
    # gsheet_csv = extract_from_gsheet()
    # join_data()
    shp_path = convert_to_shp_zip(pg_df)
    publish_to_agol(shp_path)
    cleanup()


if __name__ == "__main__":
    # TODO argparse

    main()
