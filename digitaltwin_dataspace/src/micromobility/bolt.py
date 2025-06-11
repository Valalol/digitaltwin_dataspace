import json

import os
import dotenv
import requests
import pandas as pd
import geopandas as gpd
import shapely

dotenv.load_dotenv()

from digitaltwin_dataspace import run_components, Collector, ComponentConfiguration


class BoltVehiclePositionCollector(Collector):
    def get_schedule(self) -> str:
        return "10s"

    def get_configuration(self) -> ComponentConfiguration:
        return ComponentConfiguration(
            name="bolt_vehicle_position_collector",
            tags=["Bolt", "position"],
            description="Collects vehicle positions from Bolt APIs",
            content_type="application/json",
        )

    def collect(self) -> bytes:
        endpoint = "https://mds.bolt.eu/gbfs/2/336/free_bike_status"
        response_json = requests.get(endpoint).json()
        response_df = pd.json_normalize(response_json["data"]["bikes"])
        response_gdf = gpd.GeoDataFrame(
            response_df,
            crs="epsg:4326",
            geometry=[
                shapely.geometry.Point(xy)
                for xy in zip(response_df["lon"], response_df["lat"])
            ],
        )
        # Drop lat and lon columns
        response_gdf = response_gdf.drop(columns=["lat", "lon"])

        return response_gdf.to_json().encode("utf-8")


class BoltGeofencesCollector(Collector):
    def get_schedule(self) -> str:
        return "10s"

    def get_configuration(self) -> ComponentConfiguration:
        return ComponentConfiguration(
            name="bolt_geofences_collector",
            tags=["Bolt", "fences"],
            description="Collects geofences data from Bolt APIs",
            content_type="application/json",
        )

    def collect(self) -> bytes:
        endpoint = "https://mds.bolt.eu/gbfs/2/336/geofencing_zones"
        response_json = requests.get(endpoint).json()
        return json.dumps(response_json["data"]["geofencing_zones"]).encode("utf-8")


if __name__ == "__main__":
    run_components([
        BoltVehiclePositionCollector(),
        BoltGeofencesCollector()
    ])