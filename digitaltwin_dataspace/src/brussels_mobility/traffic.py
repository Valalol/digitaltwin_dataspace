import json

import os
import dotenv
import requests
import pandas as pd
import geopandas as gpd
import shapely

dotenv.load_dotenv()

from digitaltwin_dataspace import run_components, Collector, ComponentConfiguration


class BrusselsMobilityTrafficDevicesCollector(Collector):
    def get_schedule(self) -> str:
        return "10s"

    def get_configuration(self) -> ComponentConfiguration:
        return ComponentConfiguration(
            name="traffic_devices_collector",
            tags=["Traffic", "devices"],
            description="Collects traffic devices data from Brussels Mobility API",
            content_type="application/json",
        )

    def collect(self) -> bytes:
        data = requests.get(
            "https://data.mobility.brussels/traffic/api/counts/?request=devices"
        ).json()
        return json.dumps(data).encode("utf-8")


class BrusselsMobilityTrafficCountsCollector(Collector):
    def get_schedule(self) -> str:
        return "10s"

    def get_configuration(self) -> ComponentConfiguration:
        return ComponentConfiguration(
            name="traffic_counts_collector",
            tags=["Traffic", "counts"],
            description="Collects live traffic counts data from Brussels Mobility API",
            content_type="application/json",
        )

    def collect(self) -> bytes:
        data = requests.get(
            "https://data.mobility.brussels/traffic/api/counts/?request=live"
        ).json()
        return json.dumps(data).encode("utf-8")



if __name__ == "__main__":
    run_components([
        BrusselsMobilityTrafficDevicesCollector(),
        BrusselsMobilityTrafficCountsCollector()
    ])