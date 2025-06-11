import json

import os
import dotenv
import requests
import pandas as pd
import geopandas as gpd
import shapely

dotenv.load_dotenv()

from digitaltwin_dataspace import run_components, Collector, ComponentConfiguration


class BrusselsMobilityBikeCountersCollector(Collector):
    def get_schedule(self) -> str:
        return "10s"

    def get_configuration(self) -> ComponentConfiguration:
        return ComponentConfiguration(
            name="bike_counters_collector",
            tags=["Bike", "devices"],
            description="Collects bike devices data from Brussels Mobility API",
            content_type="application/json",
        )

    def collect(self) -> bytes:
        data = requests.get(
            "https://data.mobility.brussels/bike/api/counts/?request=devices"
        ).json()
        return json.dumps(data).encode("utf-8")


class BrusselsMobilityBikeCountsCollector(Collector):
    def get_schedule(self) -> str:
        return "10s"

    def get_configuration(self) -> ComponentConfiguration:
        return ComponentConfiguration(
            name="bike_counts_collector",
            tags=["Bike", "counts"],
            description="Collects live bike counts data from Brussels Mobility API",
            content_type="application/json",
        )

    def collect(self) -> bytes:
        data = requests.get(
            "https://data.mobility.brussels/bike/api/counts/?request=live"
        ).json()
        return json.dumps(data).encode("utf-8")



if __name__ == "__main__":
    run_components([
        BrusselsMobilityBikeCountersCollector(),
        BrusselsMobilityBikeCountsCollector()
    ])