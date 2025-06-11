import json

import os
import dotenv
import requests

dotenv.load_dotenv()

from digitaltwin_dataspace import run_components, Collector, ComponentConfiguration


class DottVehiclePositionCollector(Collector):
    def get_schedule(self) -> str:
        return "10s"

    def get_configuration(self) -> ComponentConfiguration:
        return ComponentConfiguration(
            name="dott_vehicle_position_collector",
            tags=["Dott", "position"],
            description="Collects vehicle positions from Dott APIs",
            content_type="application/json",
        )

    def collect(self) -> bytes:
        MOBILITY_TWIN_API_KEY = os.getenv("MOBILITY_TWIN_API_KEY")
        api_url = "https://api.mobilitytwin.brussels/dott/vehicle-position"

        response = requests.get(api_url, headers={
            'Authorization': f'Bearer {MOBILITY_TWIN_API_KEY}'
        })
        response.raise_for_status()

        data = response.json()
        return json.dumps(data).encode("utf-8")


class DottGeofencesCollector(Collector):
    def get_schedule(self) -> str:
        return "10s"

    def get_configuration(self) -> ComponentConfiguration:
        return ComponentConfiguration(
            name="dott_geofences_collector",
            tags=["Dott", "fences"],
            description="Collects geofences data from Dott APIs",
            content_type="application/json",
        )

    def collect(self) -> bytes:
        MOBILITY_TWIN_API_KEY = os.getenv("MOBILITY_TWIN_API_KEY")
        api_url = "https://api.mobilitytwin.brussels/dott/geofences"

        response = requests.get(api_url, headers={
            'Authorization': f'Bearer {MOBILITY_TWIN_API_KEY}'
        })
        response.raise_for_status()

        data = response.json()
        return json.dumps(data).encode("utf-8")


if __name__ == "__main__":
    run_components([
        DottVehiclePositionCollector(),
        DottGeofencesCollector()
    ])