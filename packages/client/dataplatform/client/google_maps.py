import json
from typing import Optional, Tuple

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from dataplatform.core.secret_manager import get_secret


class GoogleMapsAPI:
    def __init__(self, api_key: Optional[str] = None):
        """
        Initialize Google Maps API client.

        Args:
            api_key (str, optional): Google Maps API key.
                Defaults to None, in which case it will be fetched from the secret
                manager.
        """
        self.api_key = api_key or get_secret("google-maps")
        if not self.api_key:
            raise ValueError("Google Maps API key is required.")
        self.session = requests.Session()
        retries = Retry(
            total=5,
            backoff_factor=1,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["GET", "POST"],
        )
        adapter = HTTPAdapter(max_retries=retries, pool_connections=50, pool_maxsize=50)
        self.session.mount("https://", adapter)
        self.maps_api_url = "https://maps.googleapis.com/maps/api"
        self.places_api_url = "https://places.googleapis.com/v1"

    def _get(self, url: str, endpoint: str, params: dict) -> dict:
        """Make a GET request to the Google Maps API.

        Args:
            endpoint (str): API endpoint.
            params (dict): Query parameters.

        Returns:
            dict: API response.
        """

        url = f"{url}/{endpoint}"
        params["key"] = self.api_key
        response = self.session.get(url, params=params)
        response.raise_for_status()

        data = response.json()
        if data.get("status") != "OK":
            raise Exception(
                f"API Error: {data.get('status')} - {data.get('error_message')}"
            )

        return data

    def _post(
        self, url: str, endpoint: str, payload: dict, headers: dict = None
    ) -> dict:
        """Make a POST request to the Google Maps API.

        Args:
            endpoint (str): API endpoint.
            payload (dict): Request payload.
            headers (dict, optional): Request headers. Defaults to None.

        Returns:
            dict: API response.
        """
        payload = json.dumps(payload)
        url = f"{url}/{endpoint}"

        if not headers:
            headers = {}
        headers["X-Goog-Api-Key"] = self.api_key
        response = self.session.post(url, data=payload, headers=headers)
        response.raise_for_status()

        data = response.json()

        return data

    def get_lat_lon(self, address: str) -> Tuple[Optional[float], Optional[float]]:
        """
        Get latitude and longitude for a given address.

        Args:
            address (str): The address to geocode.

        Returns:
            tuple: (latitude, longitude) or (None, None) if not found.
        """
        params = {"address": address}
        data = self._get(self.maps_api_url, "geocode/json", params)

        results = data.get("results", [])
        if results:
            location = results[0]["geometry"]["location"]
            return location["lat"], location["lng"]

        return None, None

    def get_nearby_places(
        self, lat: float, lon: float, types: list, fields: list, radius: float = 11265.0
    ):
        """
        Get nearby places for given latitude and longitude.

        Args:
            lat (float): Latitude of the location.
            lon (float): Longitude of the location.
            radius (float): Radius in meters to search for nearby places.
                Default is 11265.0 meters (approximately 7 miles).
            types (list): Type of places to search for (e.g., "restaurant", "gym").
            fields (list): Fields to include in the response.
                https://developers.google.com/maps/documentation/places/web-service/nearby-search#fieldmask

        Returns:
            list: A list of nearby places.
        """
        payload = {
            "includedPrimaryTypes": types,
            "locationRestriction": {
                "circle": {
                    "center": {"latitude": lat, "longitude": lon},
                    "radius": radius,
                }
            },
        }

        headers = {
            "Content-Type": "application/json",
            "X-Goog-FieldMask": ",".join(fields),
        }
        endpoint = "places:searchNearby"
        data = self._post(self.places_api_url, endpoint, payload, headers)

        return data.get("places", [])
