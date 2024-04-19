"""BNMP API mapping tools."""

from aws_lambda_powertools.utilities.typing import LambdaContext
from concurrent.futures import as_completed, ThreadPoolExecutor
from typing import Any, Dict, Generator, List, Literal, Union
import copy
import json
import logging
import requests

from bnmp_utils import (
    config,
    define_payload,
    IllegalArgumentError,
    InvalidCookieError,
)

cfg: dict = config()

if len(logging.getLogger().handlers) > 0:
    logging.getLogger().setLevel(logging.INFO)
else:
    logging.basicConfig(
        format=cfg["log"]["format"],
        datefmt=cfg["log"]["datefmt"],
        level=logging.INFO,
    )


class Mapper:
    """Map the BNMP API.

    The BNMP API cannot return more than 10.000 warrants per header (maximum of
    5 pages with 2.000 warrants each). The mapping occurs to calculate what
    queries must be done to scrap all the data the API contains.

    Maps are composed of query parameters such as 'state' and 'city' and the
    ammount of documents each one contains in the API. They are created with
    multiple probing queries sent to the API to undersanto how the data is
    distributed in the API.
    """

    def __init__(self) -> None:
        """Initiate mapping class."""
        logging.info("Initializing Mapper class")

    def requester(self, api_map: Dict[str, Union[str, int]]) -> Dict[Any, Any]:
        """Make probing request to the API and return the reponse as a dict.

        Args:
            api_map: API map.

        Returns:
            API response data.
        """
        data: str = define_payload(api_map)
        url: str = cfg["url"]["base"].format(page=0, query_size=1, order="ASC")
        response: requests.models.Response = requests.post(
            url,
            data=data,
            headers=cfg["requests"]["headers"],
            timeout=cfg["requests"]["timeout"],
        )
        return json.loads(response.text)

    def probe(
        self, api_map: Dict[str, Union[str, int]]
    ) -> Dict[str, Union[str, int]]:
        """Add count of available warrants to an API map.

        Fills the first probe field with a value that equals zero.

        Args:
            api_map: API map.

        Returns:
            An API map filled with one more probe value.

        Raises:
            InvalidCookieError: The current cookie is invalid or expired.
            Exception: Any exception raised by the API.
        """
        probe: Dict[Any, Any] = self.requester(api_map)
        if probe.get("type"):
            if probe["status"] == 401:
                raise InvalidCookieError("Cookies are expired.")
            else:
                raise Exception(probe)

        probe_size: int = (
            int(probe["totalPages"]) if probe.get("totalPages") else 0
        )

        # Fill the first probe field with zero value found
        for key, value in api_map.items():
            if "probe" in key and value == 0:
                api_map[key] = probe_size
                break
        return api_map

    def cities_retriever(self, state_id: int) -> Generator[int, None, None]:
        """Extract the cities ids in a state.

        Args:
            state_id: A state id.

        Returns:
            A generator containing ids of the cities in the state.
        """
        url: str = cfg["url"]["cities"].format(state=state_id)
        response: requests.models.Response = requests.get(
            url, headers=cfg["requests"]["headers"]
        )
        cities_json = json.loads(response.text)
        return (i["id"] for i in cities_json)

    def agencies_retriever(self, city_id: int) -> Generator[str, None, None]:
        """Extract the legal agencies ids of a city.

        Args:
            city_id: A city id.

        Returns:
            A generator with the ids of the legal agencies in the city.
            The ids are formatted as ``{"id":number}`` because the API expects
            a string formated as a JSON object as a ``orgaoExpeditor``
            header parameter.
        """
        url: str = cfg["url"]["agencies"].format(city=city_id)
        response: requests.models.Response = requests.get(
            url, headers=cfg["requests"]["headers"]
        )
        agencies_json: dict = json.loads(response.text)
        return (f'{{"id":{agency["id"]}}}' for agency in agencies_json)

    def threads(
        self, maps: List[dict]
    ) -> Generator[Dict[str, Union[str, int]], None, None]:
        """Create and run a requests threadpool then yield probes.

        Args:
            maps: A list of API maps.

        Returns:
            A generator with all the API maps that had a valid response during
            probing.
        """
        with ThreadPoolExecutor(
            max_workers=cfg["threads"]["max_workers"]
        ) as executor:
            futures = (executor.submit(self.probe, d) for d in maps)
            for future in as_completed(futures):
                data = future.result()
                if data is not None:
                    yield data

    def gen_map(
        self, state_id: int
    ) -> Generator[Union[Dict[str, Union[int, str, bool]], None], None, None]:
        """Generate API maps.

        API maps are dictionaries that describe to ``scraper`` how much data,
        if any, is reachable with a given set of parameters and to define what
        type of payload is needed to extract it.
        API maps may contain the following keys:
        ``state`` state id,
        ``state_probe`` size of state probe,
        ``city`` city id,
        ``city_probe`` size of city probe,
        ``agency`` agency id,
        ``agency_probe`` size of agency probe,
        ``doctype`` document type id,
        ``doctype_probe`` size of document type probe,
        ``include_desc`` boolean indicating whether or not a query with
        descending values is needed.

        Args:
            state_id: A state id.

        Returns:
            A generator of maps with varying complexity.
        """
        logging.info("API mapping initiated")

        def validate_probe(
            api_map: dict, probe_name: str
        ) -> Union[dict, None]:
            """Assure validity of a probe value in a API map.

            An API map containing a valid probe will be directly yielded by
            ``gen_map``. An API map containing invalid probe will be subjected
            to more granular probing.

            A probe is considered valid when its value

            Args:
                api_map: API map
                probe_name: The probe key to be validated.

            Returns:
                The original API map if the probe is valid or None if the probe
                is zero (occurs when there is no data under the parameters
                contained in the probe).

            Raises:
                ValueError: The probe size is too big and needs further work.
            """
            # Discard empty parameters
            if api_map[probe_name] == 0:
                return None
            # If probe < 10_000, a single query is done to extract data
            # If 10_000 < probe < 20_000, a descending query is added to the
            # default one
            if api_map[probe_name] <= 20_000:
                api_map["include_desc"] = (
                    False if api_map[probe_name] <= 10_000 else True
                )
                return api_map
            # If probe > 20_000, a more detailed query is needed
            else:
                raise ValueError

        state_map: List[dict] = [{"state": state_id, "state_probe": 0}]
        city_maps: List[dict] = []
        agency_maps: List[dict] = []
        doctype_maps: List[dict] = []

        logging.info(f"Probing 'state_id' {state_id}")
        for api_map in self.threads(state_map):
            try:
                yield validate_probe(api_map, "state_probe")
            except ValueError:
                for city in self.cities_retriever(int(api_map["state"])):
                    api_map = copy.deepcopy(api_map)
                    api_map["city"] = city
                    api_map["city_probe"] = 0
                    city_maps.append(api_map)

        logging.info(f"Probing cities from 'state_id' {state_id}")
        for api_map in self.threads(city_maps):
            try:
                yield validate_probe(api_map, "city_probe")
            except ValueError:
                for agency in self.agencies_retriever(int(api_map["city"])):
                    api_map = copy.deepcopy(api_map)
                    api_map["agency"] = agency
                    api_map["agency_probe"] = 0
                    agency_maps.append(api_map)

        logging.info(f"Probing agencies from 'state_id' {state_id}")
        for api_map in self.threads(agency_maps):
            try:
                yield validate_probe(api_map, "agency_probe")
            except ValueError:
                for doctype in range(1, 14):
                    api_map = copy.deepcopy(api_map)
                    api_map["doctype"] = doctype
                    api_map["doctype_probe"] = 0
                    doctype_maps.append(api_map)

        logging.info(f"Probing document types from 'state_id' {state_id}")
        for api_map in self.threads(doctype_maps):
            yield validate_probe(api_map, "doctype_probe")

        logging.info("API mapping completed")


def mapper(
    event: Dict[Union[str, Literal["state_id"]], int],
    context: LambdaContext = None,
) -> Dict[Literal["api_map"], List[Dict[str, Union[int, bool]]]]:
    """Map the BNMP API.

    Args:
        event: Must contain a ``state_id`` with a value between 1 and 27.
        context: A AWS Lambda Context given during the AWS Lambda execution.

    Raises:
        IllegalArgumentError: The event provided is invalid or incomplete.
    """
    if not event.get("state_id"):
        error = "'state_id' not informed'"
    elif event["state_id"] not in range(1, 28):
        error = "Invalid 'state_id'"
    if "error" in locals():
        logging.critical(error)
        raise IllegalArgumentError(error)

    mapper_ = Mapper()

    logging.info(f"Starting mapping of 'state_id' {event['state_id']}")
    state_map = mapper_.gen_map(event["state_id"])

    payload: dict = {}
    api_map: List[Dict[str, Union[int, str, bool]]] = [
        i for i in state_map if i is not None
    ]
    payload["api_map"] = api_map

    logging.info(f"Mapping of 'state_id' {event['state_id']} completed")
    return payload
