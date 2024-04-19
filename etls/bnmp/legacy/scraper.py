"""BNMP API scraper.

Scraps warrants data from the BNMP API.
"""

import json
import logging
import redshift_connector
import requests
from aws_lambda_powertools.utilities.typing import LambdaContext
from concurrent.futures import as_completed, ThreadPoolExecutor
from datetime import date, datetime
from typing import Any, Dict, Generator, List, Literal, Set, Tuple, Union

from bnmp_utils import (
    clean,
    config,
    csv_s3_redshift,
    define_payload,
    headers,
    Redshift,
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


class BulkScraper:
    """Scrap general warrants data from the BNMP API.

    The data scraped by this class is the same exibited by the BNMP's web
    interface.

    Warrants already scraped in anterior runs have their last seen dates
    updated on Redshift. New warrants are directed for detailed scraping.
    """

    def __init__(self) -> None:
        """Extract DB connection parameters."""
        logging.info("Initializing BulkScraper")
        _red = Redshift()
        self.redshift_params = _red.extract_creds()

    def calc_range(self, depth: int) -> range:
        """Calculate the page range needed to reach a given depth.

        The maximum value is 5 as the API can only return 10.000 results in a
        given query.

        Args:
            depth: Ammount of pages

        Returns:
            Range (depth) needed to reach all the pages with data.
        """
        max_range: int = (int(depth) // 2_000) + 1
        if max_range > 5:
            max_range = 5
        return range(max_range)

    def _get_response(
        self, page: int, payload: str, order: Literal["ASC", "DESC"]
    ) -> str:
        """Make requests to the API.

        Only triggered by `requester`.

        Args:
            payload: Query payload.
            order: Order of values.
            page: Page number.

        Returns:
            Data from the API.
        """
        url: str = cfg["url"]["base"].format(
            page=page, query_size=2_000, order=order
        )
        response = requests.post(
            url,
            headers=json.loads(headers()),
            data=payload,
            timeout=cfg["requests"]["timeout"],
        )
        return response.text

    def requester(self, api_map) -> Generator:
        """Request data from the API.

        Args:
            api_map: API map.

        Returns:
            Generator with pages returned by the API.
        """
        payload: str = define_payload(api_map)
        # Value of the last probe. Dictionary order is assured by Python>3.6
        depth: int = [*api_map.values()][-2]
        include_descending: bool = [*api_map.values()][-1]

        for page in self.calc_range(depth):
            yield json.loads(self._get_response(page, payload, "ASC"))
            if include_descending:
                yield json.loads(self._get_response(page, payload, "DESC"))

    def threads(self, api_map: List[Dict[Any, Any]]) -> Generator:
        """Create and run a requests threadpool and yield data.

        Args:
            api_map: API maps

        Returns:
            Generator with bulk data received from the API.
        """
        with ThreadPoolExecutor(
            max_workers=cfg["threads"]["max_workers"]
        ) as executor:
            futures = (
                executor.submit(self.requester, d)
                for d in api_map
                if d is not None
            )
            for future in as_completed(futures):
                yield from future.result()

    def scrap(
        self,
        api_map: Dict[Union[str, Literal["api_map"]], Union[Any, List[dict]]],
    ) -> Tuple[Set[Tuple[str, str, str, date, date, str]], str]:
        """Extract warrants general data.

        Args:
            api_map: The event received by `scraper`.

        Returns:
            A set with all the new warrants that need detailed scraping and the
            id of the current state being scraped.
        """
        logging.info("Listing existing warrants")
        with redshift_connector.connect(
            **self.redshift_params
        ) as conn, conn.cursor() as curs:
            curs.execute(cfg["scraper"]["sql"]["select_raw_ids"])
            existing_warrants: Set[str] = {i[0] for i in curs.fetchall()}

        old_warrants: Set[Tuple[str, str]] = set()
        new_warrants: Set[Tuple[str, str, str, date, date, str]] = set()

        logging.info("Bulk data extraction initiated")
        for obj in self.threads(api_map["api_map"]):
            if obj.get("type"):
                raise Exception(f"Error: {obj}")
            for process in obj["content"]:
                if str(process["id"]) in existing_warrants:
                    old_warrants.add(
                        (str(process["id"]), str(process["numeroProcesso"]))
                    )
                else:
                    new_warrants.add(
                        (
                            str(process["id"]),
                            str(process["idTipoPeca"]),
                            str(process["numeroProcesso"]),
                            datetime.now().date(),
                            datetime.now().date(),
                            '"' + str(clean(process)) + '"',
                        )
                    )
        logging.info("Bulk data extraction complete")

        state_id = api_map["api_map"][0]["state"]
        csv_s3_redshift(
            data=old_warrants,
            filename=f"old_warrants_{state_id}.csv",
            json_data=False,
            table="bnmp_old_ids_temp",
        )

        return new_warrants, state_id


class DetailsScraper:
    """Scrap detailed warrant data from the BNMP API.

    The data scraped by this class is the same that fills the detailed PDF
    reports generated by the BNMP API.

    All warrants will be inserted on Redshift on the end of the run.
    """

    def __init__(self) -> None:
        """Instantiate `Redshift` class."""
        logging.info("Initiating DetailsScraper")
        self.redshift = Redshift()

    def load_url(
        self, item: Tuple[str, str, str, date, date, str]
    ) -> Union[None, list]:
        """Return detailed warrant data.

        Args:
            item: Bulk warrant data.

        Returns:
            Detailed data from warrant if it is filled with valid data.
        """
        details = json.loads(
            requests.get(
                cfg["url"]["details"].format(id=item[0], type=item[1]),
                headers=json.loads(headers()),
                timeout=cfg["requests"]["timeout"],
            ).text
        )

        clean_details = str(clean(details))
        # Skip warrants with unparsed ASCII bytestrings
        if "\\" in clean_details:
            return None
        else:
            payload = [i for i in item]
            payload.append(f'"{clean_details}"')
            return payload

    def threads(
        self, items: Set[Tuple[str, str, str, date, date, str]]
    ) -> Generator[Union[None, list], None, None]:
        """Create and run a requests threadpool and yield valid data.

        Args:
            items: Set of tuples with bulk warrants data.

        Returns:
            Generator with detailed data for each warrant that received a valid
            response with valid data.
        """
        with ThreadPoolExecutor(cfg["threads"]["max_workers"]) as executor:
            futures = (executor.submit(self.load_url, item) for item in items)
            for future in as_completed(futures):
                try:
                    yield future.result()
                except json.JSONDecodeError:
                    pass

    def scrap(
        self,
        bulk_warrants: Set[Tuple[str, str, str, date, date, str]],
        state_id: str,
    ) -> None:
        """Add detailed data from the BNMP API into a temporary table.

        Args:
            bulk_warrants: Iterable containing bulk warrant data.
            state_id: A state id.
        """
        logging.info("Detailed data extraction initiated")
        new_warrants = list()
        for item in self.threads(bulk_warrants):
            if item is not None:
                new_warrants.append(item)
        logging.info("Detailed data extraction completed")

        csv_s3_redshift(
            data=new_warrants,
            filename=f"new_warrants_{state_id}.csv",
            json_data=True,
            table="bnmp_new_temp",
        )


class PDFsScraper:
    """Scrap PDFs of the warrants data from the BNMP API.

    All warrants will be inserted on S3 on the end of the run.
    """

    def __init__(self) -> None:
        """Instantiate `Redshift` class."""
        logging.info("Initiating PDFsScraper")

    def load_url(
        self, item: Tuple[str, str, str, date, date, str]
    ) -> Union[None, Tuple[str, str, bytes]]:
        """Return warrant pdf content.

        Args:
            item: Bulk warrant data.

        Returns:
            Tuple with the item id, item type and bytes representing the pdf of the warrant.
            If the API returns a status code different from 200, then None is returned.
        """
        response = requests.post(
            cfg["url"]["pdf"].format(id=item[0], type=item[1]),
            headers=json.loads(headers()),
            timeout=cfg["requests"]["timeout"],
        )

        if response.status_code == 200:
            return item[0], item[1], response.content
        return None

    def threads(
        self, items: Set[Tuple[str, str, str, date, date, str]]
    ) -> Generator[Union[None, list], None, None]:
        """Create and run a requests threadpool and yield valid data.

        Args:
            items: Set of tuples with bulk warrants data.

        Returns:
            Generator with detailed data for each warrant that received a valid
            response with valid data.
        """
        with ThreadPoolExecutor(cfg["threads"]["max_workers"]) as executor:
            futures = (executor.submit(self.load_url, item) for item in items)
            for future in as_completed(futures):
                try:
                    yield future.result()
                except json.JSONDecodeError:
                    pass

    def scrap(
        self,
        bulk_warrants: Set[Tuple[str, str, str, date, date, str]],
    ) -> None:
        """Upload the PDF of the warrants to S3.

        Args:
            bulk_warrants: Iterable containing bulk warrant data.
            state_id: A state id.
        """
        logging.info("PDFs extraction initiated")
        for item in self.threads(bulk_warrants):
            if item is not None:
                id, type, content = item
                self.__upload_to_s3("etl-bnmp", f"pdfs/{type}/{id}.pdf", item)
        logging.info("PDFs extraction completed")

    def __upload_to_s3(self, bucket: str, path: str, content: bytes) -> None:
        s3 = boto3.resource("s3")
        object = s3.Object(bucket, path)

        object.put(Body=content)


def scraper(
    event: Dict[Union[str, Literal["api_map"]], Union[Any, List[dict]]],
    context: LambdaContext = None,
):
    """AWS Lambda controller function.

    Args:
        event: A key ``api_map`` must contain a list with one or more API maps.
        Example ``"api_map":[{"state": 1, "state_probe": 6598, "include_desc":
        false}]``
        context: A AWS Lambda Context given during the AWS Lambda execution.
    """
    bulk_scraper_ = BulkScraper()
    new_warrants, state_id = bulk_scraper_.scrap(event)
    details_scraper = DetailsScraper()
    details_scraper.scrap(new_warrants, state_id)
    # pdfs_scraper = PDFsScraper()
    # pdfs_scraper.scrap(new_warrants)


if __name__ == "__main__":
    event = {
        "api_map": [{"state": 1, "state_probe": 2921, "include_desc": False}]
    }
    scraper(event)
