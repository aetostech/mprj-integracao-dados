Module bnmp.mapper
==================
BNMP API mapping tools.

Functions
---------

    
`mapper(event: Dict[Union[str, Literal['state_id']], int], context: aws_lambda_powertools.utilities.typing.lambda_context.LambdaContext = None) ‑> Dict[Literal['api_map'], List[Dict[str, Union[int, bool]]]]`
:   Map the BNMP API.
    
    Args:
        event: Must contain a ``state_id`` with a value between 1 and 27.
        context: A AWS Lambda Context given during the AWS Lambda execution.
    
    Raises:
        IllegalArgumentError: The event provided is invalid or incomplete.

Classes
-------

`Mapper()`
:   Map the BNMP API.
    
    The BNMP API cannot return more than 10.000 warrants per header (maximum of
    5 pages with 2.000 warrants each). The mapping occurs to calculate what
    queries must be done to scrap all the data the API contains.
    
    Maps are composed of query parameters such as 'state' and 'city' and the
    ammount of documents each one contains in the API. They are created with
    multiple probing queries sent to the API to undersanto how the data is
    distributed in the API.
    
    Initiate mapping class.

    ### Methods

    `agencies_retriever(self, city_id: int) ‑> Generator[str, None, None]`
    :   Extract the legal agencies ids of a city.
        
        Args:
            city_id: A city id.
        
        Returns:
            A generator with the ids of the legal agencies in the city.
            The ids are formatted as ``{"id":number}`` because the API expects
            a string formated as a JSON object as a ``orgaoExpeditor``
            header parameter.

    `cities_retriever(self, state_id: int) ‑> Generator[int, None, None]`
    :   Extract the cities ids in a state.
        
        Args:
            state_id: A state id.
        
        Returns:
            A generator containing ids of the cities in the state.

    `gen_map(self, state_id: int) ‑> Generator[Optional[Dict[str, Union[int, str, bool]]], None, None]`
    :   Generate API maps.
        
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

    `probe(self, api_map: Dict[str, Union[str, int]]) ‑> Dict[str, Union[str, int]]`
    :   Add count of available warrants to an API map.
        
        Fills the first probe field with a value that equals zero.
        
        Args:
            api_map: API map.
        
        Returns:
            An API map filled with one more probe value.
        
        Raises:
            InvalidCookieError: The current cookie is invalid or expired.
            Exception: Any exception raised by the API.

    `requester(self, api_map: Dict[str, Union[str, int]]) ‑> Dict[Any, Any]`
    :   Make probing request to the API and return the reponse as a dict.
        
        Args:
            api_map: API map.
        
        Returns:
            API response data.

    `threads(self, maps: List[dict]) ‑> Generator[Dict[str, Union[str, int]], None, None]`
    :   Create and run a requests threadpool then yield probes.
        
        Args:
            maps: A list of API maps.
        
        Returns:
            A generator with all the API maps that had a valid response during
            probing.