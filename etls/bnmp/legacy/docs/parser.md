Module bnmp.parser
==================
BNMP warrants parsing and last_seen dates updating.

Functions
---------

    
`parse(event: None = None, context: aws_lambda_powertools.utilities.typing.lambda_context.LambdaContext = None)`
:   Parse raw JSON warrants data.
    
    Args:
        event: An empty event. Any event data will be ignored.
        context: A AWS Lambda Context given during the AWS Lambda execution.

Classes
-------

`Parser()`
:   Parse data from raw and update last seen dates.
    
    Extract DB connection parameters.

    ### Methods

    `copy_dates(self) ‑> None`
    :   Update last seen dates on raw and parsed warrants tables.

    `format_name(self, name: str) ‑> Optional[str]`
    :   Format names.
        
        Args:
            name: Name string
        
        Returns:
            Upercase name with no accents.

    `new_warrants(self) ‑> Generator[+T_co, -T_contra, +V_co]`
    :   Find warrants not parsed into ``bnmp.mandados``.
        
        Returns:
            Generator with warrants pending parsing and transfer.

    `parse_detailed_json(self, warrant_data: list) ‑> Dict[str, Any]`
    :   Parse JSON rows and create CSV log file.
        
        Args:
            warrant_data: List containing scrap date and raw JSON data of a
            warrant.
        
        Returns:
            Parsed warrant values.

    `parse_warrants(self) ‑> None`
    :   Parse pending warrants and insert data into permanent tables.

    `parser(self, *keys, other_dict=False) ‑> Any`
    :   Return data from valid dictionary keys.
        
        Uses a set of keys to recursively look for keys in a dictionary. Finds
        a deeply nested value in a dictionary originated from a JSON object.
        
        Args:
            keys: Keys that have to be iterated over.
            other_dict: A dictionary that is not `self.dct`, set on stage.
        
        Returns:
            An intermediary value of the loop or a specific desired object.
        
        Raises:
            KeyError: A key provided was not found.