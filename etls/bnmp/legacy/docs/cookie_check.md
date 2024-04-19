Module bnmp.cookie_check
========================
BNMP Cookies validity checker.

This is the first stage of the API scraping. This module will halt the workflow
if the current cookie is not valid.

Functions
---------

    
`checker(event: None = None, context: aws_lambda_powertools.utilities.typing.lambda_context.LambdaContext = None) ‑> Dict[Literal['is_cookie_valid'], Literal[True]]`
:   Return a boolean depending on a response from the BNMP API.
    
    Args:
        event: An empty event. Any event data will be ignored.
        context: A AWS Lambda Context given during the AWS Lambda execution.
    
    Returns:
        A dictionary with a boolean value indicating if the current cookies are
        valid. Only returns when cookies are valid.
    
    Raises:
        InvalidCookieError: The current cookie is invalid or expired.