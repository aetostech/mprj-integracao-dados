"""BNMP Cookie validity checker.

This is the first stage of the API scraping. This module will halt the workflow
if the current cookie is not valid.
"""

import boto3
import json
import requests
import logging

logging.getLogger().setLevel(logging.INFO)


def lambda_handler(event: dict, context) -> dict:
    """Validates the BNMP cookie and updates the valid header in S3.

    Args:
        event: An dictionary with the expected format {"status_code"}
        context: A AWS Lambda Context given during the AWS Lambda execution.

    Returns:
        A dictionary with a boolean value indicating if the current cookies are
        valid. Only returns when cookies are valid.
    """

    # def cookie_extractor() -> str:
    #     """Extract cookie string from S3."""
    #     s3 = boto3.client("s3")
    #     response = s3.get_object(Bucket="etl-bnmp", Key="cookie.txt")
    #     return response["Body"].read().decode("utf-8")

    def s3_writer(headers: dict) -> bool:
        """Write valid headers to S3.

        Args:
            cookie: Cookie string

        Returns:
            True if successful.
        """
        with open("/tmp/headers.txt", "w") as f:
            f.write(json.dumps(headers))

        s3 = boto3.client("s3")
        s3.upload_file("/tmp/headers.txt", "etl-bnmp", "headers.txt")
        return True

    headers = {
        "accept": "application/json",
        "accept-encoding": "gzip, deflate, br",
        "accept-language": "en-US,en;q=0.9,pt-BR;q=0.8,pt;q=0.7",
        "content-type": "application/json;charset=UTF-8",
        "cookie": event["body"]["cookie"],
        "origin": "https://portalbnmp.cnj.jus.br",
        "referer": "https://portalbnmp.cnj.jus.br/",
        "sec-fetch-dest": "empty",
        "sec-fetch-mode": "cors",
        "sec-fetch-site": "same-origin",
        "user-agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/100.0.4896.127 Safari/537.36",
    }

    response = requests.post(
        "https://portalbnmp.cnj.jus.br/bnmpportal/api/pesquisa-pecas/filter?page=0&size=1&sort=numeroPeca,ASC",
        data='{"buscaOrgaoRecursivo":false,"orgaoExpeditor":{},"idEstado":1}',
        headers=headers,
        timeout=20,
    )

    if response.status_code == 200:
        if s3_writer(headers) is True:
            return {"statusCode": 200}

    raise ValueError("Cookies are invalid or could not be checked.")
