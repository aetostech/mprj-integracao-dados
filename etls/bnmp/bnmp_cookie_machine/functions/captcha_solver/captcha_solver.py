"""BNMP Captcha solver."""

# Source: https://github.com/diegoparrilla/headless-chrome-aws-lambda-layer
from headless_chrome import create_driver
from twocaptcha import TwoCaptcha
import logging
import os
import sys
import time

logging.getLogger().setLevel(logging.INFO)

sys.path.append(os.path.dirname(os.path.dirname(os.path.realpath(__file__))))


def lambda_handler(event: None, context) -> dict:
    """Validate the current cookie.

    Args:
        event: An empty event. Event data will be ignored.
        context: A Lambda Context given by the Step Machine.

    Returns:
        Dictionary with valid cookie string.

    Raises:
        Exception: Raised if cookie could not be solved.
    """

    def captcha_solver() -> str:
        """Use 2captcha package to resolve the captcha.

        Returns:
            The resolution code given by the 2captcha API.
        """
        api_key = os.getenv(
            "APIKEY_2CAPTCHA", "f68c412c6741bec20dc2bbcff53a939e"
        )

        solver = TwoCaptcha(api_key)

        result: dict = solver.recaptcha(
            sitekey="6LcFD2kUAAAAANxbfKI76eMBcv_eU0Y_3cRxEeuz",
            url="https://portalbnmp.cnj.jus.br/#/captcha/",
        )

        return result["code"]

    driver_params: list = [
        "--disable-dev-shm-usage",
        "--disable-extensions",
        "--disable-gpu",
        "--no-sandbox",
        "--user-agent=MyUserAgent",
        "--window-size=800x600",
        "disable-infobars",
        "start-maximized",
    ]

    driver = create_driver(driver_params)

    driver.get("https://portalbnmp.cnj.jus.br/#/pesquisa-peca")

    time.sleep(6)

    if len(driver.get_cookies()) == 0:
        solution_code: str = captcha_solver()

        driver.execute_script(
            f"""document.getElementById('g-recaptcha-response').value = "{solution_code}";"""
        )

        # Function located via
        # https://gist.github.com/2captcha/2ee70fa1130e756e1693a5d4be4d8c70
        driver.execute_script(
            f"___grecaptcha_cfg.clients['0']['V']['V']['callback']('{solution_code}');"
        )

        time.sleep(6)

    cookies: dict = driver.get_cookies()
    active_cookies: dict = cookies[0]["value"]

    if len(active_cookies) == 0:
        raise Exception("Captcha could not be solved.")

    return {
        "statusCode": 200,
        "body": {"cookie": f"portalbnmp={active_cookies}"},
    }
