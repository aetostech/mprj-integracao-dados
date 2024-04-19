"""BNMP Workflow setup and cleanup.

Creates, truncates or drops temporary warrants tables.
"""


from typing import Dict, Literal, Union
import logging

from bnmp_utils import Redshift

logging.getLogger().setLevel(logging.INFO)


class WorkflowSetup:
    """BNMP Workflow setup and cleanup class."""

    def __init__(self) -> None:
        """Set up config dictionary."""
        logging.info("Initializing WorkflowSetup")
        self.redshift = Redshift()

    def setup_temp_tables(self) -> None:
        """Create and truncate temporary warrants tables on "setup" phase.

        The unvariable truncation assures that the warrants tables will always
        be empty no matter if recently created or not.
        """
        self.redshift.run_query(
            """
            CREATE TABLE IF NOT EXISTS bnmp.bnmp_new_temp (LIKE bnmp.raw_mandados);
        """
        )
        self.redshift.run_query(
            """
            TRUNCATE TABLE bnmp.bnmp_new_temp;
        """
        )

        self.redshift.run_query(
            """
            CREATE TABLE IF NOT EXISTS bnmp.bnmp_old_ids_temp (
                id VARCHAR(64),
                numero_processo VARCHAR(128)
            );
        """
        )
        self.redshift.run_query(
            """
            TRUNCATE TABLE bnmp.bnmp_old_ids_temp;
        """
        )

        self.redshift.run_query(
            """
            CREATE TABLE IF NOT EXISTS bnmp.bnmp_mandados_temp (LIKE bnmp.mandados);
        """
        )
        self.redshift.run_query(
            """
            TRUNCATE TABLE bnmp.bnmp_mandados_temp;
        """
        )

    def drop_temp_tables(self) -> None:
        """Drop temporary warrants tables on "cleanup" phase."""
        self.redshift.run_query(
            """
            DROP TABLE bnmp.bnmp_new_temp;
        """
        )

        self.redshift.run_query(
            """
            DROP TABLE bnmp.bnmp_old_ids_temp;
        """
        )

        self.redshift.run_query(
            """
            DROP TABLE bnmp.bnmp_mandados_temp
        """
        )


def lambda_handler(
    event: Dict[Union[str, Literal["stage"]], Literal["setup", "cleanup"]],
    context,
) -> Dict[Literal["statusCode"], Literal[200]]:
    """Create or drop temporary warrants tables based on ``stage`` event value.

    Creates and truncates temp tables on setup, drops temp tables on cleanup.

    Args:
        event: Must contain a ``stage`` key with a "setup" or "cleanup" value.
        context: A AWS Lambda Context given during the AWS Lambda execution.

    Raises:
        Exception: Any exception that occured during the SQL operations. The
        native exceptions are not always correctly raised by
        ``redshift_connector`` for a unknown reason. This pattern enforces any
        exception bubble up that does not occur natively.
    """
    if not event.get("stage"):
        error = "'stage' key not found"
        logging.critical(error)
        raise Exception(error)
    elif event["stage"] not in ["setup", "cleanup"]:
        error = "Unsuported 'stage' value"
        logging.critical(error)
        raise Exception(error)

    try:
        setup_ = WorkflowSetup()

        logging.info(f"Starting {event['stage']}")
        if event["stage"] == "setup":
            setup_.setup_temp_tables()
        elif event["stage"] == "cleanup":
            setup_.drop_temp_tables()

        logging.info(f"Completed {event['stage']}")
        return {"statusCode": 200}
    except Exception as e:
        logging.critical(f"Error during {event['stage']}")
        raise Exception(e)


