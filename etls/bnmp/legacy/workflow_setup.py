"""BNMP Workflow setup and cleanup.

Creates or drops temporary warrants tables.
"""


from aws_lambda_powertools.utilities.typing import LambdaContext
from typing import Any, Dict, Literal, Union
import logging

from bnmp_utils import config, IllegalArgumentError, Redshift

cfg: Dict[str, Any] = config()

if len(logging.getLogger().handlers) > 0:
    logging.getLogger().setLevel(logging.INFO)
else:
    logging.basicConfig(
        format=cfg["log"]["format"],
        datefmt=cfg["log"]["datefmt"],
        level=logging.INFO,
    )


class WorkflowSetup:
    """BNMP Workflow setup and cleanup class."""

    def __init__(self) -> None:
        """Set up config dictionary."""
        logging.info("Initializing WorkflowSetup")
        self.redshift = Redshift()
        self.tables_queries = cfg["workflow_setup"]

    def setup_temp_tables(self) -> None:
        """Create and truncate temporary warrants tables on "setup" phase.

        The unvariable truncation assures that the warrants tables will always
        be empty no matter if recently created or not.
        """
        logging.info("Creating new warrants temporary table.")
        self.redshift.run_query(self.tables_queries["new"]["create"])
        logging.info("Truncating new warrants temporary table.")
        self.redshift.run_query(self.tables_queries["new"]["truncate"])

        logging.info("Creating old warrants temporary table.")
        self.redshift.run_query(self.tables_queries["old"]["create"])
        logging.info("Truncating old warrants temporary table.")
        self.redshift.run_query(self.tables_queries["old"]["truncate"])

        logging.info("Creating parsed warrants temporary table.")
        self.redshift.run_query(self.tables_queries["parsed"]["create"])
        logging.info("Truncating parsed warrants temporary table.")
        self.redshift.run_query(self.tables_queries["parsed"]["truncate"])

    def drop_temp_tables(self) -> None:
        """Drop temporary warrants tables on "cleanup" phase."""
        logging.info("Dropping new warrants temporary table.")
        self.redshift.run_query(self.tables_queries["new"]["drop"])

        logging.info("Dropping old warrants temporary table.")
        self.redshift.run_query(self.tables_queries["old"]["drop"])

        logging.info("Dropping parsed warrants temporary table.")
        self.redshift.run_query(self.tables_queries["parsed"]["drop"])


def setup(
    event: Dict[Union[str, Literal["stage"]], Literal["setup", "cleanup"]],
    context: LambdaContext = None,
) -> Dict[Literal["success"], Literal[True]]:
    """Create or drop temporary warrants tables based on ``stage`` event value.

    Creates and truncates temp tables on setup, drops temp tables on cleanup.

    Args:
        event: Must contain a ``stage`` key with a "setup" or "cleanup" value.
        context: A AWS Lambda Context given during the AWS Lambda execution.

    Raises:
        IllegalArgumentError: The event provided is invalid or incomplete.
        Exception: Any exception that occured during the SQL operations. The
        native exceptions are not always correctly raised by
        ``redshift_connector`` for a unknown reason. This pattern enforces any
        exception bubble up that does not occur natively.
    """
    if not event.get("stage"):
        error = "'stage' key not found"
    elif event["stage"] not in ["setup", "cleanup"]:
        error = "Unsuported 'stage' value"
    if "error" in locals():
        logging.critical(error)
        raise IllegalArgumentError(error)

    try:
        setup_ = WorkflowSetup()

        logging.info(f"Starting {event['stage']}")
        if event["stage"] == "setup":
            setup_.setup_temp_tables()
        elif event["stage"] == "cleanup":
            setup_.drop_temp_tables()

        logging.info(f"Completed {event['stage']}")
        return {"success": True}
    except Exception as e:
        logging.critical(f"Error during {event['stage']}")
        raise (e)
