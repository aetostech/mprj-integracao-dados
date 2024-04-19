Module bnmp.workflow_setup
==========================
BNMP Workflow setup and cleanup.

Creates or drops temporary warrants tables.

Functions
---------

    
`setup(event: Dict[Union[str, Literal['stage']], Literal['setup', 'cleanup']], context: aws_lambda_powertools.utilities.typing.lambda_context.LambdaContext = None) ‑> Dict[Literal['success'], Literal[True]]`
:   Create or drop temporary warrants tables based on ``stage`` event value.
    
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

Classes
-------

`WorkflowSetup()`
:   BNMP Workflow setup and cleanup class.
    
    Set up config dictionary.

    ### Methods

    `drop_temp_tables(self) ‑> None`
    :   Drop temporary warrants tables on "cleanup" phase.

    `setup_temp_tables(self) ‑> None`
    :   Create and truncate temporary warrants tables on "setup" phase.
        
        The unvariable truncation assures that the warrants tables will always
        be empty no matter if recently created or not.