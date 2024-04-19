Module bnmp.bnmp_utils
======================
Utility functions used throughout the BNMP workflow.

Functions
---------

    
`clean(i: Union[str, list, dict]) ‑> Union[str, list, dict]`
:   Recursively remove special characters any string, list or dictionary.
    
    Args:
        i: String, List or Dictionary to be cleaned
    
    Returns:
        Cleaned object.

    
`config() ‑> Dict[Any, Any]`
:   Return configuration parameters.``.
    
    References the "config.yml" file stored in AWS S3.
    
    Returns:
        A dictionary containing all configuration extracted.

    
`csv_s3_redshift(data: Union[Set[Any], List[Any]], filename: str, json_data: bool, table: str)`
:   Write CSV file, upload it to S3 and copy it to Redshift.
    
    Integrates multiple methods and classes in a compact form to avoid
    cluttering.
    
    Args:
        data: Data to be written in CSV.
        filename: Filename of CSV file. Must not contain directory information.
        json_data: Must be ``True`` if ``data`` contains one or more JSON
            fields. Disables quoting to avoid quoting already quoted fields.
        table: Target table name

    
`csv_writer(data: Union[Set[Any], List[Any]], json_data: bool, path_and_file: str) ‑> None`
:   Write a CSV file without headers.
    
    Args:
        data: Data to be written on the file. JSON fields must be formatted as
            strings such as ``"{'str': 'string', 'int': 0}"``
        json_data: Must be ``True`` if ``data`` contains one or more JSON
            fields. Disables quoting to avoid quoting already quoted fields.
        path_and_file: Filename with path as in ``path/dir/file.ext``.
            Temporary directories created with the ``tempfile`` module are
            fully supported.

    
`define_payload(d: Dict[Any, Any]) ‑> str`
:   Define and format a request payload based on the keys of a map.
    
    There are four request payload types ``doctype``, ``agency``, ``city``, and
    ``state``. This method detects which one is needed based on the keys that a
    given API map contain.
    The string won't be formatted with values it doesn't support.
    
    Args:
        d: An API map.
    
    Returns:
        A formatted payload based on the keys of the API map.

Classes
-------

`IllegalArgumentError(*args, **kwargs)`
:   Custom error for illegal arguments inherited from ``ValueError``.
    
    Used to specify error types in the AWS State Machine and provide special
    error handling on different situations.

    ### Ancestors (in MRO)

    * builtins.ValueError
    * builtins.Exception
    * builtins.BaseException

`InvalidCookieError(*args, **kwargs)`
:   Custom error for invalid cookies inherited from ``ValueError``.
    
    Used to specify error types in the AWS State Machine and provide special
    error handling on different situations.

    ### Ancestors (in MRO)

    * builtins.ValueError
    * builtins.Exception
    * builtins.BaseException

`Redshift()`
:   Redshift manipulation suite.
    
    Initialize Redshift credentials attribute.

    ### Methods

    `copy(self, *, schema: str = 'bnmp', table: str, uri: str) ‑> None`
    :   Copy from S3 to Redshift.
        
        Args:
            schema: Schema of the target table
            table: Target table name
            uri: Uri of file in S3 to be copied

    `copy_with_json_modified(self, *, table: str, uri: str)`
    :   Copy from S3 to Redshift via intermediary table.
        
        Args:
            schema: table schema
            table: table name
            uri: uri of CSV source file in S3

    `extract_creds(self) ‑> dict`
    :   Extract credentials from AWS Secrets Manager.
        
        Returns:
          Redshift credentials secret dictionary ready to be used by
          ``redshift_connector``.

    `extract_role(self) ‑> str`
    :   Extract Redshift role for IAM operations.
        
        This method must be called whenever the IAM is needed and must not be
        called or included on the class state so we can differentiate the
        permissions given to each Lambda Function to extract each secret.
        
        Returns:
            A IAM credential.

    `run_query(self, query: str) ‑> None`
    :   Run a SQL query.
        
        Args:
            query: A SQL query.

`S3()`
:   Class dedicated for AWS S3 operations.
    
    Extract credentials from environment, create S3 resource.

    ### Methods

    `upload_file(self, *, bucket: str, name_in_s3: str, path_and_file: str) ‑> None`
    :   Upload file to S3.
        
        Args:
            bucket: Bucket name.
            name_in_s3: Must include prefixes as in ``path/dir/file.ext``.
            path_and_file: Local location of file to be uploaded.