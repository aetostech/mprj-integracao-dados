"""Utility functions used throughout the BNMP workflow."""


from boto3.resources import factory

# from datetime import date
from typing import Any, Dict, List, Set, Union
import boto3
import csv
import json
import logging
import os
import re
import redshift_connector
import tempfile
import yaml


def config() -> Dict[Any, Any]:
    """Return configuration parameters.``.

    References the "config.yml" file stored in AWS S3.

    Returns:
        A dictionary containing all configuration extracted.
    """
    s3 = boto3.resource("s3")
    s3_obj = s3.Object("etl-bnmp", "code/config.yml")
    return yaml.safe_load(s3_obj.get()["Body"].read().decode("utf-8"))


def headers() -> Dict[Any, Any]:
    """Return configuration parameters.``.

    References the "headers.txt" file stored in AWS S3.

    Returns:
        A dictionary containing all configuration extracted.
    """
    s3 = boto3.client("s3")
    response = s3.get_object(Bucket="etl-bnmp", Key="headers.txt")
    return response["Body"].read().decode("utf-8")


cfg = config()

# Read logging configuration already set on the runtime, if any, or create one
# https://stackoverflow.com/a/56579088
if len(logging.getLogger().handlers) > 0:
    logging.getLogger().setLevel(logging.INFO)
else:
    logging.basicConfig(
        format=cfg["log"]["format"],
        datefmt=cfg["log"]["datefmt"],
        level=logging.INFO,
    )


def _clean_str(i: str) -> str:
    """Remove escape sequences and undesirable symbols from a string.

    This function is used before parsing Python dictonaries to JSON to remove
    unsuported characters.

    Args:
        i: String to be formatted.

    Returns:
        Formatted string.
    """
    # Remove backslash from invalid escape sequences
    i = re.sub(r'(?<!\\)\\(?!["\\/bfnrt]|u[0-9a-fA-F]{4})', " ", i)
    # Remove valid escape sequences
    escapes = "".join([chr(char) for char in range(1, 32)])
    translator = str.maketrans("", "", escapes)
    i = i.translate(translator)
    i = i.replace("'", " ").replace('"', " ")  # Remove quotes
    i = i.replace("|", " ")  # Remove pipe
    i = " ".join(i.split())  # Remove sequential whitespaces
    i = i.strip()  # Remove leading and trailing spaces
    return i


def clean(i: Union[str, list, dict]) -> Union[str, list, dict]:
    """Recursively remove special characters any string, list or dictionary.

    Args:
        i: String, List or Dictionary to be cleaned

    Returns:
        Cleaned object.
    """
    if isinstance(i, str):
        i = _clean_str(i)
    elif isinstance(i, list):
        i = [clean(s) for s in i]
    elif isinstance(i, dict):
        for k, v in i.items():
            i[k] = clean(v)
    return i


def csv_writer(
    data: Union[Set[Any], List[Any]], json_data: bool, path_and_file: str
) -> None:
    """Write a CSV file without headers.

    Args:
        data: Data to be written on the file. JSON fields must be formatted as
            strings such as ``"{'str': 'string', 'int': 0}"``
        json_data: Must be ``True`` if ``data`` contains one or more JSON
            fields. Disables quoting to avoid quoting already quoted fields.
        path_and_file: Filename with path as in ``path/dir/file.ext``.
            Temporary directories created with the ``tempfile`` module are
            fully supported.
    """
    with open(
        path_and_file, "a" if os.path.exists(path_and_file) else "w"
    ) as csvfile:
        if json_data:
            csvwriter = csv.writer(
                csvfile, delimiter="|", quoting=csv.QUOTE_NONE, quotechar=None
            )
        else:
            csvwriter = csv.writer(csvfile, delimiter="|", quotechar='"')
        for line in data:
            csvwriter.writerow(line)


def define_payload(d: Dict[Any, Any]) -> str:
    """Define and format a request payload based on the keys of a map.

    There are four request payload types ``doctype``, ``agency``, ``city``, and
    ``state``. This method detects which one is needed based on the keys that a
    given API map contain.
    The string won't be formatted with values it doesn't support.

    Args:
        d: An API map.

    Returns:
        A formatted payload based on the keys of the API map.
    """
    query_types: List[str] = ["doctype", "agency", "city", "state"]
    for query in query_types:
        if d.get(query):
            payload: str = cfg["payloads"][query]
            break

    # Due to named formatting, the payload string won't be formatted with
    # values it doesn't support.
    return payload.format(
        state=d.get("state"),
        city=d.get("city"),
        agency=d.get("agency"),
        doctype=d.get("doctype"),
    )


class S3:
    """Class dedicated for AWS S3 operations."""

    def __init__(self) -> None:
        """Extract credentials from environment, create S3 resource."""
        _session = boto3.Session(aws_session_token="token")
        _credentials = _session.get_credentials().get_frozen_credentials()

        self.s3: factory.s3.ServiceResource = boto3.resource(
            "s3",
            aws_access_key_id=_credentials.access_key,
            aws_secret_access_key=_credentials.secret_key,
        )

    def upload_file(
        self, *, bucket: str, name_in_s3: str, path_and_file: str
    ) -> None:
        """Upload file to S3.

        Args:
            bucket: Bucket name.
            name_in_s3: Must include prefixes as in ``path/dir/file.ext``.
            path_and_file: Local location of file to be uploaded.
        """
        try:
            self.s3.meta.client.upload_file(path_and_file, bucket, name_in_s3)
        except Exception as e:
            raise e


class Redshift:
    """Redshift manipulation suite."""

    def __init__(self) -> None:
        """Initialize Redshift credentials attribute."""
        self.redshift_params: dict = self.extract_creds()

    def extract_creds(self) -> dict:
        """Extract credentials from AWS Secrets Manager.

        Returns:
          Redshift credentials secret dictionary ready to be used by
          ``redshift_connector``.
        """
        session = boto3.session.Session()
        client = session.client(
            service_name="secretsmanager", region_name="us-east-1"
        )

        get_secret_value_response = client.get_secret_value(
            SecretId="robotRedshiftCredentials"
        )

        secret = json.loads(get_secret_value_response["SecretString"])

        #: ``redshift_connector.connect`` port parameter must be an integer
        secret["port"] = int(secret["port"])

        # Remove unnecessary keys
        del secret["engine"]
        del secret["dbClusterIdentifier"]

        return secret

    def extract_role(self) -> str:
        """Extract Redshift role for IAM operations.

        This method must be called whenever the IAM is needed and must not be
        called or included on the class state so we can differentiate the
        permissions given to each Lambda Function to extract each secret.

        Returns:
            A IAM credential.
        """
        session = boto3.session.Session()
        client = session.client(
            service_name="secretsmanager", region_name="us-east-1"
        )

        get_secret_value_response = client.get_secret_value(
            SecretId="redshiftRole"
        )

        secret = json.loads(get_secret_value_response["SecretString"])

        return secret["iam"]

    def run_query(self, query: str) -> None:
        """Run a SQL query.

        Args:
            query: A SQL query.
        """
        with redshift_connector.connect(
            **self.redshift_params
        ) as conn, conn.cursor() as curs:
            curs.execute(query)
            conn.commit()

    def copy(self, *, schema: str = "bnmp", table: str, uri: str) -> None:
        """Copy from S3 to Redshift.

        Args:
            schema: Schema of the target table
            table: Target table name
            uri: Uri of file in S3 to be copied
        """
        with redshift_connector.connect(
            **self.redshift_params
        ) as conn, conn.cursor() as curs:
            curs.execute(
                f"""
                    COPY {schema}.{table} FROM '{uri}'
                    WITH IAM_ROLE '{self.extract_role()}'
                    ENCODING AS UTF8 DELIMITER '|' REGION 'us-east-1';
                """
            )
            conn.commit()

    def copy_with_json_modified(self, *, table: str, uri: str):
        """Copy from S3 to Redshift via intermediary table.

        Args:
            schema: table schema
            table: table name
            uri: uri of CSV source file in S3
        """
        logging.info("Starting copy")
        copy_to_temp_sql: str = f"""
            COPY bnmp.{table}_temp
                (
                    id, numero_mandado_prisao, tipo_peca, status,
                    numero_processo, id_pessoa, nome, nome_mae, nome_pai,
                    data_nascimento, alcunha, pais_nascimento,
                    municipio_nascimento, uf_nascimento, sexo,
                    registro_judicial_individual,
                    numero_mandado_prisao_anterior, magistrado, tipo_prisao,
                    tempo_pena_ano, tempo_pena_mes, tempo_pena_dia,
                    regime_prisional, orgao_expedidor,
                    orgao_expedidor_municipio, orgao_expedidor_uf,
                    orgao_judiciario, orgao_judiciario_municipio,
                    orgao_judiciario_uf, sintese_decisao, data_expedicao,
                    data_validade, data_raspagem, data_visto_em, cpf,
                    metodo_identificacao_cpf, tipificacao, tipificacoes,
                    recaptura
                )
            FROM '{uri}'
            WITH IAM_ROLE '{self.extract_role()}'
            ENCODING AS UTF8
            DELIMITER '|'
            TRUNCATECOLUMNS
            REGION 'us-east-1';
        """

        with redshift_connector.connect(
            **self.redshift_params
        ) as conn, conn.cursor() as curs:
            logging.info("Copying to temporary table")
            curs.execute(copy_to_temp_sql)
            conn.commit()

            logging.info("Inserting new data on target")
            copy_sql: str = f"""
                INSERT INTO bnmp.{table}
                    SELECT
                        id, numero_mandado_prisao, tipo_peca, status,
                        numero_processo, id_pessoa, nome, nome_mae, nome_pai,
                        data_nascimento, alcunha, pais_nascimento,
                        municipio_nascimento, uf_nascimento, sexo,
                        registro_judicial_individual,
                        numero_mandado_prisao_anterior, magistrado,
                        tipo_prisao, tempo_pena_ano, tempo_pena_mes,
                        tempo_pena_dia, regime_prisional, orgao_expedidor,
                        orgao_expedidor_municipio, orgao_expedidor_uf,
                        orgao_judiciario, orgao_judiciario_municipio,
                        orgao_judiciario_uf, sintese_decisao, data_expedicao,
                        data_validade, data_raspagem, data_visto_em, cpf,
                        metodo_identificacao_cpf, tipificacao, tipificacoes,
                        recaptura
                    FROM {table}_temp
                ;"""
            curs.execute(copy_sql)
            conn.commit()


def csv_s3_redshift(
    data: Union[Set[Any], List[Any]],
    filename: str,
    json_data: bool,
    table: str,
):
    """Write CSV file, upload it to S3 and copy it to Redshift.

    Integrates multiple methods and classes in a compact form to avoid
    cluttering.

    Args:
        data: Data to be written in CSV.
        filename: Filename of CSV file. Must not contain directory information.
        json_data: Must be ``True`` if ``data`` contains one or more JSON
            fields. Disables quoting to avoid quoting already quoted fields.
        table: Target table name
    """
    logging.info("Creating temporary directory")
    with tempfile.TemporaryDirectory() as temp_dir:
        logging.info("Writing data to CSV")
        csv_writer(
            data=data,
            json_data=json_data,
            path_and_file=f"{temp_dir}/{filename}",
        )

        logging.info("Uploading data to S3")
        s3 = S3()
        s3.upload_file(
            bucket="etl-bnmp",
            name_in_s3=f"tmp/{filename}",
            path_and_file=f"{temp_dir}/{filename}",
        )

    logging.info("Copying data to Redshift.")
    redshift = Redshift()
    redshift.copy(
        table=table,
        uri=f"s3://etl-bnmp/tmp/{filename}",
    )


class IllegalArgumentError(ValueError):
    """Custom error for illegal arguments inherited from ``ValueError``.

    Used to specify error types in the AWS State Machine and provide special
    error handling on different situations.
    """

    pass


class InvalidCookieError(ValueError):
    """Custom error for invalid cookies inherited from ``ValueError``.

    Used to specify error types in the AWS State Machine and provide special
    error handling on different situations.
    """

    pass
