"""BNMP warrants parsing and last_seen dates updating."""


from ast import literal_eval
from aws_lambda_powertools.utilities.typing import LambdaContext
from datetime import date
from typing import Any, Dict, Generator, Union
from unicodedata import normalize
import json
import logging
import redshift_connector

from bnmp_utils import config, csv_s3_redshift, Redshift

cfg: dict = config()

if len(logging.getLogger().handlers) > 0:
    logging.getLogger().setLevel(logging.INFO)
else:
    logging.basicConfig(
        format=cfg["log"]["format"],
        datefmt=cfg["log"]["datefmt"],
        level=logging.INFO,
    )


class Parser:
    """Parse data from raw and update last seen dates."""

    def __init__(self) -> None:
        """Extract DB connection parameters."""
        logging.info("Initializing Parser")
        self.redshift = Redshift()
        self.redshift_params = self.redshift.extract_creds()

        self.dct: Dict[str, Any] = {}

    def new_warrants(self) -> Generator:
        """Find warrants not parsed into ``bnmp.mandados``.

        Returns:
            Generator with warrants pending parsing and transfer.
        """
        logging.info("Retrieving non parsed ")
        with redshift_connector.connect(
            **self.redshift_params
        ) as conn, conn.cursor() as curs:
            curs.execute(cfg["parser"]["sql"]["select_unparsed"])
            yield from curs.fetchall()

    def parser(self, *keys, other_dict=False) -> Any:
        """Return data from valid dictionary keys.

        Uses a set of keys to recursively look for keys in a dictionary. Finds
        a deeply nested value in a dictionary originated from a JSON object.

        Args:
            keys: Keys that have to be iterated over.
            other_dict: A dictionary that is not `self.dct`, set on stage.

        Returns:
            An intermediary value of the loop or a specific desired object.

        Raises:
            KeyError: A key provided was not found.
        """
        dct = self.dct if not other_dict else other_dict
        for key in keys:
            try:
                dct = dct[key]
            except (IndexError, TypeError):
                return None
            except KeyError:
                raise KeyError("Invalid key.")
        try:
            dct = dct.decode("utf-8")
        except (UnicodeDecodeError, AttributeError):
            pass
        return dct.strip() if isinstance(dct, str) else dct

    def format_name(self, name: str) -> Union[str, None]:
        """Format names.

        Args:
            name: Name string

        Returns:
            Upercase name with no accents.
        """
        return (
            (
                normalize("NFKD", name)
                .upper()
                .encode("ascii", errors="ignore")
                .decode("utf-8")
            )
            if name is not None
            else None
        )

    def parse_detailed_json(self, warrant_data: list) -> Dict[str, Any]:
        """Parse JSON rows and create CSV log file.

        Args:
            warrant_data: List containing scrap date and raw JSON data of a
            warrant.

        Returns:
            Parsed warrant values.
        """
        self.dct = literal_eval(
            json.loads(warrant_data[2].replace("'", '\\"'))
        )

        try:
            data_nascimento: Union[str, None] = "-".join(
                self.parser(
                    "pessoa", "dataNascimento", 0, "dataNascimento"
                ).split("/")[::-1]
            )
        except AttributeError:
            data_nascimento = None

        tipificacoes: Union[str, None] = (
            json.dumps(
                [
                    self.parser("rotulo", other_dict=tip)
                    for tip in self.parser("tipificacaoPenal")
                ],
                ensure_ascii=False,
            )
            if self.parser("tipificacaoPenal") is not None
            and len(self.parser("tipificacaoPenal"))
            else None
        )

        data: Dict[str, Any] = {
            "id": self.parser("id"),
            "numero_mandado_prisao": self.parser("numeroPeca"),
            "tipo_peca": self.parser("tipoPeca", "id"),
            "status": self.parser("status", "descricao"),
            "numero_processo": self.parser("numeroProcesso"),
            "id_pessoa": self.parser("pessoa", "id"),
            "nome": self.format_name(
                self.parser("pessoa", "outrosNomes", 0, "nome")
            ),
            "nome_mae": self.format_name(
                self.parser("pessoa", "nomeMae", 0, "nome")
            ),
            "nome_pai": self.format_name(
                self.parser("pessoa", "nomePai", 0, "nome")
            ),
            "data_nascimento": data_nascimento,
            "alcunha": self.parser("pessoa", "outrasAlcunhas", 0, "nome"),
            "pais_nascimento": self.parser(
                "pessoa", "dadosGeraisPessoa", "paisNascimento", "nome"
            ),
            "municipio_nascimento": self.parser(
                "pessoa", "dadosGeraisPessoa", "naturalidade", "nome"
            ),
            "uf_nascimento": self.parser(
                "pessoa", "dadosGeraisPessoa", "naturalidade", "uf", "sigla"
            ),
            "sexo": self.parser(
                "pessoa", "dadosGeraisPessoa", "sexo", "descricao"
            ),
            "registro_judicial_individual": self.parser("numeroIndividuo"),
            "numero_mandado_prisao_anterior": self.parser(
                "numeroPecaAnterior"
            ),
            "magistrado": self.format_name(self.parser("magistrado")),
            "tipo_prisao": self.parser("especiePrisao"),
            "tempo_pena_ano": str(self.parser("tempoPenaAno") or 0),
            "tempo_pena_mes": str(self.parser("tempoPenaMes") or 0),
            "tempo_pena_dia": str(self.parser("tempoPenaDia") or 0),
            "regime_prisional": self.parser("regimePrisional"),
            "orgao_expedidor": self.parser("orgaoUsuarioCriador", "nome"),
            "orgao_expedidor_municipio": self.parser(
                "orgaoUsuarioCriador", "municipio", "nome"
            ),
            "orgao_expedidor_uf": self.parser(
                "orgaoUsuarioCriador", "municipio", "uf", "sigla"
            ),
            "orgao_judiciario": self.parser("orgaoJudiciario", "nome"),
            "orgao_judiciario_municipio": self.parser(
                "orgaoJudiciario", "municipio", "nome"
            ),
            "orgao_judiciario_uf": self.parser(
                "orgaoJudiciario", "municipio", "uf", "sigla"
            ),
            "sintese_decisao": " ".join(
                str(self.parser("sinteseDecisao")).split()
            ),
            "data_expedicao": str(self.parser("dataExpedicao"))[0:10],
            "data_validade": self.parser("dataValidade"),
            "data_raspagem": warrant_data[0],
            "data_visto_em": warrant_data[1],
            "cpf": None,
            "metodo_identificacao_cpf": None,
            "tipificacao": None,
            "tipificacoes": tipificacoes,
            "recaptura": self.parser("recaptura")[0],
        }

        if len(json.dumps(data, ensure_ascii=False, default=str)) > 60000:
            data["sintese_decisao"] = {}

        return data

    def parse_warrants(self) -> None:
        """Parse pending warrants and insert data into permanent tables."""
        logging.info("Inserting new raw warrants into 'bnmp.raw_mandados'")
        self.redshift.run_query(cfg["parser"]["sql"]["insert_raw"])

        logging.info("Warrants parsing initiated")
        warrants = list()
        for warrant in self.new_warrants():
            data = self.parse_detailed_json(warrant)
            warrants.append(tuple(v for k, v in data.items()))
        logging.info("Warrants parsing completed")

        csv_s3_redshift(
            data=warrants,
            filename="parsed_warrants.csv",
            json_data=True,
            table="bnmp_mandados_temp",
        )

        logging.info("Inserting new parsed warrants into 'bnmp.mandados'")
        self.redshift.run_query(cfg["parser"]["sql"]["insert_parsed"])

    def copy_dates(self) -> None:
        """Update last seen dates on raw and parsed warrants tables."""
        logging.info("Last seen date update started")
        today = date.today().strftime("%Y-%m-%d")
        logging.info("Updating dates on 'bnmp.raw_mandados'")
        self.redshift.run_query(
            cfg["parser"]["sql"]["update_dates_raw"].format(today=today)
        )
        logging.info("Updating dates on 'bnmp.mandados'")
        self.redshift.run_query(
            cfg["parser"]["sql"]["update_dates_parsed"].format(today=today)
        )
        logging.info("Last seen date update completed")


def parse(event: None = None, context: LambdaContext = None):
    """Parse raw JSON warrants data.

    Args:
        event: An empty event. Any event data will be ignored.
        context: A AWS Lambda Context given during the AWS Lambda execution.
    """
    parser = Parser()
    parser.parse_warrants()
    parser.copy_dates()
