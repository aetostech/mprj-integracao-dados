# Linker ETL

O objetivo deste projeto é agrupar os diversos códigos de raspagem que alimentam as bases de dados do Linker.

[![instituicoesfinanceiras](https://github.com/aetostech/linker-etl/actions/workflows/instituicoesfinanceiras.yml/badge.svg)](https://github.com/aetostech/linker-etl/actions/workflows/instituicoesfinanceiras.yml)
[![sancoes](https://github.com/aetostech/linker-etl/actions/workflows/sancoes.yml/badge.svg)](https://github.com/aetostech/linker-etl/actions/workflows/sancoes.yml)
[![PEP](https://github.com/aetostech/linker-etl/actions/workflows/pep.yml/badge.svg)](https://github.com/aetostech/linker-etl/actions/workflows/pep.yml)

## Diretórios

- [`.github/workflows`](.github/workflows/README.md): Automação, CI/CD

- [`beneficios_sociais`](beneficios_sociais/README.md): Beneficiários sociais
- [`bnmp`](bnmp/README.md): Foragidos e procurados
- [`entities`](entities/README.md): Organização de pessoas e empresas em entidades no banco de dados `entities`
- [`inativos`](inativos/README.md): Código legado ou simplesmente não atualizado atualmente
  - [`advogados`](advogados/README.md): Advogados particulares e advogados de risco
  - [`aero`](aero/README.md): Proprietários e operadores de aeronaves
  - [`alerts_google`](alerts_google/README.md): Scrapper para pegar notícias do google alerts
  - [`bnmp_sets`](bnmp_sets/README.md): Conjuntos Específicos gerados a partir de dados do BNMP
  - [`ctbl`](ctbl/README.md): Contadores e escritórios de contabilidade
  - [`devedoras`](devedoras/README.md): Empresas devedoras a partir do PGFN
  - [`estudos-ld`](estudos-ld/README.md): Estudos estatísticos e calibragem do Linker
  - [`foar`](foar/README.md): Forças armadas e militares
  - [`fornecedores_licitantes`](fornecedores_licitantes/README.md): Fornecedores e licitantes de órgãos públicos
  - [`icij`](icij/README.md): Extração de entidades de interesse dos vazamentos feitos pelo ICIJ
  - [`identification_tools`](identification_tools/README.md): Identificação de indivíduos
  - [`imp_exp`](imp_exp/README.md): Extração de dados de empresas de comércio exterior publicadas pela Receita Federal
  - [`poas`](poas/README.md):  Policiais e outros agentes de segurança
  - [`polt`](polt//README.md):  Políticos
  - [`scraper_procurados_rj`](scraper_procurados_rj/README.md):  Código para extração dos dados do procurados.org
- [`instituicoes_financeiras`](instituicoes_financeiras/README.md): Instituições autorizadas pelo BACEN
- [`receita_federal`](receita_federal/README.md): ETL de dados cadastrais de CNPJs e sócios da Receita Federal
- [`pessoas_expostas_politicamente`](pessoas_expostas_politicamente/README.md): Pessoas expostas politicamente
- [`sancoes`](sancoes): Sanções aplicadas no âmbito federal

Arquivos CSV antigos foram armazenados no [Bucket `linker-etl` no S3](https://s3.console.aws.amazon.com/s3/buckets/linker-etl?prefix=git/&region=us-east-1).