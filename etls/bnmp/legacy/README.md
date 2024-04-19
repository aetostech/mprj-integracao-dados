# BNMP Scraper

## Cookie updates
New cookies are required on a daily basis. They can be extracted from any search request header on the [BNMP website](https://portalbnmp.cnj.jus.br/#/pesquisa-peca). They must be included in `config.yml`.
If `bnmp_utils.config` is set to extract data from S3 then a configuration file with valid cookies has to be stored in S3 before executing the code locally or doing a test run. Do a push to Git with a `config.yml` file containing valid cookies to fulfill this need.

## Requirements
Install requirements with `python -m pip install -r requirements.txt`

## Testing
`python3 -m pytest tests`

## About the code structure
This code was designed to be run on AWS Lambdas, having Step Functions to control the workflow and CloudWatch to log the code performance and behaviour.