SELECT contrato.id_contrato,
    contrato.id_base,
    punicao.documento AS cnpj,
    contrato.valor AS valor_contrato,
    contrato.data AS data_contrato
FROM {{ source('dados_cadastrais', 'punicao') }}
    JOIN {{ source('unibase', 'fornecedor') }} ON punicao.documento::TEXT = fornecedor.documento::TEXT
    JOIN {{ source('unibase', 'contrato') }} ON contrato.id_fornecedor = fornecedor.id_fornecedor
WHERE punicao.inicio_punicao < contrato.data
    AND punicao.fim_punicao > contrato.data