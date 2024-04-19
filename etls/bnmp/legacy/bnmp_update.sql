-- Código usado para criar a procedure que identifica CPFs em `bnmp.mandados`

CREATE OR REPLACE PROCEDURE bnmp.id()
AS $$
BEGIN
    UPDATE
        bnmp.mandados bm
    SET
        cpf = id.cpf,
        metodo_identificacao_cpf = id.metodo_identificacao_cpf
    FROM
    (
        SELECT
            id.id,
            id.numero_mandado_prisao,
            id.id_pessoa,
            MAX(id.nome) AS nome,
            MAX(id.nome_mae) AS nome_mae,
            MAX(id.data_nascimento) AS data_nascimento,
            MAX(pt.cpf) AS cpf,
            'nome, data de nascimento, nome da mãe' AS metodo_identificacao_cpf
        FROM receita.pessoa_teste pt
        LEFT JOIN bnmp.mandados id ON (
            pt.nome = id.nome AND
            pt.nomemae = id.nome_mae AND
            DATE_CMP(TO_DATE(pt.datanascimento, 'YYYYMMDD'), data_nascimento) = 0
        )
        GROUP BY
            id.id,
            id.numero_mandado_prisao,
            id.id_pessoa
        HAVING
            COUNT(id.nome) = 1
    ) AS id
    WHERE
        (bm.cpf = '' OR bm.cpf IS NULL) AND 
        bm.id = id.id AND
        bm.numero_mandado_prisao = id.numero_mandado_prisao AND
        bm.id_pessoa = id.id_pessoa;

    UPDATE
        bnmp.mandados bm
    SET
        cpf = id.cpf,
        metodo_identificacao_cpf = id.metodo_identificacao_cpf
    FROM
    (
        SELECT
            id.id,
            id.numero_mandado_prisao,
            id.id_pessoa,
            MAX(id.nome) AS nome,
            MAX(id.nome_mae) AS nome_mae,
            MAX(id.data_nascimento) AS data_nascimento,
            MAX(pt.cpf) AS cpf,
            'nome, data de nascimento' AS metodo_identificacao_cpf
        FROM receita.pessoa_teste pt
        LEFT JOIN bnmp.mandados id ON (
            pt.nome = id.nome AND
            DATE_CMP(
            (
                CASE WHEN LEFT(pt.datanascimento, 10) ~
                    '^(19|20)[0-9][0-9]-[0-1][0-9]-[0-3][0-9]$'
                THEN TO_DATE(LEFT(pt.datanascimento, 10), 'YYYYMMDD')
                END
            ), data_nascimento) = 0
        )
        GROUP BY
            id.id,
            id.numero_mandado_prisao,
            id.id_pessoa
        HAVING
            COUNT(id.nome) = 1
    ) AS id
    WHERE
        (bm.cpf = '' OR bm.cpf IS NULL) AND 
        bm.id = id.id AND
        bm.numero_mandado_prisao = id.numero_mandado_prisao AND
        bm.id_pessoa = id.id_pessoa;

    UPDATE
        bnmp.mandados bm
    SET
        cpf = id.cpf,
        metodo_identificacao_cpf = id.metodo_identificacao_cpf
    FROM
    (
        SELECT
            id.id,
            id.numero_mandado_prisao,
            id.id_pessoa,
            MAX(id.nome) AS nome,
            MAX(id.nome_mae) AS nome_mae,
            MAX(id.data_nascimento) AS data_nascimento,
            MAX(pt.cpf) AS cpf,
            'nome, nome da mãe' AS metodo_identificacao_cpf
        FROM receita.pessoa_teste pt
        LEFT JOIN bnmp.mandados id ON (
            pt.nome = id.nome AND
            pt.nomemae = id.nome_mae
        )
        GROUP BY
            id.id,
            id.numero_mandado_prisao,
            id.id_pessoa
        HAVING
            COUNT(id.nome) = 1
    ) AS id
    WHERE
        (bm.cpf = '' OR bm.cpf IS NULL) AND 
        bm.id = id.id AND
        bm.numero_mandado_prisao = id.numero_mandado_prisao AND
        bm.id_pessoa = id.id_pessoa;

    UPDATE
        bnmp.mandados bm
    SET
        cpf = id.cpf,
        metodo_identificacao_cpf = id.metodo_identificacao_cpf
    FROM
    (
        SELECT
            id.id,
            id.numero_mandado_prisao,
            id.id_pessoa,
            MAX(id.nome) AS nome,
            MAX(id.nome_mae) AS nome_mae,
            MAX(id.data_nascimento) AS data_nascimento,
            MAX(pt.cpf) AS cpf,
            'nome' AS metodo_identificacao_cpf
        FROM receita.pessoa_teste pt
        LEFT JOIN bnmp.mandados id ON (pt.nome = id.nome)
        GROUP BY
            id.id,
            id.numero_mandado_prisao,
            id.id_pessoa
        HAVING
            COUNT(id.nome) = 1
    ) AS id
    WHERE
        (bm.cpf = '' OR bm.cpf IS NULL) AND 
        bm.id = id.id AND
        bm.numero_mandado_prisao = id.numero_mandado_prisao AND
        bm.id_pessoa = id.id_pessoa;
END;
$$ LANGUAGE plpgsql;