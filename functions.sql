
-- VALIDACAO OUTROS SINTOMAS E CONDICOES

CREATE OR REPLACE FUNCTION valida_outros_sintomas_condicoes() 
RETURNS TRIGGER
LANGUAGE plpgsql AS $$
DECLARE
    tem_outros_sintomas BOOLEAN;
	tem_outros_condicoes BOOLEAN;
BEGIN
    SELECT EXISTS (
        SELECT 1
        FROM notificacao_sintoma
        WHERE notificacao_id = NEW.notificacao_id
          AND sintoma_id = 10
    ) INTO tem_outros_sintomas;

    IF tem_outros_sintomas AND NEW.especificacao_outros_sintomas IS NULL THEN
        RAISE EXCEPTION 'Descrição de sintomas "outros" deve ser preenchida.';
    END IF;

    IF NOT tem_outros_sintomas AND NEW.especificacao_outros_sintomas IS NOT NULL THEN
        RAISE EXCEPTION 'Descrição de "outros sintomas" só pode ser preenchida se houver sintoma Outros.';
    END IF;

	SELECT EXISTS (
        SELECT 1
        FROM notificacao_condicao
        WHERE notificacao_id = NEW.notificacao_id
          AND condicao_id = 9
    ) INTO tem_outros_condicoes;
	
    IF tem_outros_condicoes AND NEW.especificacao_outras_condicoes IS NULL THEN
        RAISE EXCEPTION 'Descrição de condicoes "outros" deve ser preenchida.';
    END IF;

    IF NOT tem_outros_condicoes AND NEW.especificacao_outras_condicoes IS NOT NULL THEN
        RAISE EXCEPTION 'Descrição de "outras condicoes" só pode ser preenchida se houver condicao Outros.';
    END IF;

    RETURN NEW;
END;
$$;

CREATE TRIGGER trg_valida_outros_sintomas_condicoes
AFTER INSERT OR UPDATE ON dados_clinicos
FOR EACH ROW EXECUTE FUNCTION valida_outros_sintomas_condicoes();

-- VALIDACAO ESTADO TESTE =====================================

CREATE OR REPLACE FUNCTION validar_estado_teste()
RETURNS TRIGGER
LANGUAGE plpgsql AS $$
DECLARE
    v_count INT;
BEGIN
 
    SELECT COUNT(*) INTO v_count
    FROM teste_laboratorial
    WHERE notificacao_id = NEW.notificacao_id
      AND tipo_id = NEW.tipo_id;

    IF v_count >= 1 THEN

        IF NEW.estado_id IS NULL THEN
            RAISE EXCEPTION
                'Estado do teste é obrigatório quando o mesmo tipo de teste é realizado mais de uma vez.';
        END IF;
    END IF;

    RETURN NEW;
END;
$$;

CREATE TRIGGER trg_validar_estado_teste
BEFORE INSERT OR UPDATE ON teste_laboratorial
FOR EACH ROW
EXECUTE FUNCTION validar_estado_teste();



-- INDICADORES REGIONAIS =============================================

CREATE TABLE IF NOT EXISTS indicadores_regionais (
    indicador_id SERIAL PRIMARY KEY,
    municipio_id INT NOT NULL,
    data_inicio DATE NOT NULL,
    data_fim DATE NOT NULL,
    total_testes INT NOT NULL DEFAULT 0,
    testes_positivos INT NOT NULL DEFAULT 0,
    taxa_positividade NUMERIC(5,2) DEFAULT 0.00,
    data_atualizacao TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (municipio_id) REFERENCES municipio(municipio_id),
    UNIQUE (municipio_id, data_inicio, data_fim)
);

CREATE INDEX IF NOT EXISTS idx_indicadores_municipio ON indicadores_regionais(municipio_id);
CREATE INDEX IF NOT EXISTS idx_indicadores_periodo ON indicadores_regionais(data_inicio, data_fim);

CREATE OR REPLACE FUNCTION fx_calcular_taxa_positividade(
    p_data_inicio DATE,
    p_data_fim DATE
)
RETURNS TABLE (
    municipio_nome VARCHAR,
    total_testes BIGINT,
    testes_positivos BIGINT,
    taxa_positividade NUMERIC
)
LANGUAGE plpgsql AS $$
BEGIN

    INSERT INTO indicadores_regionais (municipio_id, data_inicio, data_fim, total_testes, testes_positivos, taxa_positividade)
    SELECT 
        n.municipio_notificacao_id AS municipio_id,
        p_data_inicio,
        p_data_fim,
        COUNT(tl.teste_id) AS total_testes,
        COUNT(CASE WHEN tl.resultado_id = 1 THEN 1 END) AS testes_positivos,
        CASE 
            WHEN COUNT(tl.teste_id) > 0 THEN 
                ROUND((COUNT(CASE WHEN tl.resultado_id = 1 THEN 1 END)::NUMERIC / COUNT(tl.teste_id)::NUMERIC) * 100, 2)
            ELSE 0
        END AS taxa_positividade
    FROM notificacao n
    INNER JOIN teste_laboratorial tl ON n.notificacao_id = tl.notificacao_id
    WHERE tl.data_coleta BETWEEN p_data_inicio AND p_data_fim
        AND tl.resultado_id IS NOT NULL
    GROUP BY n.municipio_notificacao_id
    ON CONFLICT (municipio_id, data_inicio, data_fim) 
    DO UPDATE SET
        total_testes = EXCLUDED.total_testes,
        testes_positivos = EXCLUDED.testes_positivos,
        taxa_positividade = EXCLUDED.taxa_positividade,
        data_atualizacao = CURRENT_TIMESTAMP;
    RETURN QUERY
    SELECT 
        m.nome::VARCHAR,
        ir.total_testes::BIGINT,
        ir.testes_positivos::BIGINT,
        ir.taxa_positividade::NUMERIC
    FROM indicadores_regionais ir
    INNER JOIN municipio m ON ir.municipio_id = m.municipio_id
    WHERE ir.data_inicio = p_data_inicio 
        AND ir.data_fim = p_data_fim
    ORDER BY ir.taxa_positividade DESC;
END;
$$;

-- calcular tempo médio entre início de sintomas e testagem
CREATE OR REPLACE FUNCTION fx_calcular_tempo_medio_sintomas_testagem(
    p_data_inicio DATE,
    p_data_fim DATE
)
RETURNS TABLE (
    municipio_nome VARCHAR,
    total_casos BIGINT,
    tempo_medio_dias NUMERIC
)
LANGUAGE plpgsql AS $$
BEGIN
    RETURN QUERY
    SELECT 
        m.nome::VARCHAR,
        COUNT(DISTINCT n.notificacao_id)::BIGINT AS total_casos,
        ROUND(AVG(tl.data_coleta - dc.data_inicio_sintomas)::NUMERIC, 2) AS tempo_medio_dias
    FROM notificacao n
    INNER JOIN municipio m ON n.municipio_notificacao_id = m.municipio_id
    INNER JOIN dados_clinicos dc ON n.notificacao_id = dc.notificacao_id
    INNER JOIN teste_laboratorial tl ON n.notificacao_id = tl.notificacao_id
    WHERE dc.data_inicio_sintomas BETWEEN p_data_inicio AND p_data_fim
        AND tl.data_coleta IS NOT NULL
        AND dc.data_inicio_sintomas IS NOT NULL
        AND tl.data_coleta >= dc.data_inicio_sintomas
    GROUP BY m.municipio_id, m.nome
    ORDER BY tempo_medio_dias ASC;
END;
$$;

-- calcular percentual de profissionais de saúde infectados
CREATE OR REPLACE FUNCTION fx_calcular_percentual_prof_saude_infectados(
    p_data_inicio DATE,
    p_data_fim DATE
)
RETURNS TABLE (
    municipio_nome VARCHAR,
    total_notificacoes BIGINT,
    total_prof_saude BIGINT,
    percentual_prof_saude NUMERIC
)
LANGUAGE plpgsql AS $$
BEGIN
    RETURN QUERY
    SELECT 
        m.nome::VARCHAR,
        COUNT(n.notificacao_id)::BIGINT AS total_notificacoes,
        COUNT(CASE WHEN n.profissional_saude = TRUE THEN 1 END)::BIGINT AS total_prof_saude,
        CASE 
            WHEN COUNT(n.notificacao_id) > 0 THEN 
                ROUND((COUNT(CASE WHEN n.profissional_saude = TRUE THEN 1 END)::NUMERIC / 
                       COUNT(n.notificacao_id)::NUMERIC) * 100, 2)
            ELSE 0
        END AS percentual_prof_saude
    FROM notificacao n
    INNER JOIN municipio m ON n.municipio_notificacao_id = m.municipio_id
    WHERE n.data_notificacao BETWEEN p_data_inicio AND p_data_fim
    GROUP BY m.municipio_id, m.nome
    ORDER BY percentual_prof_saude DESC;
END;
$$;

-- calcular média de doses de vacina aplicadas por município
CREATE OR REPLACE FUNCTION fx_calcular_media_doses_vacina_municipio(
    p_data_inicio DATE,
    p_data_fim DATE
)
RETURNS TABLE (
    municipio_nome VARCHAR,
    total_notificacoes BIGINT,
    total_doses BIGINT,
    media_doses_por_pessoa NUMERIC,
    primeira_dose BIGINT,
    segunda_dose BIGINT
)
LANGUAGE plpgsql AS $$
BEGIN
    RETURN QUERY
    SELECT 
        m.nome::VARCHAR,
        COUNT(DISTINCT n.notificacao_id)::BIGINT AS total_notificacoes,
        COUNT(nv.vacina_id)::BIGINT AS total_doses,
        CASE 
            WHEN COUNT(DISTINCT n.notificacao_id) > 0 THEN 
                ROUND(COUNT(nv.vacina_id)::NUMERIC / COUNT(DISTINCT n.notificacao_id)::NUMERIC, 2)
            ELSE 0
        END AS media_doses_por_pessoa,
        COUNT(CASE WHEN nv.dose_numero = 1 THEN 1 END)::BIGINT AS primeira_dose,
        COUNT(CASE WHEN nv.dose_numero = 2 THEN 1 END)::BIGINT AS segunda_dose
    FROM notificacao n
    INNER JOIN municipio m ON n.municipio_notificacao_id = m.municipio_id
    LEFT JOIN notificacao_vacina nv ON n.notificacao_id = nv.notificacao_id
    WHERE n.data_notificacao BETWEEN p_data_inicio AND p_data_fim
    GROUP BY m.municipio_id, m.nome
    ORDER BY media_doses_por_pessoa DESC;
END;
$$;

-- VIEWS =============================================

-- casos por município
CREATE OR REPLACE VIEW vw_casos_por_municipio AS
SELECT 
    m.municipio_id,
    m.nome AS municipio_nome,
    e.nome AS estado_nome,
    COUNT(n.notificacao_id) AS total_notificacoes,
    COUNT(CASE WHEN cf.descricao LIKE 'Confirmado%' THEN 1 END) AS casos_confirmados,
    COUNT(CASE WHEN cf.descricao = 'Descartado' THEN 1 END) AS casos_descartados,
    COUNT(CASE WHEN cf.descricao IS NULL THEN 1 END) AS casos_sem_classificacao,
    COUNT(CASE WHEN ec.descricao = 'Óbito' THEN 1 END) AS obitos
FROM notificacao n
INNER JOIN municipio m ON n.municipio_notificacao_id = m.municipio_id
INNER JOIN estado e ON m.estado_id = e.estado_id
LEFT JOIN dados_clinicos dc ON n.notificacao_id = dc.notificacao_id
LEFT JOIN classificacao_final cf ON dc.classificacao_final_id = cf.classificacao_final_id
LEFT JOIN evolucao_caso ec ON dc.evolucao_caso_id = ec.evolucao_caso_id
GROUP BY m.municipio_id, m.nome, e.nome
ORDER BY total_notificacoes DESC, m.nome;

-- vacinação por resultado de teste
CREATE OR REPLACE VIEW vw_vacinacao_por_resultado AS
SELECT 
    m.nome AS municipio_nome,
    CASE 
        WHEN rt.codigo = 1 THEN 'Positivo'
        WHEN rt.codigo = 2 THEN 'Negativo'
        WHEN rt.codigo = 3 THEN 'Inconclusivo'
        ELSE 'Sem Resultado'
    END AS resultado_teste,
    COUNT(DISTINCT n.notificacao_id) AS total_casos,
    COUNT(DISTINCT CASE WHEN nv.vacina_id IS NOT NULL THEN n.notificacao_id END) AS casos_vacinados,
    COUNT(DISTINCT CASE WHEN nv.vacina_id IS NULL THEN n.notificacao_id END) AS casos_nao_vacinados,
    COUNT(CASE WHEN nv.dose_numero = 1 THEN 1 END) AS total_primeira_dose,
    COUNT(CASE WHEN nv.dose_numero = 2 THEN 1 END) AS total_segunda_dose,
    ROUND(
        (COUNT(DISTINCT CASE WHEN nv.vacina_id IS NOT NULL THEN n.notificacao_id END)::NUMERIC / 
         NULLIF(COUNT(DISTINCT n.notificacao_id), 0)::NUMERIC) * 100, 2
    ) AS percentual_vacinados
FROM notificacao n
INNER JOIN municipio m ON n.municipio_notificacao_id = m.municipio_id
LEFT JOIN teste_laboratorial tl ON n.notificacao_id = tl.notificacao_id
LEFT JOIN resultado_teste rt ON tl.resultado_id = rt.codigo
LEFT JOIN notificacao_vacina nv ON n.notificacao_id = nv.notificacao_id
GROUP BY m.nome, rt.codigo
ORDER BY m.nome, rt.codigo;

-- sintomas frequentes entre casos confirmados
CREATE OR REPLACE VIEW vw_sintomas_frequentes AS
SELECT 
    s.sintoma_id,
    s.descricao AS sintoma_descricao,
    COUNT(ns.notificacao_id) AS total_ocorrencias,
    ROUND(
        (COUNT(ns.notificacao_id)::NUMERIC / 
         (SELECT COUNT(DISTINCT n.notificacao_id) 
          FROM notificacao n 
          INNER JOIN dados_clinicos dc ON n.notificacao_id = dc.notificacao_id
          INNER JOIN classificacao_final cf ON dc.classificacao_final_id = cf.classificacao_final_id
          WHERE cf.descricao LIKE 'Confirmado%')::NUMERIC) * 100, 2
    ) AS percentual_casos_confirmados,
    COUNT(DISTINCT CASE WHEN p.sexo_id = 1 THEN ns.notificacao_id END) AS casos_masculino,
    COUNT(DISTINCT CASE WHEN p.sexo_id = 2 THEN ns.notificacao_id END) AS casos_feminino,
    ROUND(AVG(p.idade), 1) AS idade_media
FROM sintoma s
INNER JOIN notificacao_sintoma ns ON s.sintoma_id = ns.sintoma_id
INNER JOIN notificacao n ON ns.notificacao_id = n.notificacao_id
INNER JOIN paciente p ON n.paciente_id = p.paciente_id
INNER JOIN dados_clinicos dc ON n.notificacao_id = dc.notificacao_id
INNER JOIN classificacao_final cf ON dc.classificacao_final_id = cf.classificacao_final_id
WHERE cf.descricao LIKE 'Confirmado%'
GROUP BY s.sintoma_id, s.descricao
ORDER BY total_ocorrencias DESC;

