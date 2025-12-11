--LEMBRETES ==============================================
-- outros em sintomas tem que ter id 10, condicoes tem que ser 9

-- TABELAS =============================================

-- IDENTIFICACAO DO PACIENTE ===========================
CREATE TABLE IF NOT EXISTS sexo(
	sexo_id SERIAL PRIMARY KEY,
	descricao VARCHAR NOT NULL UNIQUE,
	CHECK (descricao IN ('Masculino','Feminino'))
);

CREATE TABLE IF NOT EXISTS raca(
	raca_id SERIAL PRIMARY KEY,
	descricao VARCHAR NOT NULL UNIQUE,
	CHECK (descricao IN ('Branca', 'Preta','Parda','Amarela','Indígena','Ignorado'))
);

CREATE TABLE IF NOT EXISTS paciente(
	paciente_id SERIAL PRIMARY KEY,
	idade INT,
	sexo_id INT NOT NULL,
	raca_id INT NOT NULL,
	membro_povo_tradicional BOOLEAN NOT NULL DEFAULT FALSE,
	FOREIGN KEY (sexo_id)
		REFERENCES sexo(sexo_id),
	FOREIGN KEY (raca_id)
		REFERENCES raca(raca_id)
);

CREATE TABLE IF NOT EXISTS estado(
	estado_id SERIAL PRIMARY KEY,
	nome VARCHAR NOT NULL,
	codigo_ibge CHAR(2) UNIQUE
);

CREATE TABLE IF NOT EXISTS municipio(
	municipio_id SERIAL PRIMARY KEY,
	estado_id INT NOT NULL,
	nome VARCHAR NOT NULL,
	codigo_ibge CHAR(7) UNIQUE,
	FOREIGN KEY (estado_id)
		REFERENCES estado(estado_id)
);

CREATE TABLE IF NOT EXISTS residencia_paciente(
	paciente_id INT NOT NULL,
	municipio_id INT NOT NULL,
	PRIMARY KEY(paciente_id, municipio_id),
	FOREIGN KEY (paciente_id)
		REFERENCES paciente(paciente_id),
	FOREIGN KEY (municipio_id)
		REFERENCES municipio(municipio_id)
);

CREATE TABLE IF NOT EXISTS cbo(
	cbo_id SERIAL PRIMARY KEY,
	codigo CHAR(6) NOT NULL UNIQUE,
	titulo VARCHAR NOT NULL UNIQUE
);

-- ESTRATEGIAS E LOCAL DE REALIZACAO DA TESTAGEM =============

CREATE TABLE IF NOT EXISTS estrategia(
	codigo INT NOT NULL UNIQUE
		CHECK (codigo IN (1,2,3)),
	PRIMARY KEY (codigo)
);

CREATE TABLE IF NOT EXISTS local_testagem(
	codigo INT NOT NULL UNIQUE
		CHECK (codigo IN (1,2,3,4,5,6,7)),
	PRIMARY KEY (codigo)
);

-- DADOS CLINICOS EPIDEMIOLOGICOS ==================

CREATE TABLE IF NOT EXISTS evolucao_caso(
	evolucao_caso_id SERIAL PRIMARY KEY,
	descricao VARCHAR NOT NULL UNIQUE,
		CHECK (descricao IN ('Cancelado','Ignorado','Em tratamento domiciliar','Internado em UTI','Internado','Óbito','Cura'))
);

CREATE TABLE IF NOT EXISTS classificacao_final(
	classificacao_final_id SERIAL PRIMARY KEY,
	descricao VARCHAR NOT NULL UNIQUE,
		CHECK (descricao IN ('Confirmado Laboratorial','Confirmado Clínico-Epidemiológico','Descartado','Síndrome Gripal Não Especificada','Confirmado','Confirmado por Critério Clínico', 'Clínico-Imagem'))
);

CREATE TABLE IF NOT EXISTS sintoma(
	sintoma_id SERIAL PRIMARY KEY,
	descricao VARCHAR NOT NULL UNIQUE,
		CHECK (descricao IN ('Assintomático','Dor de Cabeça','Febre','Distúrbios Gustativos','Distúrbios Olfativos','Dor de Garganta','Dispneia','Tosse','Coriza','Outros'))
);

CREATE TABLE IF NOT EXISTS condicao(
	condicao_id SERIAL PRIMARY KEY,
	descricao VARCHAR NOT NULL UNIQUE,
		CHECK (descricao IN ('Doenças respiratórias crônicas descompensadas','Outros','Doenças cardíacas crônicas','Diabetes','Doenças renais crônicas em estágio avançado (graus 3, 4 ou 5)','Imunossupressão','Portador de doenças cromossômicas ou estado de fragilidade imunológica','Puérpera (até 45 dias do parto)','Obesidade','Gestante'))
);

CREATE TABLE IF NOT EXISTS laboratorio_vacina(
	laboratorio_vacina_id SERIAL PRIMARY KEY,
    nome VARCHAR NOT NULL UNIQUE
);

-- NOTIFICACAO E TABELAS DEPENDENTES ==============================

CREATE TABLE IF NOT EXISTS notificacao(
	notificacao_id SERIAL PRIMARY KEY,
	paciente_id INT NOT NULL,
	municipio_notificacao_id INT NOT NULL,
	cbo_id INT,
	profissional_saude BOOLEAN NOT NULL DEFAULT FALSE,
	profissional_seguranca BOOLEAN,
	data_notificacao DATE NOT NULL,
	origem VARCHAR,
	excluido BOOLEAN,
	validado BOOLEAN,
	FOREIGN KEY (paciente_id)
		REFERENCES paciente(paciente_id),
	FOREIGN KEY (municipio_notificacao_id)
		REFERENCES municipio(municipio_id),
	FOREIGN KEY (cbo_id)
		REFERENCES cbo(cbo_id),
	CONSTRAINT chk_cbo
		CHECK ((cbo_id IS NOT NULL AND profissional_saude = TRUE) OR (cbo_id IS NULL AND profissional_saude = FALSE)),
	CONSTRAINT chk_data_notificacao
		CHECK (data_notificacao > '01/01/2020' AND data_notificacao < CURRENT_DATE)
);

CREATE TABLE notificacao_vacina (
    vacina_id SERIAL PRIMARY KEY,
    notificacao_id INT NOT NULL,
    dose_numero INT NOT NULL 
		CHECK (dose_numero IN (1,2)),
    data_vacinacao DATE,
    lab_id INT,
    lote VARCHAR,
    FOREIGN KEY (notificacao_id) REFERENCES notificacao(notificacao_id),
    FOREIGN KEY (lab_id) REFERENCES laboratorio_vacina(laboratorio_vacina_id)
);

CREATE TABLE IF NOT EXISTS notificacao_sintoma(
	notificacao_id INT NOT NULL,
    sintoma_id INT NOT NULL,
    PRIMARY KEY (notificacao_id, sintoma_id),
    FOREIGN KEY (notificacao_id) REFERENCES notificacao(notificacao_id),
    FOREIGN KEY (sintoma_id) REFERENCES sintoma(sintoma_id)
);

CREATE TABLE IF NOT EXISTS notificacao_condicao (
    notificacao_id INT NOT NULL,
    condicao_id INT NOT NULL,
    PRIMARY KEY (notificacao_id, condicao_id),
    FOREIGN KEY (notificacao_id) REFERENCES notificacao(notificacao_id),
    FOREIGN KEY (condicao_id) REFERENCES condicao(condicao_id)
);

CREATE TABLE IF NOT EXISTS fabricante_teste (
    fabricante_id SERIAL PRIMARY KEY,
    codigo VARCHAR NOT NULL UNIQUE,
	descricao VARCHAR UNIQUE
);

CREATE TABLE IF NOT EXISTS resultado_teste (
    codigo INT NOT NULL UNIQUE
		CHECK (codigo IN (1,2,3)),
	PRIMARY KEY (codigo)
);

CREATE TABLE IF NOT EXISTS tipo_teste (
    codigo INT NOT NULL UNIQUE
		CHECK (codigo IN (1,2,3,4,5,6,7,8,9)),
	PRIMARY KEY (codigo)
);

CREATE TABLE IF NOT EXISTS estado_teste (
   codigo INT NOT NULL UNIQUE
		CHECK (codigo IN (1,2,3,4)),
	PRIMARY KEY (codigo)
);

CREATE TABLE  IF NOT EXISTS teste_laboratorial (
    teste_id SERIAL PRIMARY KEY,
    notificacao_id INT NOT NULL,
    resultado_id INT,
    fabricante_id INT,
    tipo_id INT,
    estado_id INT,
    data_coleta DATE,
    FOREIGN KEY (notificacao_id)
        REFERENCES notificacao(notificacao_id)
        ON DELETE CASCADE,
    FOREIGN KEY (resultado_id)
        REFERENCES resultado_teste(codigo),
    FOREIGN KEY (fabricante_id)
        REFERENCES fabricante_teste(fabricante_id),
    FOREIGN KEY (tipo_id)
        REFERENCES tipo_teste(codigo),
    FOREIGN KEY (estado_id)
        REFERENCES estado_teste(codigo),
	CHECK (tipo_id IS NOT NULL),
    CHECK (estado_id IN (1,2,3,4)),

    CONSTRAINT chk_estado_teste
    CHECK (
        (estado_id = 1 AND resultado_id IS NULL AND fabricante_id IS NULL AND data_coleta IS NULL)
        OR
        (estado_id = 2 AND data_coleta IS NOT NULL)
        OR
        (estado_id = 3 AND resultado_id IS NOT NULL AND data_coleta IS NOT NULL)
        OR
        (estado_id = 4 AND resultado_id IS NULL AND fabricante_id IS NULL AND data_coleta IS NULL)
    )
);

CREATE TABLE IF NOT EXISTS dados_estrategia_local_testagem(
	notificacao_id INT NOT NULL UNIQUE,
	local_testagem_id INT NOT NULL,
	especificacao_outro_testagem VARCHAR,
	estrategia_id INT,
	codigo_busca_ativa INT,
	codigo_triagem_especifica INT,
	especificacao_outro_ba VARCHAR,
	especificacao_outro_te VARCHAR,
	FOREIGN KEY (notificacao_id)
		REFERENCES notificacao(notificacao_id),
	FOREIGN KEY (estrategia_id)
		REFERENCES estrategia(codigo),
	FOREIGN KEY (local_testagem_id)
		REFERENCES local_testagem(codigo),
	CONSTRAINT chk_busca_ativa
		CHECK ((estrategia_id = 2 AND codigo_busca_ativa IN (1,2,3,4)) OR (estrategia_id <> 2 AND codigo_busca_ativa IS NULL)),
	CONSTRAINT chk_outro_busca_ativa
		CHECK ((codigo_busca_ativa = 4 AND especificacao_outro_ba IS NOT NULL) OR (codigo_busca_ativa <> 4 AND especificacao_outro_ba IS NULL)),
	CONSTRAINT chk_triagem_especifica
		CHECK ((estrategia_id = 3 AND codigo_triagem_especifica IN (1,2,3,4,5)) OR (estrategia_id <> 3 AND codigo_triagem_especifica IS NULL)),
	CONSTRAINT chk_outro_triagem_especifica
		CHECK ((codigo_triagem_especifica  = 5 AND especificacao_outro_te IS NOT NULL) OR (codigo_triagem_especifica  <> 5 AND especificacao_outro_te IS NULL)),
	CONSTRAINT chk_outro_testagem
		CHECK ((local_testagem_id = 7 AND especificacao_outro_testagem IS NOT NULL) OR (local_testagem_id <> 7 AND especificacao_outro_testagem IS NULL))
);

CREATE TABLE IF NOT EXISTS dados_clinicos(
	notificacao_id INT NOT NULL UNIQUE,
	data_inicio_sintomas DATE NOT NULL,
	classificacao_final_id INT,
	evolucao_caso_id INT,
	especificacao_outros_sintomas VARCHAR,
	especificacao_outras_condicoes VARCHAR,
	codigo_recebeu_vacina INT,
	data_encerramento DATE,
	FOREIGN KEY (notificacao_id)
		REFERENCES notificacao(notificacao_id),
	FOREIGN KEY (evolucao_caso_id)
		REFERENCES evolucao_caso(evolucao_caso_id),
	FOREIGN KEY (classificacao_final_id)
		REFERENCES classificacao_final(classificacao_final_id),
	CONSTRAINT chk_data_encerramento
		CHECK ((data_encerramento IS NOT NULL AND evolucao_caso_id IS NOT NULL) OR (data_encerramento IS NULL AND evolucao_caso_id IS NULL)),
	CONSTRAINT chk_codigo_vacina
		CHECK ((codigo_recebeu_vacina IS NOT NULL AND codigo_recebeu_vacina IN (1,2,3)) OR (codigo_recebeu_vacina IS NULL))
);


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

-- AUDITORIA =============================================

-- Tabela de log de alterações
CREATE TABLE IF NOT EXISTS log_alteracoes (
    log_id SERIAL PRIMARY KEY,
    tabela VARCHAR(50) NOT NULL,
    operacao VARCHAR(10) NOT NULL CHECK (operacao IN ('INSERT', 'UPDATE', 'DELETE')),
    registro_id INT NOT NULL,
    usuario VARCHAR(100) DEFAULT CURRENT_USER,
    data_hora TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Índices para melhorar performance de consultas
CREATE INDEX IF NOT EXISTS idx_log_tabela ON log_alteracoes(tabela);
CREATE INDEX IF NOT EXISTS idx_log_operacao ON log_alteracoes(operacao);
CREATE INDEX IF NOT EXISTS idx_log_data_hora ON log_alteracoes(data_hora);

-- Função de auditoria para notificacao
CREATE OR REPLACE FUNCTION auditoria_notificacao()
RETURNS TRIGGER
LANGUAGE plpgsql AS $$
BEGIN
    IF (TG_OP = 'DELETE') THEN
        INSERT INTO log_alteracoes (tabela, operacao, registro_id)
        VALUES (
            'notificacao',
            'DELETE',
            OLD.notificacao_id
        );
        RETURN OLD;
    ELSIF (TG_OP = 'UPDATE') THEN
        INSERT INTO log_alteracoes (tabela, operacao, registro_id)
        VALUES (
            'notificacao',
            'UPDATE',
            NEW.notificacao_id
        );
        RETURN NEW;
    ELSIF (TG_OP = 'INSERT') THEN
        INSERT INTO log_alteracoes (tabela, operacao, registro_id)
        VALUES (
            'notificacao',
            'INSERT',
            NEW.notificacao_id
        );
        RETURN NEW;
    END IF;
    RETURN NULL;
END;
$$;

CREATE TRIGGER trg_auditoria_notificacao
AFTER INSERT OR UPDATE OR DELETE ON notificacao
FOR EACH ROW EXECUTE FUNCTION auditoria_notificacao();

-- Função de auditoria para teste_laboratorial
CREATE OR REPLACE FUNCTION auditoria_teste_laboratorial()
RETURNS TRIGGER
LANGUAGE plpgsql AS $$
BEGIN
    IF (TG_OP = 'DELETE') THEN
        INSERT INTO log_alteracoes (tabela, operacao, registro_id)
        VALUES ('teste_laboratorial', 'DELETE', OLD.teste_id);
        RETURN OLD;
    ELSIF (TG_OP = 'UPDATE') THEN
        INSERT INTO log_alteracoes (tabela, operacao, registro_id)
        VALUES ('teste_laboratorial', 'UPDATE', NEW.teste_id);
        RETURN NEW;
    ELSIF (TG_OP = 'INSERT') THEN
        INSERT INTO log_alteracoes (tabela, operacao, registro_id)
        VALUES ('teste_laboratorial', 'INSERT', NEW.teste_id);
        RETURN NEW;
    END IF;
    RETURN NULL;
END;
$$;

CREATE TRIGGER trg_auditoria_teste_laboratorial
AFTER INSERT OR UPDATE OR DELETE ON teste_laboratorial
FOR EACH ROW EXECUTE FUNCTION auditoria_teste_laboratorial();

-- Função de auditoria para paciente
CREATE OR REPLACE FUNCTION auditoria_paciente()
RETURNS TRIGGER
LANGUAGE plpgsql AS $$
BEGIN
    IF (TG_OP = 'DELETE') THEN
        INSERT INTO log_alteracoes (tabela, operacao, registro_id)
        VALUES ('paciente', 'DELETE', OLD.paciente_id);
        RETURN OLD;
    ELSIF (TG_OP = 'UPDATE') THEN
        INSERT INTO log_alteracoes (tabela, operacao, registro_id)
        VALUES ('paciente', 'UPDATE', NEW.paciente_id);
        RETURN NEW;
    ELSIF (TG_OP = 'INSERT') THEN
        INSERT INTO log_alteracoes (tabela, operacao, registro_id)
        VALUES ('paciente', 'INSERT', NEW.paciente_id);
        RETURN NEW;
    END IF;
    RETURN NULL;
END;
$$;

CREATE TRIGGER trg_auditoria_paciente
AFTER INSERT OR UPDATE OR DELETE ON paciente
FOR EACH ROW EXECUTE FUNCTION auditoria_paciente();

-- Função de auditoria para dados_clinicos
CREATE OR REPLACE FUNCTION auditoria_dados_clinicos()
RETURNS TRIGGER
LANGUAGE plpgsql AS $$
BEGIN
    IF (TG_OP = 'DELETE') THEN
        INSERT INTO log_alteracoes (tabela, operacao, registro_id)
        VALUES ('dados_clinicos', 'DELETE', OLD.notificacao_id);
        RETURN OLD;
    ELSIF (TG_OP = 'UPDATE') THEN
        INSERT INTO log_alteracoes (tabela, operacao, registro_id)
        VALUES ('dados_clinicos', 'UPDATE', NEW.notificacao_id);
        RETURN NEW;
    ELSIF (TG_OP = 'INSERT') THEN
        INSERT INTO log_alteracoes (tabela, operacao, registro_id)
        VALUES ('dados_clinicos', 'INSERT', NEW.notificacao_id);
        RETURN NEW;
    END IF;
    RETURN NULL;
END;
$$;

CREATE TRIGGER trg_auditoria_dados_clinicos
AFTER INSERT OR UPDATE OR DELETE ON dados_clinicos
FOR EACH ROW EXECUTE FUNCTION auditoria_dados_clinicos();

-- Função de auditoria para notificacao_vacina
CREATE OR REPLACE FUNCTION auditoria_notificacao_vacina()
RETURNS TRIGGER
LANGUAGE plpgsql AS $$
BEGIN
    IF (TG_OP = 'DELETE') THEN
        INSERT INTO log_alteracoes (tabela, operacao, registro_id)
        VALUES ('notificacao_vacina', 'DELETE', OLD.vacina_id);
        RETURN OLD;
    ELSIF (TG_OP = 'UPDATE') THEN
        INSERT INTO log_alteracoes (tabela, operacao, registro_id)
        VALUES ('notificacao_vacina', 'UPDATE', NEW.vacina_id);
        RETURN NEW;
    ELSIF (TG_OP = 'INSERT') THEN
        INSERT INTO log_alteracoes (tabela, operacao, registro_id)
        VALUES ('notificacao_vacina', 'INSERT', NEW.vacina_id);
        RETURN NEW;
    END IF;
    RETURN NULL;
END;
$$;

CREATE TRIGGER trg_auditoria_notificacao_vacina
AFTER INSERT OR UPDATE OR DELETE ON notificacao_vacina
FOR EACH ROW EXECUTE FUNCTION auditoria_notificacao_vacina();

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
    n.data_notificacao,
    COUNT(n.notificacao_id) AS total_notificacoes,
    COUNT(CASE WHEN cf.descricao LIKE 'Confirmado%' THEN 1 END) AS casos_confirmados,
    COUNT(CASE WHEN cf.descricao = 'Descartado' THEN 1 END) AS casos_descartados,
    COUNT(CASE WHEN cf.descricao IS NULL THEN 1 END) AS casos_sem_classificacao,
    COUNT(CASE WHEN dc.evolucao_caso_id = 6 THEN 1 END) AS obitos
FROM notificacao n
INNER JOIN municipio m ON n.municipio_notificacao_id = m.municipio_id
INNER JOIN estado e ON m.estado_id = e.estado_id
LEFT JOIN dados_clinicos dc ON n.notificacao_id = dc.notificacao_id
LEFT JOIN classificacao_final cf ON dc.classificacao_final_id = cf.classificacao_final_id
GROUP BY m.municipio_id, m.nome, e.nome, n.data_notificacao
ORDER BY n.data_notificacao DESC, m.nome;

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

