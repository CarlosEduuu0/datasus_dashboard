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
	CHECK (descricao IN ('Branca', 'Preta','Parda','Amarela','Indígena','Ignorado','NÃO INFORMADO'))
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
		CHECK (descricao IN ('Cancelado','Ignorado','Em tratamento domiciliar','Internado em UTI','Internado','Óbito','Cura','NÃO INFORMADO'))
);

CREATE TABLE IF NOT EXISTS classificacao_final(
	classificacao_final_id SERIAL PRIMARY KEY,
	descricao VARCHAR NOT NULL UNIQUE,
		CHECK (descricao IN ('Confirmado Laboratorial','Confirmado Clínico-Epidemiológico','Descartado','Síndrome Gripal Não Especificada','Confirmado','Confirmado por Critério Clínico', 'Clínico-Imagem','NÃO INFORMADO'))
);

CREATE TABLE IF NOT EXISTS sintoma(
	sintoma_id SERIAL PRIMARY KEY,
	descricao VARCHAR NOT NULL UNIQUE,
		CHECK (descricao IN ('Assintomático','Dor de Cabeça','Febre','Distúrbios Gustativos','Distúrbios Olfativos','Dor de Garganta','Dispneia','Tosse','Coriza','Outros','NÃO INFORMADO'))
);

CREATE TABLE IF NOT EXISTS condicao(
	condicao_id SERIAL PRIMARY KEY,
	descricao VARCHAR NOT NULL UNIQUE,
		CHECK (descricao IN ('Doenças respiratórias crônicas descompensadas','Outros','Doenças cardíacas crônicas','Diabetes','Doenças renais crônicas em estágio avançado (graus 3, 4 ou 5)','Imunossupressão','Portador de doenças cromossômicas ou estado de fragilidade imunológica','Puérpera (até 45 dias do parto)','Obesidade','Gestante','NÃO INFORMADO'))
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
		CHECK (dose_numero IN (1,2,3,4)),
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
    codigo VARCHAR NOT NULL UNIQUE 
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
	data_inicio_sintomas DATE,
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
	CONSTRAINT chk_codigo_vacina
		CHECK ((codigo_recebeu_vacina IS NOT NULL AND codigo_recebeu_vacina IN (1,2,3)) OR (codigo_recebeu_vacina IS NULL))
);

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