CREATE TABLE IF NOT EXISTS dw.dim_entidade (
    id_entidade SERIAL PRIMARY KEY,
    cod_concedente TEXT,
    cod_financiador TEXT,
    cod_gestora TEXT,
    cod_orgao TEXT,
    cod_secretaria TEXT,
    cpf_cnpj_financiador TEXT,
    plain_cpf_cnpj_financiador TEXT,
    descricao_nome_credor TEXT
);

CREATE TABLE IF NOT EXISTS dw.dim_modalidade (
    id_modalidade SERIAL PRIMARY KEY,
    descricao_modalidade TEXT,
    descricao_tipo TEXT,
    flg_tipo BIGINT,
    isn_modalidade BIGINT,
    descricao_justificativa TEXT
);

CREATE TABLE IF NOT EXISTS dw.dim_projeto (
    id_projeto SERIAL PRIMARY KEY,
    descricao_objeto TEXT,
    tipo_objeto TEXT,
    cod_plano_trabalho TEXT,
    num_spu TEXT,
    num_spu_licitacao TEXT,
    descricao_edital TEXT
);

CREATE TABLE IF NOT EXISTS dw.dim_contrato (
    id_contrato SERIAL PRIMARY KEY,
    num_contrato TEXT,
    plain_num_contrato TEXT,
    contract_type TEXT,
    infringement_status BIGINT,
    cod_financiador_including_zeroes TEXT,
    accountability_status TEXT,
    descricao_situacao TEXT
);

CREATE TABLE IF NOT EXISTS dw.dim_participacao (
    id_participacao SERIAL PRIMARY KEY,
    isn_parte_destino BIGINT,
    isn_parte_origem TEXT,
    isn_sic BIGINT,
    isn_entidade BIGINT,
    gestor_contrato TEXT,
    num_certidao TEXT
);

-- Insere dados na tabela de dimensão 'dim_entidade' a partir de 'contratos'
INSERT INTO dw.dim_entidade (
    cod_concedente, 
    cod_financiador, 
    cod_gestora, 
    cod_orgao, 
    cod_secretaria, 
    cpf_cnpj_financiador, 
    plain_cpf_cnpj_financiador, 
    descricao_nome_credor
)
SELECT DISTINCT 
    cod_concedente, 
    cod_financiador, 
    cod_gestora, 
    cod_orgao, 
    cod_secretaria, 
    cpf_cnpj_financiador, 
    plain_cpf_cnpj_financiador, 
    descricao_nome_credor
FROM 
    stage.contratos
ON CONFLICT DO NOTHING;

-- Insere dados na tabela de dimensão 'dim_entidade' a partir de 'convenios'
INSERT INTO dw.dim_entidade (
    cod_concedente, 
    cod_financiador, 
    cod_gestora, 
    cod_orgao, 
    cod_secretaria, 
    cpf_cnpj_financiador, 
    plain_cpf_cnpj_financiador, 
    descricao_nome_credor
)
SELECT DISTINCT 
    cod_concedente, 
    cod_financiador, 
    cod_gestora, 
    cod_orgao, 
    cod_secretaria, 
    cpf_cnpj_financiador, 
    plain_cpf_cnpj_financiador, 
    descricao_nome_credor
FROM 
    stage.convenios
ON CONFLICT DO NOTHING;

-- Dimensão Modalidade
INSERT INTO dw.dim_modalidade (
    descricao_modalidade, 
    descricao_tipo, 
    flg_tipo, 
    isn_modalidade, 
    descricao_justificativa
)
SELECT DISTINCT 
    decricao_modalidade, 
    descricao_tipo, 
    flg_tipo, 
    isn_modalidade, 
    descricao_justificativa
FROM stage.contratos
ON CONFLICT DO NOTHING;

INSERT INTO dw.dim_modalidade (
    descricao_modalidade, 
    descricao_tipo, 
    flg_tipo, 
    isn_modalidade, 
    descricao_justificativa
)
SELECT DISTINCT 
    decricao_modalidade, 
    descricao_tipo, 
    flg_tipo, 
    isn_modalidade, 
    descricao_justificativa
FROM stage.convenios
ON CONFLICT DO NOTHING;

-- Dimensão Projeto
INSERT INTO dw.dim_projeto (
    descricao_objeto, 
    tipo_objeto, 
    cod_plano_trabalho, 
    num_spu, 
    num_spu_licitacao, 
    descricao_edital
)
SELECT DISTINCT 
    descricao_objeto, 
    tipo_objeto, 
    cod_plano_trabalho, 
    num_spu, 
    num_spu_licitacao, 
    descriaco_edital
FROM stage.contratos
ON CONFLICT DO NOTHING;

INSERT INTO dw.dim_projeto (
    descricao_objeto, 
    tipo_objeto, 
    cod_plano_trabalho, 
    num_spu, 
    num_spu_licitacao, 
    descricao_edital
)
SELECT DISTINCT 
    descricao_objeto, 
    tipo_objeto, 
    cod_plano_trabalho, 
    num_spu, 
    num_spu_licitacao, 
    descriaco_edital
FROM stage.convenios
ON CONFLICT DO NOTHING;

-- Inserção na Dimensão Contrato a partir de Contratos
INSERT INTO dw.dim_contrato (
    num_contrato, 
    plain_num_contrato, 
    contract_type, 
    infringement_status, 
    cod_financiador_including_zeroes, 
    accountability_status, 
    descricao_situacao
)
SELECT DISTINCT 
    num_contrato, 
    plain_num_contrato, 
    contract_type, 
    infringement_status, 
    cod_financiador_including_zeroes, 
    accountability_status, 
    descricao_situacao
FROM 
    stage.contratos
ON conflict DO NOTHING;

-- Inserção na Dimensão Contrato a partir de Convenios
-- (Assumindo que os campos sejam compatíveis e que 'num_contrato' seja único)
INSERT INTO dw.dim_contrato (
    num_contrato, 
    plain_num_contrato, 
    contract_type, 
    infringement_status, 
    cod_financiador_including_zeroes, 
    accountability_status, 
    descricao_situacao
)
SELECT DISTINCT 
    num_contrato, 
    plain_num_contrato, 
    contract_type, 
    infringement_status, 
    cod_financiador_including_zeroes, 
    accountability_status, 
    descricao_situacao
FROM 
    stage.convenios
ON CONFLICT DO NOTHING;

-- Inserção na Dimensão Participação a partir de Contratos
-- (Os campos devem ser ajustados conforme a estrutura exata da sua tabela 'stage.contratos')
INSERT INTO dw.dim_participacao (
    isn_parte_destino, 
    isn_parte_origem, 
    isn_sic, isn_entidade, 
    gestor_contrato, 
    num_certidao
)
SELECT DISTINCT 
    isn_parte_destino, 
    isn_parte_origem, 
    isn_sic, isn_entidade, 
    gestor_contrato, 
    num_certidao
FROM 
    stage.contratos
ON CONFLICT DO NOTHING; -- Use uma coluna apropriada para a cláusula ON CONFLICT

-- Inserção na Dimensão Participação a partir de Convenios
-- (Os campos devem ser ajustados conforme a estrutura exata da sua tabela 'stage.convenios')
INSERT INTO dw.dim_participacao (
    isn_parte_destino, 
    isn_parte_origem, 
    isn_sic, 
    isn_entidade, 
    gestor_contrato, 
    num_certidao
)
SELECT DISTINCT 
    isn_parte_destino, 
    isn_parte_origem, 
    isn_sic, 
    isn_entidade, 
    gestor_contrato, 
    num_certidao
FROM 
    stage.convenios
ON CONFLICT DO NOTHING; -- Use uma coluna apropriada para a cláusula ON conflict

CREATE TABLE IF NOT EXISTS dw.fato_contratos (
    id_fato_contrato SERIAL PRIMARY KEY,
    valor_contrato NUMERIC,
    valor_can_rstpg NUMERIC,
    valor_original_concedente NUMERIC,
    valor_original_contrapartida NUMERIC,
    valor_atualizado_concedente NUMERIC,
    valor_atualizado_contrapartida NUMERIC,
    calculated_valor_aditivo NUMERIC,
    calculated_valor_ajuste NUMERIC,
    calculated_valor_empenhado NUMERIC,
    calculated_valor_pago NUMERIC,
    data_assinatura DATE,
    data_processamento DATE,
    data_termino DATE,
    data_publicacao_doe DATE,
    data_auditoria DATE,
    data_termino_original DATE,
    data_inicio DATE,
    data_rescisao DATE,
    data_finalizacao_prestacao_contas DATE,
    cod_orgao TEXT,
    descricao_modalidade TEXT,
    descricao_objeto TEXT,
    num_contrato TEXT,
    gestor_contrato TEXT,
    id_entidade BIGINT REFERENCES dw.dim_entidade(id_entidade),
    id_modalidade BIGINT REFERENCES dw.dim_modalidade(id_modalidade),
    id_projeto BIGINT REFERENCES dw.dim_projeto(id_projeto),
    id_contrato BIGINT REFERENCES dw.dim_contrato(id_contrato),
    id_participacao BIGINT REFERENCES dw.dim_participacao(id_participacao)
);


INSERT INTO dw.fato_contratos (
    valor_contrato,
    valor_can_rstpg,
    valor_original_concedente,
    valor_original_contrapartida,
    valor_atualizado_concedente,
    valor_atualizado_contrapartida,
    calculated_valor_aditivo,
    calculated_valor_ajuste,
    calculated_valor_empenhado,
    calculated_valor_pago,
    data_assinatura,
    data_processamento,
    data_termino,
    data_publicacao_doe,
    data_auditoria,
    data_termino_original,
    data_inicio,
    data_rescisao,
    data_finalizacao_prestacao_contas,
    cod_orgao,
    descricao_modalidade,
    descricao_objeto,
    num_contrato,
    gestor_contrato,
    id_entidade,
    id_modalidade,
    id_projeto,
    id_contrato,
    id_participacao
)
SELECT
    valor_contrato,
    valor_can_rstpg,
    valor_original_concedente,
    valor_original_contrapartida,
    valor_atualizado_concedente,
    valor_atualizado_contrapartida,
    calculated_valor_aditivo,
    calculated_valor_ajuste,
    calculated_valor_empenhado,
    calculated_valor_pago,
    data_assinatura,
    data_processamento,
    data_termino,
    data_publicacao_doe,
    data_auditoria,
    data_termino_original,
    data_inicio,
    data_rescisao,
    data_finalizacao_prestacao_contas,
    cod_orgao,
    decricao_modalidade,
    descricao_objeto,
    num_contrato,
    gestor_contrato,
    (SELECT id_entidade FROM dw.dim_entidade WHERE cod_orgao = stage.contratos.cod_orgao LIMIT 1),
    (SELECT id_modalidade FROM dw.dim_modalidade WHERE decricao_modalidade = stage.contratos.decricao_modalidade LIMIT 1),
    (SELECT id_projeto FROM dw.dim_projeto WHERE descricao_objeto = stage.contratos.descricao_objeto LIMIT 1),
    (SELECT id_contrato FROM dw.dim_contrato WHERE num_contrato = stage.contratos.num_contrato LIMIT 1),
    (SELECT id_participacao FROM dw.dim_participacao WHERE gestor_contrato = stage.contratos.gestor_contrato LIMIT 1)
from
	stage.contratos;


CREATE TABLE IF NOT EXISTS dw.fato_convenios (
    id_fato_contrato SERIAL PRIMARY KEY,
    valor_contrato NUMERIC,
    valor_can_rstpg NUMERIC,
    valor_original_concedente NUMERIC,
    valor_original_contrapartida NUMERIC,
    valor_atualizado_concedente NUMERIC,
    valor_atualizado_contrapartida NUMERIC,
    calculated_valor_aditivo NUMERIC,
    calculated_valor_ajuste NUMERIC,
    calculated_valor_empenhado NUMERIC,
    calculated_valor_pago NUMERIC,
    data_assinatura DATE,
    data_processamento DATE,
    data_termino DATE,
    data_publicacao_doe DATE,
    data_auditoria DATE,
    data_termino_original DATE,
    data_inicio DATE,
    data_rescisao DATE,
    data_finalizacao_prestacao_contas DATE,
    cod_orgao TEXT,
    descricao_modalidade TEXT,
    descricao_objeto TEXT,
    num_contrato TEXT,
    gestor_contrato TEXT,
    id_entidade BIGINT REFERENCES dw.dim_entidade(id_entidade),
    id_modalidade BIGINT REFERENCES dw.dim_modalidade(id_modalidade),
    id_projeto BIGINT REFERENCES dw.dim_projeto(id_projeto),
    id_contrato BIGINT REFERENCES dw.dim_contrato(id_contrato),
    id_participacao BIGINT REFERENCES dw.dim_participacao(id_participacao)
);


INSERT INTO dw.fato_convenios (
    valor_contrato,
    valor_can_rstpg,
    valor_original_concedente,
    valor_original_contrapartida,
    valor_atualizado_concedente,
    valor_atualizado_contrapartida,
    calculated_valor_aditivo,
    calculated_valor_ajuste,
    calculated_valor_empenhado,
    calculated_valor_pago,
    data_assinatura,
    data_processamento,
    data_termino,
    data_publicacao_doe,
    data_auditoria,
    data_termino_original,
    data_inicio,
    data_rescisao,
    data_finalizacao_prestacao_contas,
    cod_orgao,
    descricao_modalidade,
    descricao_objeto,
    num_contrato,
    gestor_contrato,
    id_entidade,
    id_modalidade,
    id_projeto,
    id_contrato,
    id_participacao
)
SELECT
    valor_contrato,
    valor_can_rstpg,
    valor_original_concedente,
    valor_original_contrapartida,
    valor_atualizado_concedente,
    valor_atualizado_contrapartida,
    calculated_valor_aditivo,
    calculated_valor_ajuste,
    calculated_valor_empenhado,
    calculated_valor_pago,
    data_assinatura,
    data_processamento,
    data_termino,
    data_publicacao_doe,
    data_auditoria,
    data_termino_original,
    data_inicio,
    data_rescisao,
    data_finalizacao_prestacao_contas,
    cod_orgao,
    decricao_modalidade,
    descricao_objeto,
    num_contrato,
    gestor_contrato,
    (SELECT id_entidade FROM dw.dim_entidade WHERE cod_orgao = stage.convenios.cod_orgao LIMIT 1),
    (SELECT id_modalidade FROM dw.dim_modalidade WHERE decricao_modalidade = stage.convenios.decricao_modalidade LIMIT 1),
    (SELECT id_projeto FROM dw.dim_projeto WHERE descricao_objeto = stage.convenios.descricao_objeto LIMIT 1),
    (SELECT id_contrato FROM dw.dim_contrato WHERE num_contrato = stage.convenios.num_contrato LIMIT 1),
    (SELECT id_participacao FROM dw.dim_participacao WHERE gestor_contrato = stage.convenios.gestor_contrato LIMIT 1)
from
	stage.convenios;

-- Para fato_contratos
ALTER TABLE dw.fato_contratos
    DROP COLUMN IF EXISTS cod_orgao,
    DROP COLUMN IF EXISTS descricao_modalidade,
    DROP COLUMN IF EXISTS descricao_objeto,
    DROP COLUMN IF EXISTS num_contrato,
    DROP COLUMN IF EXISTS gestor_contrato;

-- Para fato_convenios
ALTER TABLE dw.fato_convenios
    DROP COLUMN IF EXISTS cod_orgao,
    DROP COLUMN IF EXISTS descricao_modalidade,
    DROP COLUMN IF EXISTS descricao_objeto,
    DROP COLUMN IF EXISTS num_contrato,
    DROP COLUMN IF EXISTS gestor_contrato;
