![logo](logo_gov_ce.png)

# Estrutura do Data Warehouse para Contratos e Convênios

Este Data Warehouse é projetado para armazenar e facilitar a análise de contratos e convênios do Governo do Estado do Ceará. A estrutura inclui tabelas Fato separadas para detalhar as informações quantitativas e as datas relevantes dos contratos e convênios, juntamente com várias dimensões que descrevem os atributos qualitativos.

## Tabelas Fato

### Tabela Fato: `fato_contratos`

Contém informações quantitativas e datas específicas de contratos.

| Coluna                                   | Tipo de Dado | Descrição                                       |
|------------------------------------------|--------------|-------------------------------------------------|
| `id_fato_contratos`                      | INTERGER     | Identificador único do registro de contrato.    |
| `valor_contrato`                         | NUMERIC      | Valor total do contrato.                        |
| `valor_can_rstpg`                        | NUMERIC      | Valor cancelado/restante.                       |
| `valor_original_concedente`              | NUMERIC      | Valor original concedido pelo concedente.       |
| `valor_original_contrapartida`           | NUMERIC      | Valor original de contrapartida.                |
| `valor_atualizado_concedente`            | NUMERIC      | Valor atualizado concedido.                     |
| `valor_atualizado_contrapartida`         | NUMERIC      | Valor atualizado de contrapartida.              |
| `calculated_valor_aditivo`               | NUMERIC      | Valor total de aditivos.                        |
| `calculated_valor_ajuste`                | NUMERIC      | Valor total de ajustes.                         |
| `calculated_valor_empenhado`             | NUMERIC      | Valor total empenhado.                          |
| `calculated_valor_pago`                  | NUMERIC      | Valor total pago.                               |
| `data_assinatura`                        | DATE         | Data de assinatura do contrato.                 |
| `data_processamento`                     | DATE         | Data de processamento do contrato.              |
| `data_termino`                           | DATE         | Data de término do contrato.                    |
| `data_publicacao_doe`                    | DATE         | Data de publicação no Diário Oficial.           |
| `data_auditoria`                         | DATE         | Data da auditoria.                              |
| `data_termino_original`                  | DATE         | Data original de término.                       |
| `data_inicio`                            | DATE         | Data de início do contrato.                     |
| `data_rescisao`                          | DATE         | Data de rescisão do contrato.                   |
| `data_finalizacao_prestacao_contas`      | DATE         | Data de finalização da prestação de contas.     |
| `id_entidade`                            | INTERGER     | FK para `dim_entidade`.                         |
| `id_modalidade`                          | INTERGER     | FK para `dim_modalidade`.                       |
| `id_projeto`                             | INTERGER     | FK para `dim_projeto`.                          |
| `id_contrato`                            | INTERGER     | FK para `dim_contrato`.                         |
| `id_participacao`                        | INTERGER     | FK para `dim_participacao`.                     |

### Tabela Fato: `fato_convenios`

Contém informações quantitativas e datas específicas de convênios.

| Coluna                                   | Tipo de Dado | Descrição                                       |
|------------------------------------------|--------------|-------------------------------------------------|
| `id_fato_convenios`                      | INTERGER     | Identificador único do registro de convenios.    |
| `valor_contrato`                         | NUMERIC      | Valor total do contrato.                        |
| `valor_can_rstpg`                        | NUMERIC      | Valor cancelado/restante.                       |
| `valor_original_concedente`              | NUMERIC      | Valor original concedido pelo concedente.       |
| `valor_original_contrapartida`           | NUMERIC      | Valor original de contrapartida.                |
| `valor_atualizado_concedente`            | NUMERIC      | Valor atualizado concedido.                     |
| `valor_atualizado_contrapartida`         | NUMERIC      | Valor atualizado de contrapartida.              |
| `calculated_valor_aditivo`               | NUMERIC      | Valor total de aditivos.                        |
| `calculated_valor_ajuste`                | NUMERIC      | Valor total de ajustes.                         |
| `calculated_valor_empenhado`             | NUMERIC      | Valor total empenhado.                          |
| `calculated_valor_pago`                  | NUMERIC      | Valor total pago.                               |
| `data_assinatura`                        | DATE         | Data de assinatura do contrato.                 |
| `data_processamento`                     | DATE         | Data de processamento do contrato.              |
| `data_termino`                           | DATE         | Data de término do contrato.                    |
| `data_publicacao_doe`                    | DATE         | Data de publicação no Diário Oficial.           |
| `data_auditoria`                         | DATE         | Data da auditoria.                              |
| `data_termino_original`                  | DATE         | Data original de término.                       |
| `data_inicio`                            | DATE         | Data de início do contrato.                     |
| `data_rescisao`                          | DATE         | Data de rescisão do contrato.                   |
| `data_finalizacao_prestacao_contas`      | DATE         | Data de finalização da prestação de contas.     |
| `id_entidade`                            | INTERGER     | FK para `dim_entidade`.                         |
| `id_modalidade`                          | INTERGER     | FK para `dim_modalidade`.                       |
| `id_projeto`                             | INTERGER     | FK para `dim_projeto`.                          |
| `id_contrato`                            | INTERGER     | FK para `dim_contrato`.                         |
| `id_participacao`                        | INTERGER     | FK para `dim_participacao`.                     |

## Dimensões Compartilhadas

### Dimensão: `dim_entidade`

Agrupa informações relacionadas a entidades envolvidas nos contratos e convênios.

| Coluna                      | Tipo de Dado | Descrição                             |
|-----------------------------|--------------|---------------------------------------|
| `id_entidade`               | INTERGE      | Identificador único da entidade.      |
| `cod_concedente`            | TEXT         | Código do concedente.                 |
| `cod_financiador`           | TEXT         | Código do financiador.                |
| `cod_gestora`               | TEXT         | Código da gestora.                    |
| `cod_orgao`                 | TEXT         | Código do órgão.                      |
| `cod_secretaria`            | TEXT         | Código da secretaria.                 |
| `cpf_cnpj_financiador`      | TEXT         | CPF ou CNPJ do financiador.           |
| `plain_cpf_cnpj_financiador`| TEXT         | CPF ou CNPJ do financiador sem máscara.|
| `descricao_nome_credor`     | TEXT         | Nome do credor.                       |

### Dimensão: `dim_modalidade`

Captura detalhes sobre a modalidade do contrato ou convênio.

| Coluna                    | Tipo de Dado | Descrição                           |
|---------------------------|--------------|-------------------------------------|
| `id_modalidade`           | INTERGER     | Identificador único da modalidade.  |
| `decricao_modalidade`     | TEXT         | Descrição da modalidade.            |
| `descricao_tipo`          | TEXT         | Descrição do tipo.                  |
| `flg_tipo`                | INTERGER     | Flag do tipo.                       |
| `isn_modalidade`          | INTEGER      | Identificador do sistema para a modalidade.|
| `descricao_justificativa` | TEXT         | Justificativa da modalidade.        |

### Dimensão: `dim_projeto`

Contém informações sobre o projeto ou objeto do contrato/convenio.

| Coluna                | Tipo de Dado | Descrição                         |
|-----------------------|--------------|-----------------------------------|
| `id_projeto`          | INTERGER     | Identificador único do projeto.   |
| `descricao_objeto`    | TEXT         | Descrição do objeto do projeto.   |
| `tipo_objeto`         | TEXT         | Tipo do objeto do projeto.        |
| `cod_plano_trabalho`  | TEXT         | Código do plano de trabalho.      |
| `num_spu`             | TEXT         | Número do SPU.                    |
| `num_spu_licitacao`   | TEXT         | Número do SPU da licitação.       |
| `descriaco_edital`    | TEXT         | Descrição do edital.              |

### Dimensão: `dim_contrato`

Detalhes específicos do contrato, incluindo identificadores e status.

| Coluna                          | Tipo de Dado | Descrição                             |
|---------------------------------|--------------|---------------------------------------|
| `id_contrato`                   | INTERGER     | Identificador único do contrato.      |
| `num_contrato`                  | TEXT         | Número do contrato.                   |
| `plain_num_contrato`            | TEXT         | Número do contrato sem máscara.       |
| `contract_type`                 | TEXT         | Tipo de contrato.                     |
| `infringement_status`           | INTERGER     | Status de infração.                   |
| `cod_financiador_including_zeroes` | TEXT      | Código do financiador com zeros.      |
| `accountability_status`         | TEXT         | Status de prestação de contas.        |
| `descricao_situacao`            | TEXT         | Descrição da situação do contrato.    |

### Dimensão: `dim_participacao`

Informações sobre a participação de entidades e origens nos contratos e convênios.

| Coluna                | Tipo de Dado | Descrição                             |
|-----------------------|--------------|---------------------------------------|
| `id_participacao`     | INTERGER     | Identificador único da participação.  |
| `isn_parte_destino`   | INTERGER     | Identificador do sistema para a parte destino.|
| `isn_parte_origem`    | TEXT         | Identificador do sistema para a parte origem. |
| `isn_sic`             | INTERGER     | Identificador do sistema interno de controle. |
| `isn_entidade`        | INTEGER      | Identificador do sistema para a entidade.    |
| `gestor_contrato`     | TEXT         | Gestor do contrato.                   |
| `num_certidao`        | TEXT         | Número da certidão.                   |

