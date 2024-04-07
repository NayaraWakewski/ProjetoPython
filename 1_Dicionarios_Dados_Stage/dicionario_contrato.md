![logo](logo_gov_ce.png)



## Dicionário de Dados: Tabela `contratos`

| Coluna                                | Tipo de Dados | Descrição                                                                               |
|---------------------------------------|---------------|-----------------------------------------------------------------------------------------|
| `id`                                  | Integer       | Identificador único do registro de contrato.                                            |
| `cod_concedente`                      | Text          | Código identificador do órgão concedente do contrato.                                   |
| `cod_financiador`                     | Text          | Código identificador do financiador do contrato.                                        |
| `cod_gestora`                         | Text          | Código identificador da entidade gestora do contrato.                                   |
| `cod_orgao`                           | Text          | Código identificador do órgão governamental associado ao contrato.                      |
| `cod_secretaria`                      | Text          | Código identificador da secretaria responsável pelo contrato.                           |
| `decricao_modalidade`                 | Text          | Descrição da modalidade de licitação ou seleção para o contrato.                        |
| `descricao_objeto`                    | Text          | Descrição detalhada do objeto ou finalidade do contrato.                                |
| `descricao_tipo`                      | Text          | Descrição do tipo de contrato (exemplo: obra, serviço, compra, etc.).                   |
| `descricao_url`                       | Text          | URL para a descrição ou documento detalhado do contrato.                                |
| `data_assinatura`                     | Date          | Data em que o contrato foi assinado.                                                    |
| `data_processamento`                  | Date          | Data de processamento ou registro do contrato no sistema.                               |
| `data_termino`                        | Date          | Data prevista para a conclusão ou término do contrato.                                  |
| `flg_tipo`                            | Integer       | Indicador numérico para o tipo de contrato ou status (exemplo: ativo, concluído).       |
| `isn_parte_destino`                   | Integer       | Identificador da parte ou entidade destinatária do contrato.                            |
| `isn_sic`                             | Integer       | Identificador do Sistema de Informação ao Cidadão associado ao contrato.                |
| `num_spu`                             | Text          | Número de identificação no Sistema de Patrimônio da União, se aplicável.                |
| `valor_contrato`                      | Numeric       | Valor financeiro do contrato.                                                           |
| `isn_modalidade`                      | Integer       | Identificador numérico da modalidade do contrato.                                       |
| `isn_entidade`                        | Integer       | Identificador da entidade envolvida no contrato.                                        |
| `tipo_objeto`                         | Text          | Tipo de objeto do contrato (exemplo: bem, serviço).                                     |
| `num_spu_licitacao`                   | Text          | Número da licitação associada ao contrato no Sistema de Patrimônio da União.            |
| `descricao_justificativa`             | Text          | Justificativa para a escolha do fornecedor ou para o procedimento de contratação.        |
| `valor_can_rstpg`                     | Numeric       | Valor cancelado ou restituição prevista no contrato.                                    |
| `data_publicacao_portal`              | Date          | Data de publicação do contrato no portal de transparência ou diário oficial.            |
| `descricao_url_pltrb`                 | Text          | URL para o plano de trabalho associado ao contrato.                                     |
| `descricao_url_ddisp`                 | Text          | URL para o detalhamento da despesa do contrato.                                         |
| `descricao_url_inexg`                 | Text          | URL para o detalhamento da inexigibilidade de licitação, se aplicável.                  |
| `cod_plano_trabalho`                  | Text          | Código do plano de trabalho vinculado ao contrato.                                      |
| `num_certidao`                        | Text          | Número da certidão ou documento associado ao contrato.                                  |
| `descriaco_edital`                    | Text          | Descrição do edital vinculado ao contrato.                                              |
| `cpf_cnpj_financiador`                | Text          | CPF ou CNPJ do financiador do contrato.                                                 |
| `num_contrato`                        | Text          | Número do contrato.                                                                     |
| `valor_original_concedente`           | Numeric       | Valor original a ser pago pelo concedente.                                               |
| `valor_original_contrapartida`        | Numeric       | Valor original da contrapartida financeira.                                             |
| `valor_atualizado_concedente`         | Numeric       | Valor atualizado a ser pago pelo concedente.                                            |
| `valor_atualizado_contrapartida`      | Numeric       | Valor atualizado da contrapartida financeira.                                           |
| `created_at`                          | Timestamp     | Data e hora de criação do registro do contrato.                                         |
| `updated_at`                          | Timestamp     | Data e hora da última atualização do registro do contrato.                              |
| `plain_num_contrato`                  | Text          | Número do contrato em formato simplificado, sem dígitos ou caracteres especiais.        |
| `calculated_valor_aditivo`            | Numeric       | Valor calculado de aditivos ao contrato.                                                |
| `calculated_valor_ajuste`             | Numeric       | Valor calculado de ajustes ao contrato.                                                 |
| `calculated_valor_empenhado`          | Numeric       | Valor calculado do empenho no contrato.                                                 |
| `calculated_valor_pago`               | Numeric       | Valor calculado do que foi efetivamente pago no contrato.                               |
| `contract_type`                       | Text          | Tipo de contrato, categorizado segundo critérios internos.                              |
| `infringement_status`                 | Integer       | Estado de infração ou cumprimento do contrato.                                          |
| `cod_financiador_including_zeroes`    | Text          | Código do financiador incluindo zeros à esquerda para formatação.                       |
| `accountability_status`               | Text          | Status de prestação de contas do contrato.                                              |
| `plain_cpf_cnpj_financiador`          | Text          | CPF ou CNPJ do financiador em formato simplificado.                                     |
| `descricao_situacao`                  | Text          | Descrição da situação atual do contrato.                                                |
| `data_publicacao_doe`                 | Date          | Data de publicação do contrato no Diário Oficial do Estado.                             |
| `descricao_nome_credor`               | Text          | Nome do credor como descrito no contrato.                                               |
| `isn_parte_origem`                    | Integer       | Identificador da parte de origem do contrato.                                           |
| `data_auditoria`                      | Date          | Data da última auditoria feita no contrato.                                             |
| `data_termino_original`               | Date          | Data originalmente prevista para término do contrato.                                   |
| `data_inicio`                         | Date          | Data de início do contrato.                                                             |
| `data_rescisao`                       | Date          | Data de rescisão do contrato, se aplicável.                                             |
| `confidential`                        | Boolean       | Indicador de se o contrato é confidencial.                                              |
| `gestor_contrato`                     | Text          | Nome ou código do gestor do contrato.                                                   |
| `data_finalizacao_prestacao_contas`   | Date          | Data da finalização da prestação de contas do contrato.                                 |
                           

### Observação sobre a Chave Única

A chave única da tabela `contratos` é composta pelas seguintes colunas:

- `cpf_cnpj_financiador`: CPF ou CNPJ do financiador do contrato.
- `num_contrato`: Número do contrato.
- `data_assinatura`: Data de assinatura do contrato.

Essa combinação de colunas forma uma chave única que identifica de forma exclusiva cada registro na tabela `contratos`. O `cpf_cnpj_financiador` é o CPF ou CNPJ do financiador do contrato. O `num_contrato` é o número do contrato atribuído a ele. A `data_assinatura` é a data em que o contrato foi assinado. Juntas, essas três colunas garantem a unicidade dos registros na tabela.

