![logo](logo_gov_ce.png)

## Dicionário de Dados: Tabela `convenios`

| Coluna                                | Tipo de Dados | Descrição                                                                               |
|---------------------------------------|---------------|-----------------------------------------------------------------------------------------|
| `id`                                  | Integer       | Identificador único do registro de convênio.                                            |
| `cod_concedente`                      | Text          | Código identificador do órgão concedente do convênio.                                   |
| `cod_financiador`                     | Text          | Código identificador do financiador do convênio.                                        |
| `cod_gestora`                         | Text          | Código identificador da entidade gestora do convênio.                                   |
| `cod_orgao`                           | Text          | Código identificador do órgão governamental associado ao convênio.                      |
| `cod_secretaria`                      | Text          | Código identificador da secretaria responsável pelo convênio.                           |
| `decricao_modalidade`                 | Text          | Descrição da modalidade de licitação ou seleção para o convênio.                        |
| `descricao_objeto`                    | Text          | Descrição detalhada do objeto ou finalidade do convênio.                                |
| `descricao_tipo`                      | Text          | Descrição do tipo de convênio (exemplo: cooperação técnica, financeira, etc.).          |
| `descricao_url`                       | Text          | URL para a descrição ou documento detalhado do convênio.                                |
| `data_assinatura`                     | Date          | Data em que o convênio foi assinado.                                                    |
| `data_processamento`                  | Date          | Data de processamento ou registro do convênio no sistema.                               |
| `data_termino`                        | Date          | Data prevista para a conclusão ou término do convênio.                                  |
| `flg_tipo`                            | Integer       | Indicador numérico para o tipo de convênio ou status (exemplo: ativo, concluído).       |
| `isn_parte_destino`                   | Integer       | Identificador da parte ou entidade destinatária do convênio.                            |
| `isn_sic`                             | Integer       | Identificador do Sistema de Informação ao Cidadão associado ao convênio.                |
| `num_spu`                             | Text          | Número de identificação no Sistema de Patrimônio da União, se aplicável.                |
| `valor_contrato`                      | Numeric       | Valor financeiro do convênio.                                                           |
| `isn_modalidade`                      | Integer       | Identificador numérico da modalidade do convênio.                                       |
| `isn_entidade`                        | Integer       | Identificador da entidade envolvida no convênio.                                        |
| `tipo_objeto`                         | Text          | Tipo de objeto do convênio (exemplo: bem, serviço).                                     |
| `num_spu_licitacao`                   | Text          | Número da licitação associada ao convênio no Sistema de Patrimônio da União.            |
| `descricao_justificativa`             | Text          | Justificativa para a celebração ou para o procedimento de seleção do convênio.          |
| `valor_can_rstpg`                     | Numeric       | Valor cancelado ou restituição prevista no convênio.                                    |
| `data_publicacao_portal`              | Date          | Data de publicação do convênio no portal de transparência ou diário oficial.            |
| `descricao_url_pltrb`                 | Text          | URL para o plano de trabalho associado ao convênio.                                     |
| `descricao_url_ddisp`                 | Text          | URL para o detalhamento da despesa do convênio.                                         |
| `descricao_url_inexg`                 | Text          | URL para o detalhamento da inexigibilidade de licitação, se aplicável.                  |
| `cod_plano_trabalho`                  | Text          | Código do plano de trabalho vinculado ao convênio.                                      |
| `num_certidao`                        | Text          | Número da certidão ou documento associado ao convênio.                                  |
| `descriaco_edital`                    | Text          | Descrição do edital vinculado ao convênio.                                              |
| `cpf_cnpj_financiador`                | Text          | CPF ou CNPJ do financiador do convênio.                                                 |
| `num_contrato`                        | Text          | Número do convênio.                                                                     |
| `valor_original_concedente`           | Numeric       | Valor original a ser pago pelo concedente.                                               |
| `valor_original_contrapartida`        | Numeric       | Valor original da contrapartida financeira.                                             |
| `valor_atualizado_concedente`         | Numeric       | Valor atualizado a ser pago pelo concedente.                                            |
| `valor_atualizado_contrapartida`      | Numeric       | Valor atualizado da contrapartida financeira.                                           |
| `created_at`                          | Timestamp     | Data e hora de criação do registro do convênio.                                         |
| `updated_at`                          | Timestamp     | Data e hora da última atualização do registro do convênio.                              |
| `plain_num_contrato`                  | Text          | Número do convênio em formato simplificado, sem dígitos ou caracteres especiais.        |
| `calculated_valor_aditivo`            | Numeric       | Valor calculado de aditivos ao convênio.                                                |
| `calculated_valor_ajuste`             | Numeric       | Valor calculado de ajustes ao convênio.                                                 |
| `calculated_valor_empenhado`          | Numeric       | Valor calculado do empenho no convênio.                                                 |
| `calculated_valor_pago`               | Numeric       | Valor calculado do que foi efetivamente pago no convênio.                               |
| `contract_type`                       | Text          | Tipo de convênio, categorizado segundo critérios internos.                              |
| `infringement_status`                 | Integer       | Estado de infração ou cumprimento do convênio.                                          |
| `cod_financiador_including_zeroes`    | Text          | Código do financiador incluindo zeros à esquerda para formatação.                       |
| `accountability_status`               | Text          | Status de prestação de contas do convênio.                                              |
| `plain_cpf_cnpj_financiador`          | Text          | CPF ou CNPJ do financiador em formato simplificado.                                     |
| `descricao_situacao`                  | Text          | Descrição da situação atual do convênio.                                                |
| `data_publicacao_doe`                 | Date          | Data de publicação do convênio no Diário Oficial do Estado.                             |
| `descricao_nome_credor`               | Text          | Nome do credor como descrito no convênio.                                               |
| `isn_parte_origem`                    | Integer       | Identificador da parte de origem do convênio.                                           |
| `data_auditoria`                      | Date          | Data da última auditoria feita no convênio.                                             |
| `data_termino_original`               | Date          | Data originalmente prevista para término do convênio.                                   |
| `data_inicio`                         | Date          | Data de início do convênio.                                                             |
| `data_rescisao`                       | Date          | Data de rescisão do convênio, se aplicável.                                             |
| `confidential`                        | Boolean       | Indicador de se o convênio é confidencial.                                              |
| `gestor_contrato`                     | Text          | Nome ou código do gestor do convênio.                                                   |
| `data_finalizacao_prestacao_contas`   | Date          | Data da finalização da prestação de contas do convênio.                                 |

                             
### Observação sobre a Chave Única

A chave única da tabela `convenios` é composta pelas seguintes colunas:

- `cpf_cnpj_financiador`: CPF ou CNPJ do financiador do convênio.
- `num_contrato`: Número do convênio.
- `data_assinatura`: Data de assinatura do convênio.

Essa combinação de colunas forma uma chave única que identifica de forma exclusiva cada registro na tabela `convenios`. O `cpf_cnpj_financiador` é o CPF ou CNPJ do financiador do convênio. O `num_contrato` é o número do convênio atribuído a ele. A `data_assinatura` é a data em que o convênio foi assinado. Juntas, essas três colunas garantem a unicidade dos registros na tabela.
