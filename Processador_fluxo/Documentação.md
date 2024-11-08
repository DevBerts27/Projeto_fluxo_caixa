### Documentação do Código

Este código foi desenvolvido para processar e manipular dados financeiros relacionados a fluxos de caixa, saldo bancário e investimentos a partir de arquivos Excel. O código está estruturado em classes, cada uma responsável por uma parte específica do processamento. A seguir, descreverei cada classe, função e sua relação dentro do contexto do código.

---

#### Classe `ProcessadorFluxoArquivosCaminhoDatas`

**Descrição:** Esta classe é responsável por listar e extrair datas dos arquivos relacionados ao fluxo de caixa. A ideia é facilitar o processamento posterior dos arquivos, separando-os por datas extraídas dos seus nomes.

- **Método `__init__`:** 
    - Inicializa a classe com o caminho onde os arquivos estão localizados.
    - Chama os métodos privados `_listar_arquivos_fluxo` para listar os arquivos e `_extrair_datas_dos_arquivos` para extrair as datas contidas nos nomes dos arquivos.

- **Método `_listar_arquivos_fluxo`:**
    - Lista todos os arquivos no diretório especificado que contêm "Fluxo" no nome. 
    - Retorna uma lista de nomes de arquivos.

- **Método `_extrair_datas_dos_arquivos`:**
    - Extrai datas no formato "MM-AAAA" dos nomes dos arquivos listados.
    - Utiliza expressões regulares para buscar as datas.
    - Retorna uma lista de datas.

---

#### Classe `TabelaBancoCompromissoLancamentos`

**Descrição:** Manipula os dados do fluxo, limpando e formatando para obter os lançamentos por banco e tipo de compromisso.

- **Método `_limpa_fluxo`:**
    - Carrega os dados de uma folha específica de um arquivo Excel e aplica filtros para selecionar apenas as linhas relevantes.
    - Filtra as colunas indesejadas e padroniza os nomes das colunas.
    - Adiciona a data associada àquela folha de dados ao DataFrame resultante.

- **Método `processar_arquivos`:**
    - Processa uma lista de arquivos e datas, carregando as folhas de cada arquivo que contêm as datas especificadas.
    - Utiliza o método `_limpa_fluxo` para limpar e formatar os dados.
    - Retorna uma lista de DataFrames.

- **Método `concatenar_dfs`:**
    - Concatena uma lista de DataFrames ao longo do eixo 0 (linhas).
    - Retorna um DataFrame concatenado.

- **Método `melt_dataframe`:**
    - Reorganiza o DataFrame (melt) para transformar colunas de banco em valores de uma nova coluna.
    - Utiliza as colunas fixas "tipos de compromisso" e "data" e reorganiza as demais colunas.

- **Método `classifica_tipo`:**
    - Classifica o tipo de compromisso com base nos primeiros três dígitos do código do compromisso.
    - Verifica se o código pertence a entradas ou saídas, utilizando listas definidas externamente.
    - Retorna "Entrada", "Saída" ou "Desconhecido".

- **Método `adicionar_coluna_tipo`:**
    - Aplica a classificação do tipo de compromisso aos dados reorganizados (melted DataFrame).
    - Retorna o DataFrame atualizado.

- **Método `processar_dados`:**
    - Concatena, reorganiza e classifica os dados em um fluxo completo de processamento.
    - Retorna o DataFrame final, pronto para análise ou uso posterior.

---

#### Classe `TabelaSaldoInicialFinal`

**Descrição:** Processa e limpa os dados relacionados ao saldo inicial e final de diferentes bancos, extraindo essas informações dos arquivos de fluxo de caixa.

- **Método `_limpa_fluxo_corrigido_v2`:**
    - Filtra as linhas de uma folha Excel para incluir apenas as relacionadas a "SALDO FINAL" ou "SALDO INICIAL".
    - Remove colunas indesejadas e renomeia as colunas para padronização.
    - Adiciona a data ao DataFrame e transforma colunas selecionadas em linhas (melt).
    - Filtra e limpa os dados, garantindo que os valores numéricos estejam formatados e corretos.

- **Método `processar_arquivos`:**
    - Similar ao método na classe anterior, processa os arquivos e datas, mas foca em dados de saldo.
    - Retorna uma lista de DataFrames.

---

#### Classe `TabelaInvestimentos`

**Descrição:** Manipula os dados relacionados a investimentos, limpando e formatando-os para análise.

- **Método `_limpa_fluxo_investimentos`:**
    - Carrega os dados da folha "Investimentos" de um arquivo Excel.
    - Remove colunas irrelevantes e padroniza os nomes das colunas.
    - Converte e limpa os dados, substituindo valores nulos e transformando tipos de dados conforme necessário.

- **Método `processar_arquivos`:**
    - Processa os arquivos de fluxo de caixa, mas foca nas folhas que contêm informações de investimentos.
    - Retorna uma lista de DataFrames.

---

#### Função `main`

**Descrição:** Esta função coordena o uso das classes para processar todos os arquivos disponíveis no caminho especificado.

- **Etapas do `main`:**
    1. Inicializa a classe `ProcessadorFluxoArquivosCaminhoDatas` para listar os arquivos e extrair datas.
    2. Processa os arquivos utilizando `TabelaBancoCompromissoLancamentos`, `TabelaSaldoInicialFinal`, e `TabelaInvestimentos`.
    3. Exibe os resultados processados.

---

Este código permite a manipulação e análise eficiente dos dados financeiros extraídos de diferentes tipos de folhas Excel. Cada classe está focada em uma parte específica do processo, o que facilita a manutenção e a expansão do código para incluir novos tipos de processamento ou novas fontes de dados.