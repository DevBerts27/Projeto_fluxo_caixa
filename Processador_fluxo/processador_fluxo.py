import pandas as pd
import os
import re
from pyfiglet import figlet_format
from pathlib import Path
from unidecode import unidecode

import listas
import conn_db as db
import passwd
import dim

import enviar_painel

# Completo
class ProcessadorFluxoArquivosCaminhoDatas:
    """Esta Classe processa os arquivos do Fluxo de caixa."""

    def __init__(self, caminho: str):
        self.caminho = caminho
        self.arquivos = self._listar_arquivos_fluxo()
        self.datas = self._extrair_datas_dos_arquivos()

    def _listar_arquivos_fluxo(self) -> list:
        arquivos = os.listdir(self.caminho)
        arquivos_fluxo = []
        filtro = r"^Fluxo de Caixa Diário \d{2}-\d{4}( atualizado)?\.xlsx$"
        # filtro = "Fluxo de Caixa Diário 11-2024.xlsx"
        for arquivo in arquivos:
            if re.match(filtro, arquivo):
                arquivos_fluxo.append(arquivo)
        print(f"Arquivos encontrados: {arquivos_fluxo}")
        return arquivos_fluxo

    def _extrair_datas_dos_arquivos(self) -> list:
        return [
            re.search(r"\d{2}-\d{4}", arquivo).group()
            for arquivo in self.arquivos
            if re.search(r"\d{2}-\d{4}", arquivo)
        ]


# Completo
class TabelaBancoCompromissoLancamentos:
    """Esta classe manipula os dados do fluxo, limpando e formatando para obter os lançamentos por Banco e Compromisso."""

    def _limpa_fluxo(self, arquivo: str, data: str) -> pd.DataFrame:
        df_fluxo = pd.read_excel(arquivo, sheet_name=f"{data}")

        padrao = r"\b\d{1,3} - .+"

        # Aplicando o filtro diretamente na primeira coluna
        primeira_coluna = df_fluxo.columns[0]
        df_filtrado = df_fluxo[
            df_fluxo[primeira_coluna].str.contains(padrao, na=False)
        ].copy()

        df_filtrado.columns = df_filtrado.columns.str.lower()

        # Especificando o formato da data corretamente
        df_filtrado["data"] = pd.to_datetime(f"{data}", format="%d-%m-%Y")

        # Garantindo que os nomes das colunas são strings antes de aplicar o filtro
        df_filtrado.columns = df_filtrado.columns.astype(str)

        df_filtrado = df_filtrado.loc[
            :, ~df_filtrado.columns.str.contains("^unnamed|total", case=False)
        ]

        coluna = df_filtrado.columns[0]
        df_filtrado = df_filtrado.rename(columns={coluna: "tipos de compromisso"})

        # Extraindo os primeiros caracteres dos valores da coluna 'tipos de compromisso'
        # df_filtrado['id compromisso'] = df_filtrado['tipos de compromisso'].str.extract(r'(\d{1,3})')

        # Extraindo os primeiros 3 caracteres de 'tipos de compromisso'
        df_filtrado["tipos de compromisso"] = df_filtrado["tipos de compromisso"].str[
            :3
        ]

        return df_filtrado

    def _concatenar_dfs(self, dfs: list) -> pd.DataFrame:
        if dfs:
            return pd.concat(dfs, axis=0)
        else:
            print("Nenhum DataFrame foi concatenado. Verifique o processo de extração.")
            return pd.DataFrame()

    def _melt_dataframe(self, df_final: pd.DataFrame) -> pd.DataFrame:
        colunas_fixas = ["tipos de compromisso", "data"]
        df_melted = pd.melt(
            df_final,
            id_vars=colunas_fixas,
            value_vars=listas.colunas_bancos,
            var_name="Banco",
            value_name="Valores",
        )

        df_melted["Valores"] = pd.to_numeric(df_melted["Valores"], errors="coerce")

        df_melted["Banco"] = df_melted["Banco"].str.upper()

        return df_melted

    def _classifica_tipo(self, compromisso: str) -> str:
        # Extrai os primeiros três dígitos do código do compromisso
        codigo = re.match(r"^\d{1,3}", compromisso)
        if codigo:
            codigo = codigo.group()
            if codigo in listas.entradas:
                return "Entrada"
            elif codigo in listas.saidas:
                return "Saída"
        return "Desconhecido"

    def _adicionar_coluna_tipo(self, df_melted: pd.DataFrame) -> pd.DataFrame:
        df_melted["Tipo"] = df_melted["tipos de compromisso"].apply(
            self._classifica_tipo
        )

        # Removendo a coluna 'Tipo'
        df_melted.drop(columns=["Tipo"], inplace=True)

        return df_melted

    def processar_arquivos(
        self, caminho: str, arquivos: list, datas: list | None = None
    ) -> list:
        return [
            self._limpa_fluxo(os.path.join(caminho, arquivo), data)
            for arquivo in arquivos
            for data_inicial in [
                pd.to_datetime(
                    re.sub(r"^.*(\d{2}-\d{4}).*$", r"01-\1", arquivo), format="%d-%m-%Y"
                )
            ]
            for data in pd.date_range(
                start=data_inicial,
                end=pd.offsets.MonthEnd().rollforward(data_inicial),
                freq="D",
            ).strftime("%d-%m-%Y")
        ]

        # dfs = []
        # for arquivo in arquivos:
        #     caminho_completo = os.path.join(caminho, arquivo)
        #     xls = pd.ExcelFile(caminho_completo)
        #     sheets = xls.sheet_names
        #     sheets = [sheet for sheet in sheets if any(data in sheet for data in datas)]

        #     print("A", sheets.__len__())

        #     print("A", sheets)

        #     if listas.tem_dia_31(sheets):
        #         if len(sheets) > 30:
        #             if re.match(r"01-\d{2}-2024", sheets[-1]):
        #                 # Nenhuma ação necessária
        #                 pass
        #             else:
        #                 sheets = sheets[:-1]
        #         else:
        #             continue
        #     else:
        #         if len(sheets) > 31:
        #             sheets = sheets[:-1]
        #         else:
        #             continue

        #     print("D", sheets)
        #     print("D", sheets.__len__())
        #     # sheets = sheets[:-1]

        #     if not sheets:
        #         print(f"Aviso: Nenhuma folha correspondente encontrada em {arquivo}")
        #         continue

        #     for sheet in sheets:
        #         print(f"Processando {arquivo} - Folha: {sheet}")
        #         df = self._limpa_fluxo(caminho_completo, sheet)
        #         if not df.empty:
        #             dfs.append(df)
        #         else:
        #             print(
        #                 f"Aviso: DataFrame vazio retornado para {arquivo} - Folha: {sheet}"
        #             )
        # return dfs

    def processar_dados(self, dfs: list) -> pd.DataFrame:
        df_final = self._concatenar_dfs(dfs)
        if not df_final.empty:
            df_melted = self._melt_dataframe(df_final)
            df_final_classificado = self._adicionar_coluna_tipo(df_melted)
            return df_final_classificado
        return pd.DataFrame()


# Completo
class TabelaSaldoInicialFinal:
    """Esta classe processa e limpa os dados relacionados ao saldo inicial e final."""

    def _limpa_fluxo_corrigido_v2(self, arquivo: str, data: str) -> pd.DataFrame:
        # Ler o arquivo Excel com a aba correspondente
        df_fluxo = pd.read_excel(arquivo, sheet_name=f"{data}")

        # Filtrar as linhas para incluir apenas as que contêm "SALDO FINAL" ou "SALDO INICIAL"
        df_filtrado = df_fluxo[
            df_fluxo.iloc[:, 0].isin(["SALDO FINAL", "SALDO INICIAL"])
        ].copy()

        # Remover as colunas indesejadas (caso existam)
        colunas_para_remover = [
            "Unnamed: 22",
            "TOTAL",
            "Column25",
            "Column26",
            "Column27",
            "Column28",
            "Column29",
        ]
        colunas_existentes = [
            col for col in colunas_para_remover if col in df_filtrado.columns
        ]
        df_filtrado.drop(columns=colunas_existentes, inplace=True)

        df_filtrado["SAFRA2"] = df_filtrado["SAFRA"]

        # Renomear as colunas
        df_filtrado.rename(
            columns={
                df_filtrado.columns[0]: "Saldo FINAL/INICIAL",
                df_filtrado.columns[1]: "Data",
            },
            inplace=True,
        )

        df_filtrado.rename(columns={"SAFRA2": "SAFRA"}, inplace=True)

        # Adicionar a coluna de data
        df_filtrado["Data"] = pd.to_datetime(f"{data}", format="%d-%m-%Y")

        return df_filtrado

    def _concatenando_colunas(self, df_filtrado: pd.DataFrame) -> pd.DataFrame:

        # Verificar quais colunas dos bancos estão realmente presentes
        colunas_bancos_existentes = [
            col for col in listas.colunas_bancos_saldos if col in df_filtrado.columns
        ]

        # Verificação de segurança
        if not colunas_bancos_existentes:
            raise ValueError(
                "Nenhuma das colunas de bancos esperadas está presente no DataFrame."
            )

        # Transformar as colunas selecionadas em linhas (melt/unpivot) apenas para as colunas presentes
        df_melted = pd.melt(
            df_filtrado,
            id_vars=["Saldo FINAL/INICIAL", "Data"],
            value_vars=colunas_bancos_existentes,
            var_name="Banco",
            value_name="Valor",
        )

        # Filtrar erros (remover linhas onde os valores são inválidos)

        # df_melted.dropna(subset=["Valor"], inplace=True)

        # Alterar o tipo da coluna "Valor" para número e "Data" para data
        df_melted["Valor"] = pd.to_numeric(df_melted["Valor"], errors="coerce")
        df_melted["Data"] = pd.to_datetime(
            df_melted["Data"], format="%d-%m-%Y", errors="coerce"
        )

        # Deixa o numero como um float e arrenda para 2 numeros depois da vírgula
        df_melted["Valor"] = df_melted["Valor"].astype("float64").round(decimals=2)

        # Renomear a coluna de valor final
        df_melted.rename(columns={"Valor": "Valor Saldo Final/inicial"}, inplace=True)

        return df_melted

    def processar_arquivos(self, caminho: str, arquivos: list, datas: list|None = None) -> list:
        return [
            self._limpa_fluxo_corrigido_v2(os.path.join(caminho, arquivo), data)
            for arquivo in arquivos
            for data_inicial in [
                pd.to_datetime(
                    re.sub(r"^.*(\d{2}-\d{4}).*$", r"01-\1", arquivo), format="%d-%m-%Y"
                )
            ]
            for data in pd.date_range(
                start=data_inicial,
                end=pd.offsets.MonthEnd().rollforward(data_inicial),
                freq="D",
            ).strftime("%d-%m-%Y")
        ]
        
        # dfs = []
        # for arquivo in arquivos:
        #     caminho_completo = os.path.join(caminho, arquivo)
        #     xls = pd.ExcelFile(caminho_completo)
        #     sheets = xls.sheet_names
        #     sheets = [sheet for sheet in sheets if any(data in sheet for data in datas)]

        #     print("A", sheets.__len__())

        #     print("A", sheets)

        #     if listas.tem_dia_31(sheets):
        #         if len(sheets) > 30:
        #             if re.match(r"01-\d{2}-2024", sheets[-1]):
        #                 # Nenhuma ação necessária
        #                 pass
        #             else:
        #                 sheets = sheets[:-1]
        #         else:
        #             continue
        #     else:
        #         if len(sheets) > 31:
        #             sheets = sheets[:-1]
        #         else:
        #             continue

        #     print("D", sheets)
        #     print("D", sheets.__len__())
        #     # sheets = sheets[:-1]

        #     if not sheets:
        #         print(f"Aviso: Nenhuma folha correspondente encontrada em {arquivo}")
        #         continue

        #     for sheet in sheets:
        #         print(f"Processando {arquivo} - Folha: {sheet}")
        #         df = self._limpa_fluxo_corrigido_v2(caminho_completo, sheet)
        #         if not df.empty:
        #             dfs.append(df)
        #         else:
        #             print(
        #                 f"Aviso: DataFrame vazio retornado para {arquivo} - Folha: {sheet}"
        #             )
        # return dfs

    def processar_dados(self, dfs: list) -> pd.DataFrame:
        df_concatenado = pd.concat(dfs, axis=0)
        if df_concatenado.empty:
            print("Nenhum dado para processar.")
            return pd.DataFrame()
        df_final = self._concatenando_colunas(df_concatenado)
        return df_final


# Completa
class TabelaInvestimentos:
    """Esta classe processa os dados relacionados aos investimentos."""

    def _limpa_fluxo_investimentos(self, arquivo: str) -> pd.DataFrame:
        # Carregar apenas a sheet "Investimentos"
        df_fluxo = pd.read_excel(arquivo, sheet_name="Investimentos")

        # Remover colunas que começam com "unnamed" ou contêm "total"
        df_fluxo = df_fluxo.loc[
            :, ~df_fluxo.columns.str.contains("^unnamed|total", case=False)
        ]

        # Converter o nome das colunas para minúsculas
        df_fluxo.columns = df_fluxo.columns.str.lower()

        # Adicionar a coluna "Data" no formato adequado (usando a data do arquivo como referência)
        # df_fluxo["data"] = pd.to_datetime(df_fluxo["data"], errors='coerce')

        # Remove colunas desnecessárias
        colunas_para_remover = [
            "data",
            "dia da semana",
            "coluna",
            "taxa carteira geral",
            "taxa carteira cdb",
            "taxa carteira compr",
            "coeficiente",
            "b3",
            "ipca",
            "juros mensal",
            "100% cdi",
            "rendimento 100% cdi",
            "% saldo bloqueado",
        ]

        colunas_existentes = [
            col for col in colunas_para_remover if col in df_fluxo.columns
        ]

        data_inicial = pd.to_datetime(
            re.sub(r"^.*(\d{2}-\d{4}).*$", r"01-\1", arquivo), format="%d-%m-%Y"
        )

        data_final = pd.offsets.MonthEnd().rollforward(data_inicial)
        
        datas_validas = pd.date_range(start=data_inicial, end=data_final, freq="D")

        print(f"{datas_validas}")

        df_fluxo["data.1"] = pd.to_datetime(df_fluxo["data.1"])

        print(f"{df_fluxo['data.1']}")

        df_fluxo = df_fluxo[df_fluxo["data.1"].isin(datas_validas)]

        print(f"{df_fluxo}")

        df_fluxo.drop(columns=colunas_existentes, inplace=True)

        # Renomea as colunas
        df_fluxo.rename(columns={"data.1": "Data"}, inplace=True)

        # Substituir valores nulos por 0 em colunas específicas
        df_fluxo["rentabilidade"] = df_fluxo["rentabilidade"].fillna(0)
        df_fluxo["saldo bloqueado"] = df_fluxo["saldo bloqueado"].fillna(0)

        return df_fluxo

    def _formata_numeros(self, df: pd.DataFrame) -> pd.DataFrame:

        df.replace("", 0)

        for coluna in listas.colunas_para_formatar_money:
            if coluna in df.columns:
                df[coluna] = pd.to_numeric(df[coluna], errors="coerce").fillna(0)

        # Formatar as colunas no formato com 2 decimais
        for coluna in listas.colunas_para_formatar_money:
            if coluna in df.columns:
                df[coluna] = df[coluna].astype("float64").round(decimals=2)

        # Formata as colunas para porcentagem
        for coluna in listas.colunas_para_formatar_porcentagem:
            if coluna in df.columns:
                df[coluna] = df[coluna].astype("float64")

        return df

    def processar_arquivos(
        self, caminho: str, arquivos: list, investimentos: list
    ) -> list:
        dfs = []
        
        for arquivo in arquivos:
            caminho_completo = os.path.join(caminho, arquivo)
            xls = pd.ExcelFile(caminho_completo)
            sheets = xls.sheet_names
            sheets = [sheet for sheet in sheets if sheet == "Investimentos"]
            
            if not sheets:
                print(f"Aviso: Nenhuma folha correspondente encontrada em {arquivo}")
                continue

            for sheet in sheets:
                print(f"Processando {arquivo} - Folha: {sheet}", end="\r")
                df = self._limpa_fluxo_investimentos(caminho_completo)
                if not df.empty:
                    dfs.append(df)
                else:
                    print(
                        f"Aviso: DataFrame vazio retornado para {arquivo} - Folha: {sheet}"
                    )

        return dfs

    def processar_dados(self, dfs: list) -> pd.DataFrame:
        df_concatenado = pd.concat(dfs, axis=0)
        if df_concatenado.empty:
            print("Nenhum dado para processar.")
            return pd.DataFrame()
        df_final = self._formata_numeros(df_concatenado)
        return df_final


def processar_tabelas():
    # caminho = R"C:\Users\pedro.bertoldo\OneDrive - Balaroti Comércio de Materiais de Construção SA\Documentos Compartilhados - Planejamento Financeiro\_Projetos Caixa\arquivos fluxo de caixa"
    # caminho = R"\\portaarquivos\Financeiro\Pedro\Processador_fluxo\Arquivos_fluxo"
    # caminho = R"\\portaarquivos\Financeiro\Fluxo de Caixa Diário\2024"
    caminho_onedrive = Path(
        os.environ.get("OneDriveCommercial") or os.environ.get("OneDrive")
    )
    pasta_fluxo = R"Shared Documents - Tesouraria\Fluxo de Caixa Diário\2024"
    caminho = f"{caminho_onedrive}"
    processador = ProcessadorFluxoArquivosCaminhoDatas(f"{caminho}/{pasta_fluxo}")

    # Tabela 1

    print("\nProcessando Tabela com TabelaBancoCompromissoLancamentos...\n")
    tabela_1 = TabelaBancoCompromissoLancamentos()
    print("\nProcessando Arquivos...\n")
    dfs_tabela_1 = tabela_1.processar_arquivos(
        processador.caminho, processador.arquivos, processador.datas
    )
    print("\nProcessando Dados...\n")
    tabela_BancoCompromissoLancamentos = tabela_1.processar_dados(dfs_tabela_1)
    tabela_BancoCompromissoLancamentos_formatada = formata_tabelas(
        tabela_BancoCompromissoLancamentos
    )

    print(formata_tabelas(tabela_BancoCompromissoLancamentos))

    # Tabela 2

    print("\nProcessando Tabela com TabelaSaldoInicialFinal...\n")
    tabela_2 = TabelaSaldoInicialFinal()
    print("\nProcessando Arquivos...\n")
    dfs_tabela_2 = tabela_2.processar_arquivos(
        processador.caminho, processador.arquivos, processador.datas
    )
    print("\nProcessando Dados...\n")
    tabela_SaldoInicialFinal = tabela_2.processar_dados(dfs_tabela_2)
    tabela_SaldoInicialFinal_formatada = formata_tabelas(tabela_SaldoInicialFinal)

    print(formata_tabelas(tabela_SaldoInicialFinal))

    # Tabela 3

    print("Processando Arquivos com TabelaInvestimentos...")
    tabela_3 = TabelaInvestimentos()
    print("Processando Arquivos")
    dfs_tabela_3 = tabela_3.processar_arquivos(
        processador.caminho, processador.arquivos, processador.datas
    )
    print("Processando Dados")
    tabela_insvestimentos = tabela_3.processar_dados(dfs_tabela_3)
    tabela_insvestimentos_formatada = formata_tabelas(tabela_insvestimentos)

    print(formata_tabelas(tabela_insvestimentos))

    return (
        tabela_BancoCompromissoLancamentos_formatada,
        tabela_SaldoInicialFinal_formatada,
        tabela_insvestimentos_formatada,
    )


def formata_tabelas(tabela: pd.DataFrame) -> pd.DataFrame:
    """Função que formata as colunas do Dataframe no padrão do banco de dados"""
    print(f"{tabela.columns = }")
    print(f"{tabela.empty = }")
    if tabela.empty:
        return tabela
    tabela.columns = (
        tabela.columns.str.strip()
        .str.lower()
        .map(unidecode)
        .str.replace(r"\W", "_", regex=True)
    )

    return tabela


def salvar_em_postgres(
    lancamentos: pd.DataFrame,
    saldos: pd.DataFrame,
    investimentos: pd.DataFrame,
    dim_contas: pd.DataFrame,
    dim_compromissos: pd.DataFrame,
    dim_data: pd.DataFrame,
    engine,
) -> None:
    lancamentos.to_sql("fluxo_lancamentos", engine, if_exists="replace", index=True)
    saldos.to_sql("fluxo_saldos", engine, if_exists="replace", index=True)
    investimentos.to_sql("fluxo_investimentos", engine, if_exists="replace", index=True)
    # dim_contas.to_sql("fluxo_dim_contas", engine, if_exists="replace", index=True)
    # dim_compromissos.to_sql(
    #     "fluxo_dim_compromissos", engine, if_exists="replace", index=True
    # )
    # dim_data.to_sql("fluxo_dim_datas", engine, if_exists="replace", index=True)


def main():

    print(f"{figlet_format("Cashflow\nProcessor",font='slant')}\nby Pedro")

    host = passwd.host
    dbname = passwd.dbname
    user = passwd.user
    password = passwd.password
    port = passwd.port

    print("\nIniciando Processa Fluxo...\n")
    lancamentos, saldos, investimentos = processar_tabelas()

    print("\nConetando ao PostgreSQL...\n")
    engine = db.conectar_postgresql(host, dbname, user, password, port)

    print("\nSalvando no PostgresSQL...\n")
    salvar_em_postgres(
        lancamentos, saldos, investimentos, dim.contas, dim.compromissos,dim.datas, engine
    )

    print("\nFechando conexão com o PostgreSQL...\n")
    db.fechar_conexao(engine)
    
    # email = ""
    # while email != "S" or email != "N":
    #     email = input("\nDeseja enviar o email ?(S/N)\n").upper()
    #     if email == "S":
    #         print("\nEnviando email...\n")
    #         enviar_painel.main()
    #         break
    #     elif email == "N":
    #         print("\nEmail não enviado.\n")
    #         break
    #     else:
    #         print("\nOpção inválida.\nDigite 'S' para enviar o email ou 'N' para não enviar.\n")
    # print("\nProcesso finalizado.\n")
if __name__ == "__main__":
    main()
