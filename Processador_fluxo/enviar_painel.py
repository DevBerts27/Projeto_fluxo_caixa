import os
import io
import base64
from datetime import date, timedelta, datetime
from typing import Union, List, Dict, Any

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt

import django
from django.conf import settings as django_settings
from django.core.mail import EmailMessage
import django.template
from django.template.loader import render_to_string

from sqlalchemy import create_engine, Column, Integer, Float, String, Date, cast
from sqlalchemy.orm import declarative_base, sessionmaker

import dim as dm
import settings
import listas
import passwd as pg

from pyfiglet import figlet_format

Base = declarative_base()

def create_database_session(database_url: str):
    """Cria e retorna uma sessão do banco de dados."""
    print(f"Creating database session with URL: {database_url}")
    engine = create_engine(database_url)
    Session = sessionmaker(bind=engine)
    session = Session()
    print("Database session created successfully.")
    return session


def connect_to_database(engine):
    """Tenta conectar ao banco de dados."""
    try:
        engine.connect()
        print("Conexão com o banco de dados estabelecida com sucesso.")
    except Exception as e:
        print(f"Erro ao conectar ao banco de dados: {e}")


def configure_django():
    """Configura o Django, caso ainda não esteja configurado."""
    if not django_settings.configured:
        print("Configuring Django settings...")
        django_settings.configure(
            TEMPLATES=[
                {
                    "BACKEND": "django.template.backends.django.DjangoTemplates",
                    "DIRS": [os.path.join(os.path.dirname(__file__), "templates")],
                    "APP_DIRS": True,
                    "OPTIONS": {"context_processors": []},
                }
            ],
            INSTALLED_APPS=[],
        )
        django.setup()
        print("Django configured successfully.")


def _get_data_rel(data_input: date | None) -> List[date]:
    """
    Retorna uma lista de datas, onde data_rel é o dia anterior ao fornecido ou ao atual.
    """
    if isinstance(data_input, date):
        # Se `data_input` for uma única data, retornamos uma lista contendo essa data
        return [data_input]
    else:
        # Se `data_input` for None, gera uma lista dos últimos 7 dias
        data_rel = date.today() - timedelta(days=1)
        dias = [data_rel - timedelta(days=i) for i in range(7)]
        dias.reverse()
        print(f"Lista de datas para consulta: {dias}")
        return dias


def get_saldos(session, data_rel: List[date]) -> List[Dict[str, date | str | float]]:
    """
    Consulta e retorna todos os campos de 'FluxoSaldos' do banco de dados para as datas fornecidas.
    """
    # Garantir que data_rel é uma lista
    if not isinstance(data_rel, list):
        raise ValueError("data_rel deve ser uma lista de objetos datetime.date")

    print("Fetching all columns from FluxoSaldos...")
    saldos = (
        session.query(FluxoSaldos)
        .filter(cast(FluxoSaldos.data, Date).in_(data_rel))
        .all()
    )

    return [
        {col.name: getattr(saldo, col.name) for col in FluxoSaldos.__table__.columns}
        for saldo in saldos
    ]

def get_lancamentos(
    session, data_rel: List[date]
) -> List[Dict[str, date | str | float]]:
    """
    Consulta e retorna todos os campos de 'FluxoLancamentos' do banco de dados para as datas fornecidas.
    """
    if not isinstance(data_rel, list):
        raise ValueError("data_rel deve ser uma lista de objetos datetime.date")

    print("Fetching all columns from FluxoLancamentos...")
    lancamentos = (
        session.query(FluxoLancamentos)
        .filter(cast(FluxoLancamentos.data, Date).in_(data_rel))
        .all()
    )

    return [
        {
            col.name: getattr(lancamento, col.name)
            for col in FluxoLancamentos.__table__.columns
        }
        for lancamento in lancamentos
    ]

def get_investimentos(
    session, data_rel: List[date]
) -> List[Dict[str, date | str | float]]:
    """
    Consulta e retorna todos os campos de 'FluxoInvestimentos' do banco de dados para as datas fornecidas.
    """
    if not isinstance(data_rel, list):
        raise ValueError("data_rel deve ser uma lista de objetos datetime.date")

    print("Fetching all columns from FluxoInvestimentos...")
    investimentos = (
        session.query(FluxoInvestimentos)
        .filter(cast(FluxoInvestimentos.data, Date).in_(data_rel))
        .all()
    )

    return [
        {
            col.name: getattr(investimento, col.name)
            for col in FluxoInvestimentos.__table__.columns
        }
        for investimento in investimentos
    ]

def fetch_data(
    session, data_input: date | None
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """Obtém todos os dados de saldos, lançamentos e investimentos do banco de dados."""
    print("Fetching all data from database...")

    # Obtendo uma lista de datas, sempre garantindo que é uma lista
    data_rel = _get_data_rel(data_input)

    # Garantir que `data_rel` é uma lista de datas
    if not isinstance(data_rel, list):
        raise ValueError("data_rel deve ser uma lista de objetos datetime.date")
    
    saldos = pd.DataFrame(get_saldos(session, data_rel))
    lancamentos = pd.DataFrame(get_lancamentos(session, data_rel))
    investimentos = pd.DataFrame(get_investimentos(session, data_rel))

    print(len(data_rel))
    
    if len(data_rel) == 1:
        datas = [(data_input - timedelta(days=i)).strftime('%Y-%m-%d') for i in range(7)]
        print(f"Datas do grafico: {datas}")
        lancamentos_grafico = pd.DataFrame(get_lancamentos(session, datas))
    else:
        lancamentos_grafico = pd.DataFrame(get_lancamentos(session, data_rel))
        
    return saldos, lancamentos, investimentos, lancamentos_grafico

def DAX_entradas_liquidas(lancamentos: pd.DataFrame) -> float:
    """
    Calcula as entradas líquidas com base em tipos de compromisso
    presentes na lista de entradas, sem usar 'isin'.
    """
    lancamentos["tipos_de_compromisso"] = lancamentos["tipos_de_compromisso"].astype(
        str
    )
    entradas_a = lancamentos[
        (
            lancamentos["tipos_de_compromisso"]
            .astype(int)
            .isin(map(int, listas.entradas))
        )
        & (
            ~lancamentos["tipos_de_compromisso"]
            .astype(int)
            .isin(map(int, ["11", "08", "09"]))
        )
    ]
    entradas_liquidas = entradas_a["valores"].sum()
    entradas_liquidas = round(entradas_liquidas, 2)

    return entradas_liquidas

def DAX_saidas_liquidas(lancamentos: pd.DataFrame) -> float:
    """Calcula as entradas líquidas."""
    saidas_a = lancamentos[
        (lancamentos["tipos_de_compromisso"].astype(int).isin(map(int, listas.saidas)))
        & (
            ~lancamentos["tipos_de_compromisso"]
            .astype(int)
            .isin(map(int, ["94", "98", "97"]))
        )
    ]
    saidas_liquidas = saidas_a["valores"].sum()
    saidas_liquidas.astype(float)
    saidas_liquidas = round(saidas_liquidas, 2)
    return saidas_liquidas

def DAX_saldo_inicial(saldos: pd.DataFrame) -> float:
    """Calcula o saldo inicial."""
    min_data = saldos["data"].min()
    filtro = (saldos["data"] == min_data) & (
        saldos["saldo_final_inicial"].str.upper() == "SALDO INICIAL"
    )
    saldo_inicial = saldos.loc[filtro, "valor_saldo_final_inicial"].sum()
    saldo_inicial = round(saldo_inicial, 2)
    return saldo_inicial

def DAX_saldo_final(saldos: pd.DataFrame) -> float:
    """Calcula o saldo final."""
    max_data = saldos["data"].max()
    filtro = (saldos["data"] == max_data) & (
        saldos["saldo_final_inicial"].str.upper() == "SALDO FINAL"
    )
    saldo_final = saldos.loc[filtro, "valor_saldo_final_inicial"].sum()
    saldo_final = round(saldo_final, 2)
    return saldo_final

def DAX_entradas_saidas(lancamentos: pd.DataFrame) -> float:
    # entradas_liquidas = DAX_entradas_liquidas(lancamentos)
    # saidas_liquidas = DAX_saidas_liquidas(lancamentos)
    # entradas_saidas = round(entradas_saidas, 2)
    entradas = lancamentos[
        (
            lancamentos["tipos_de_compromisso"]
            .astype(int)
            .isin(map(int, listas.entradas))
        )
    ]
    entradas = entradas.groupby("banco")["valores"].sum().round(2)

    # Cálculo das Saídas
    saidas = lancamentos[
        (lancamentos["tipos_de_compromisso"].astype(int).isin(map(int, listas.saidas)))
    ]
    saidas = saidas.groupby("banco")["valores"].sum().round(2)
    entradas_liquidas = entradas.astype(float).sum()
    saidas_liquidas = saidas.astype(float).sum()
    entradas_saidas = entradas_liquidas - saidas_liquidas
    return entradas_saidas

def DAX_resgate_aplicacao(investimentos: pd.DataFrame) -> float:
    """Calcula o resgate de aplicação."""
    resgate = investimentos["resgate"].sum()
    aplicacao = investimentos["aplicacao"].sum()
    resgate_aplicacao = resgate - aplicacao
    resgate_aplicacao = round(resgate_aplicacao, 2)
    return resgate_aplicacao

def DAX_saldo_aplicado(investimentos: pd.DataFrame) -> float:
    """Calcula o saldo aplicado."""
    max_data = investimentos["data"].max()
    filtro = (investimentos["data"] == max_data) & (investimentos["saldo_atual"])
    saldo_aplicado = investimentos.loc[filtro, "saldo_atual"].sum()
    saldo_aplicado = round(saldo_aplicado, 2)
    return saldo_aplicado

def DAX_saldo_total(saldos: pd.DataFrame, investimentos: pd.DataFrame) -> float:
    """Calcula o saldo total."""
    saldo_final = DAX_saldo_final(saldos)
    saldo_aplicado = DAX_saldo_aplicado(investimentos)
    saldo_total = saldo_final + saldo_aplicado
    saldo_total = round(saldo_total, 2)
    return saldo_total

def DAX_saldo_bloqueado(investimentos: pd.DataFrame) -> float:
    """Calcula o saldo bloqueado."""
    max_data = investimentos["data"].max()

    filtro = (investimentos["data"] == max_data) & (investimentos["saldo_bloqueado"])
    saldo_bloqueado = investimentos["saldo_bloqueado"].sum()
    saldo_bloqueado = round(saldo_bloqueado, 2)
    return saldo_bloqueado

def data_box(data_rel: List[date] | None) -> str:
    """Retorna a data do relatório."""
    if data_rel == None:
        data_rel = date.today() - timedelta(days=1)
        return data_rel.strftime("%d/%m/%Y")

    data = data_rel.strftime("%d/%m/%Y")
    print(f"Data do relatório: {data}")
    return data

def tabela_saldo_investimentos_atual(investimentos: pd.DataFrame) -> dict:

    max_data = investimentos["data"].max()
    # Filtrar o DataFrame para a última data
    filtro = investimentos["data"] == max_data
    # Selecionar as colunas 'banco', 'data' e 'saldo_atual'
    saldo_atual = investimentos.loc[filtro, ["banco", "data", "saldo_atual"]]
    # Arredondar os valores de saldo_atual
    saldo_atual = saldo_atual.drop(saldo_atual[saldo_atual["saldo_atual"] == 0].index)
    saldo_atual = saldo_atual.groupby("banco", as_index=False).agg(
        {"saldo_atual": "sum", "data": "first"}
    )
    saldo_atual = saldo_atual.sort_values(by="saldo_atual", ascending=False).round(2)
    saldo_atual = saldo_atual.to_dict(orient="records")

    return saldo_atual

def tabela_saldo_investimentos_bloqueado(investimentos: pd.DataFrame) -> dict:
    max_data = investimentos["data"].max()
    # Filtrar o DataFrame para a última data
    filtro = investimentos["data"] == max_data
    # Selecionar as colunas 'banco', 'data' e 'saldo_bloqueado'
    saldo_bloqueado = investimentos.loc[filtro, ["banco", "data", "saldo_bloqueado"]]
    # Arredondar os valores de saldo_bloqueado
    saldo_bloqueado["saldo_bloqueado"] = saldo_bloqueado["saldo_bloqueado"].round(2)
    saldo_bloqueado = saldo_bloqueado.drop(
        saldo_bloqueado[saldo_bloqueado["saldo_bloqueado"] == 0].index
    )
    saldo_bloqueado = saldo_bloqueado.groupby("banco", as_index=False).agg(
        {"saldo_bloqueado": "sum", "data": "first"}
    )
    saldo_bloqueado = saldo_bloqueado.sort_values(by="saldo_bloqueado", ascending=False)
    saldo_bloqueado = saldo_bloqueado.to_dict(orient="records")

    return saldo_bloqueado

def tabela_saldo_disponivel(saldos: pd.DataFrame, investimentos: pd.DataFrame) -> dict:

    max_data = saldos["data"].max()

    # Filtrar saldos pela data máxima e onde saldo_final_inicial for "SALDO FINAL"
    filtro_saldo = (saldos["data"] == max_data) & (
        saldos["saldo_final_inicial"].astype(str).str.upper() == "SALDO FINAL"
    )

    # Filtrar investimentos pela data máxima
    filtro_investimentos = investimentos["data"] == max_data

    # Selecionar os saldos e investimentos filtrados
    saldos_filtrados = saldos.loc[filtro_saldo, ["banco", "valor_saldo_final_inicial"]]
    investimentos_filtrados = investimentos.loc[
        filtro_investimentos, ["banco", "saldo_disponivel"]
    ]

    # Agrupar e somar os saldos e investimentos por banco
    saldo_final_por_banco = (
        saldos_filtrados.groupby("banco")["valor_saldo_final_inicial"]
        .sum()
        .reset_index()
    )
    saldo_disponivel_por_banco = (
        investimentos_filtrados.groupby("banco")["saldo_disponivel"].sum().reset_index()
    )

    # Combinar os DataFrames de saldo final e saldo disponível por banco
    saldo_total_por_banco = pd.merge(
        saldo_final_por_banco, saldo_disponivel_por_banco, on="banco", how="outer"
    ).fillna(0)

    # Somar saldo final e saldo disponível para cada banco
    saldo_total_por_banco["saldo_total"] = (
        saldo_total_por_banco["valor_saldo_final_inicial"]
        + saldo_total_por_banco["saldo_disponivel"]
    )

    # Converter para dicionário com banco como chave e saldo total como valor
    saldo_disponivel = (
        saldo_total_por_banco.set_index("banco")["saldo_total"]
        .sort_values(ascending=False)
        .round(2)
        .to_dict()
    )

    return saldo_disponivel

def tabela_entradas_tipo(lancamentos: pd.DataFrame) -> dict:
    compromissos = dm.compromissos
    
    lancamentos["tipos_de_compromisso"] = lancamentos["tipos_de_compromisso"].astype(int)
    compromissos["id_compromisso"] = compromissos["id_compromisso"].astype(int)
    
    lancamentos = lancamentos.merge(
        compromissos[['id_compromisso', 'compromisso', 'tipo']],  # Colunas relevantes de compromissos
        left_on="tipos_de_compromisso", 
        right_on="id_compromisso", 
        how="left"
    )
    entradas = lancamentos[lancamentos["tipo"] == "Entrada"]
    entradas = (
        entradas.groupby("compromisso")["valores"]
        .sum()
        .round(2)
        .sort_values(ascending=False)
    )
    entradas = entradas.drop(entradas[entradas == 0].index)
    entradas = entradas.to_dict()

    return entradas

def tabela_saidas_tipo(lancamentos: pd.DataFrame) -> dict:
    compromissos = dm.compromissos
    
    lancamentos["tipos_de_compromisso"] = lancamentos["tipos_de_compromisso"].astype(int)
    compromissos["id_compromisso"] = compromissos["id_compromisso"].astype(int)
    
    lancamentos = lancamentos.merge(
        compromissos[['id_compromisso', 'compromisso', 'tipo']],  # Colunas relevantes de compromissos
        left_on="tipos_de_compromisso", 
        right_on="id_compromisso", 
        how="left"
    )
    saidas_l = lancamentos[(lancamentos["tipo"] == "Saída")]
    saidas_l = (
        saidas_l.groupby("compromisso")["valores"]
        .sum()
        .round(2)
        .sort_values(ascending=False)
    )
    saidas_l = saidas_l.drop(saidas_l[saidas_l == 0].index)
    saidas_l = saidas_l.to_dict()

    return saidas_l

def tabela_fluxo_de_caixa(lancamentos: pd.DataFrame, saldos: pd.DataFrame) -> dict:
    # Cálculo das Entradas

    entradas = lancamentos[
        (
            lancamentos["tipos_de_compromisso"]
            .astype(int)
            .isin(map(int, listas.entradas))
        )
    ]
    entradas = entradas.groupby("banco")["valores"].sum().round(2)

    # Cálculo das Saídas
    saidas = lancamentos[
        (lancamentos["tipos_de_compromisso"].astype(int).isin(map(int, listas.saidas)))
    ]
    saidas = saidas.groupby("banco")["valores"].sum().round(2)

    # Cálculo do Saldo Inicial
    saldo_inicial = (
        saldos[saldos["saldo_final_inicial"] == "SALDO INICIAL"]
        .groupby("banco")["valor_saldo_final_inicial"]
        .sum()
        .round(2)
    )

    # Cálculo do Saldo Final
    saldo_final = (
        saldos[saldos["saldo_final_inicial"] == "SALDO FINAL"]
        .groupby("banco")["valor_saldo_final_inicial"]
        .sum()
        .round(2)
    )

    # Monta o dicionário de fluxo de caixa para cada banco
    fluxo_de_caixa_dict = {}

    bancos = saldos["banco"].unique()

    for banco in bancos:
        fluxo_de_caixa_dict[banco] = {
            "saldo_inicial": saldo_inicial.get(banco, 0),
            "entradas": entradas.get(banco, 0),
            "saidas": saidas.get(banco, 0),
            "saldo_final": saldo_final.get(banco, 0),
        }
    
    fluxo_de_caixa_ordenado = dict(
    sorted(fluxo_de_caixa_dict.items(), key=lambda item: item[1]["entradas"], reverse=True)
    )
    
     # Cálculo dos Totais
    fluxo_de_caixa_ordenado["Totais"] = {
        "saldo_inicial": saldo_inicial.sum(),
        "entradas": entradas.sum(),
        "saidas": saidas.sum(),
        "saldo_final": saldo_final.sum(),
    }

    return fluxo_de_caixa_ordenado

def _entradas_grafico(lancamentos_grafico: pd.DataFrame) -> pd.DataFrame:
    compromissos = dm.compromissos
    
    # Certificar que 'tipos_de_compromisso' e 'id_compromisso' são inteiros
    lancamentos_grafico["tipos_de_compromisso"] = lancamentos_grafico["tipos_de_compromisso"].astype(int)
    compromissos["id_compromisso"] = compromissos["id_compromisso"].astype(int)
    
    # Mesclar o DataFrame 'lancamentos' com 'compromissos' para trazer a descrição do compromisso
    lancamentos_grafico = lancamentos_grafico.merge(
        compromissos[['id_compromisso', 'compromisso', 'tipo']],  # Colunas relevantes de compromissos
        left_on="tipos_de_compromisso", 
        right_on="id_compromisso", 
        how="left"
    )

    # Filtrar as entradas_grafico (tipo "Entrada")
    entradas_grafico = lancamentos_grafico[(lancamentos_grafico["tipo"] == "Entrada") & (~lancamentos_grafico["tipos_de_compromisso"].isin([11, 8, 9]))]

    # Agrupar por 'compromisso' e 'data', somar os valores, e ordenar
    entradas_grafico = (
        entradas_grafico.groupby(["compromisso", "data"])["valores"]
        .sum()
        .reset_index()  # Redefinir o índice para manter a estrutura de DataFrame
        .round(2)
        .sort_values(by="valores", ascending=False)
    )

    # Remover as entradas_grafico com valor zero
    entradas_grafico = entradas_grafico[entradas_grafico["valores"] != 0]

    return entradas_grafico

def _saidas_grafico(lancamentos_grafico: pd.DataFrame) -> pd.DataFrame:
    compromissos = dm.compromissos
    
    # Certificar que 'tipos_de_compromisso' e 'id_compromisso' são inteiros
    lancamentos_grafico["tipos_de_compromisso"] = lancamentos_grafico["tipos_de_compromisso"].astype(int)
    compromissos["id_compromisso"] = compromissos["id_compromisso"].astype(int)
    
    # Mesclar o DataFrame 'lancamentos' com 'compromissos' para trazer a descrição do compromisso
    lancamentos_grafico = lancamentos_grafico.merge(
        compromissos[['id_compromisso', 'compromisso', 'tipo']],  # Colunas relevantes de compromissos
        left_on="tipos_de_compromisso", 
        right_on="id_compromisso", 
        how="left"
    )

    # Filtrar as saidas_grafico (tipo "Entrada")
    saidas_grafico = lancamentos_grafico[(lancamentos_grafico["tipo"] == "Saída") & (~lancamentos_grafico["tipos_de_compromisso"].isin([94, 98, 97]))]

    # Agrupar por 'compromisso' e 'data', somar os valores, e ordenar
    saidas_grafico = (
        saidas_grafico.groupby(["compromisso", "data"])["valores"]
        .sum()
        .reset_index()  # Redefinir o índice para manter a estrutura de DataFrame
        .round(2)
        .sort_values(by="valores", ascending=False)
    )

    # Remover as saidas_grafico com valor zero
    saidas_grafico = saidas_grafico[saidas_grafico["valores"] != 0]

    return saidas_grafico

def grafico_entrdas_saidas_7dias(lancamentos_grafico: pd.DataFrame) -> str:
    # Gerar o gráfico conforme o código anterior
    entradas = _entradas_grafico(lancamentos_grafico)
    saidas = _saidas_grafico(lancamentos_grafico)
    
    # Agrupar entradas e saídas por data
    entradas_agrupadas = entradas.groupby("data")["valores"].sum().reset_index()
    saidas_agrupadas = saidas.groupby("data")["valores"].sum().reset_index()
    
    # Merge e cálculo das entradas-saídas
    entradas_saidas = pd.merge(entradas_agrupadas, saidas_agrupadas, on="data", how="outer", suffixes=('_entradas', '_saidas')).fillna(0)
    entradas_saidas["Entradas_Saidas"] = entradas_saidas["valores_entradas"] - entradas_saidas["valores_saidas"]
    
    # Filtrar os últimos 7 dias
    entradas_saidas = entradas_saidas.sort_values(by="data", ascending=False).head(7).sort_values(by="data")
    
    # Gerar o gráfico de barras
    fig, ax = plt.subplots(figsize=(10, 6))
    bars = ax.bar(entradas_saidas["data"], entradas_saidas["Entradas_Saidas"], 
                  color=["blue" if x >= 0 else "orange" for x in entradas_saidas["Entradas_Saidas"]])
    
    for bar in bars:
        yval = bar.get_height()
        ax.text(bar.get_x() + bar.get_width()/2, yval, f'R$ {yval:,.2f}'.replace(",", "X").replace(".", ",").replace("X", "."),
                ha='center', va='bottom' if yval >= 0 else 'top')

    ax.set_title("Entradas e Saídas dos Últimos 7 Dias", fontsize=13)
    ax.set_ylabel("Entradas - Saídas", fontsize=10)
    ax.set_xlabel("Data", fontsize=10)
    ax.grid(True, which='both', linestyle='--', linewidth=0.5, axis='y')
    ax.set_axisbelow(True)
    plt.xticks(rotation=45)
    plt.tight_layout()

    # Salvar o gráfico em memória como PNG
    buffer = io.BytesIO()
    plt.savefig(buffer, format='png')
    buffer.seek(0)
    
    # Converter o gráfico em string base64
    image_png = buffer.getvalue()
    buffer.close()
    image_base64 = base64.b64encode(image_png).decode('utf-8')
    
    return image_base64

def render_template(
    dados: List[pd.DataFrame], template: django.template.Template, data_rel: date
) -> str:
    """Renderiza o template Django com os dados fornecidos."""
    with open("templates/painel.html", "r", encoding="utf-8") as f:
        template = django.template.Template(f.read())
    print("Rendering template with data...")
    configure_django()

    saldos, lancamentos, investimentos, lancamentos_grafico = dados

    print("Data rendered successfully.")

    entradas_tipo = tabela_entradas_tipo(lancamentos)
    entradas_total = sum(entradas_tipo.values())
    
    saidas_tipo = tabela_saidas_tipo(lancamentos)
    saidas_total = sum(saidas_tipo.values())

    saldo_disponivel = tabela_saldo_disponivel(saldos, investimentos)
    saldo_disponivel_total = sum(saldo_disponivel.values())

    saldo_bloqueado = tabela_saldo_investimentos_bloqueado(investimentos)
    saldo_bloqueado_total = []
    for saldo in saldo_bloqueado:
        saldo_bloqueado_total.append(saldo["saldo_bloqueado"])
    saldo_bloqueado_total = sum(saldo_bloqueado_total)
    
    saldo_atual = tabela_saldo_investimentos_atual(investimentos)
    saldo_atual_total = []
    for saldo in saldo_atual:
        saldo_atual_total.append(saldo["saldo_atual"])
    saldo_atual_total = sum(saldo_atual_total)
    
    contexto = django.template.Context(
        {
            "entradas_liquidas": formatar_float_brasileiro(DAX_entradas_liquidas(lancamentos)),
            "saidas_liquidas": formatar_float_brasileiro(DAX_saidas_liquidas(lancamentos)),
            "saldo_inicial": formatar_float_brasileiro(DAX_saldo_inicial(saldos)),
            "saldo_final": formatar_float_brasileiro(DAX_saldo_final(saldos)),
            "entradas_saidas": formatar_float_brasileiro(DAX_entradas_saidas(lancamentos)),
            "resgate_aplicacao": formatar_float_brasileiro(DAX_resgate_aplicacao(investimentos)),
            "saldo_aplicado": formatar_float_brasileiro(DAX_saldo_aplicado(investimentos)),
            "saldo_total": formatar_float_brasileiro(DAX_saldo_total(saldos, investimentos)),
            "saldo_bloqueado": formatar_float_brasileiro(DAX_saldo_bloqueado(investimentos)),
            
            "Data": data_box(data_rel),  # mudar comforme o dia do relatório
            
            "entradas_saidas_7dias": grafico_entrdas_saidas_7dias(lancamentos_grafico),  # grafico
            
            "saldo_investimentos_atual": formatar_float_brasileiro_dict(tabela_saldo_investimentos_atual(
                investimentos
            )),
            "saldo_investimentos_bloqueado": formatar_float_brasileiro_dict(tabela_saldo_investimentos_bloqueado(
                investimentos
            )),
            "saldo_disponivel": formatar_float_brasileiro_dict(saldo_disponivel),
            "fluxo_de_caixa": formatar_float_brasileiro_dict_EXCLUSIVO(tabela_fluxo_de_caixa(lancamentos, saldos)),  # tabela
            "entradas_tipo": formatar_float_brasileiro_dict(entradas_tipo),  # tabela
            "saidas_tipo": formatar_float_brasileiro_dict(saidas_tipo),  # tabela
            
            "total_investimentos_atual":formatar_float_brasileiro(saldo_atual_total),
            "total_investimentos_bloqueado":formatar_float_brasileiro(saldo_bloqueado_total),
            "total_saldo_disponivel":formatar_float_brasileiro(saldo_disponivel_total),
            # "total_fluxo_de_caixa":formatar_float_brasileiro_dict_EXCLUSIVO(tabela_fluxo_de_caixa(lancamentos, saldos)),
            "total_entradas_tipo":formatar_float_brasileiro(entradas_total),
            "total_saidas_tipo":formatar_float_brasileiro(saidas_total),
        }
    )
    rendered_html = template.render(contexto)
    print("Template rendered successfully.")
    with open("templates/painel_rendered.html", "w", encoding="utf-8") as f:
        f.write(rendered_html)
    return rendered_html


def send_email(destinatarios: List[str], html_content: str, cc: List[str] | None, data_input):
    """Envia um email com o conteúdo HTML fornecido."""
    print(f"Sending email to: {destinatarios}")
    if data_input == None:
        data_input = date.today() - timedelta(days=1)
    assunto = f"Painel de Fluxo de Caixa Diário: {data_input.strftime("%d/%m/%Y")}"
    email = EmailMessage(assunto, html_content, to=destinatarios,cc=cc)
    email.content_subtype = "html"
    email.send()
    print("Email enviado com sucesso.")


# Definições de tabelas do banco de dados
class FluxoSaldos(Base):
    __tablename__ = "fluxo_saldos"
    index = Column(Integer, primary_key=True)
    saldo_final_inicial = Column(String)
    data = Column(Date)
    valor_saldo_final_inicial = Column(Float)
    banco = Column(String)


class FluxoLancamentos(Base):
    __tablename__ = "fluxo_lancamentos"
    index = Column(Integer, primary_key=True)
    tipos_de_compromisso = Column(String)
    data = Column(Date)
    banco = Column(String)
    valores = Column(Float)


class FluxoInvestimentos(Base):
    __tablename__ = "fluxo_investimentos"
    index = Column(Integer, primary_key=True)
    data = Column(Date)
    banco = Column(String)
    modalidade = Column(String)
    aplicacao = Column(Float)
    resgate = Column(Float)
    rendimento_bruto = Column(Float)
    rendimento_liquido = Column(Float)
    saldo_atual = Column(Float)
    rentabilidade = Column(Float)
    # cdi = Column(Float)
    rentabilidade_dia = Column(Float)
    tipo_de_bloqueio = Column(String)
    saldo_bloqueado = Column(Float)
    saldo_disponivel = Column(Float)


# Execução do painel
def execute_panel(database_url: str, destinatarios: List[str], data_input: date | None, cc: List[str] | None = None):
    """Executa o processo completo de obter dados, renderizar template e enviar email."""
    print("Executing panel process...")
    session = create_database_session(database_url)
    dados = fetch_data(session, data_input)
    template = "templates/painel_rendered.html"

    html_content = render_template(dados, template, data_input)

    # Envia email
    send_email(destinatarios, html_content, cc, data_input)
    print("Panel process executed successfully.")

def formatar_float_brasileiro(valor: float) -> str:
    # Formatar o número como string com duas casas decimais e substituindo '.' por ','
    return f"{valor:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")


def formatar_float_brasileiro_dict_EXCLUSIVO(dados: Union[Dict[str, Dict[str, float]], List[Dict[str, Any]]]) -> Union[Dict[str, Dict[str, str]], List[Dict[str, Any]]]:
    # Caso a entrada seja uma lista de dicionários
    if isinstance(dados, list):
        for registro in dados:
            for chave, valor in registro.items():
                if isinstance(valor, float):
                    registro[chave] = f"{valor:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")

    # Caso a entrada seja um dicionário aninhado
    elif isinstance(dados, dict):
        for banco, valores in dados.items():
            if isinstance(valores, dict):
                for chave, valor in valores.items():
                    if isinstance(valor, float):
                        valores[chave] = f"{valor:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")

    return dados

def formatar_float_brasileiro_dict(dados: Dict[Any, float]) -> Dict[Any, str]:
    if isinstance(dados, list):
        for registro in dados:
            for chave, valor in registro.items():
                if isinstance(valor, float):
                    registro[chave] = f"{valor:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
    
    # Caso a entrada seja um dicionário simples (ex: {banco: saldo_total})
    elif isinstance(dados, dict):
        for chave, valor in dados.items():
            if isinstance(valor, float):
                dados[chave] = f"{valor:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
    
    return dados

def main():

    print(f"{figlet_format("Cashflow\nPanel\nSender",font='slant')}\nby Pedro\n")

    dest = input("Deseja enviar para o chefe ?(S/N)\n").strip().upper()
    
    if dest == "S":    
        print("chefe selecionado...")
        destinatarios = ["angeloluiz@balaroti.com.br"]
        cc = ["pedro.bertoldo@balaroti.com.br","ly.salles@balaroti.com.br","vitor.maia@balaroti.com.br"]
    
    else:
        print("Eu mesmo selecionado")
        destinatarios = ["pedro.bertoldo@balaroti.com.br"]
        cc = ["pedro.bertoldo@balaroti.com.br"]

    pergunta = (
        input(
            "Deseja um data específica? (S/N): \n Caso N, será considerado o dia anterior do dia atual(ontem). \n"
        )
        .strip()
        .upper()
    )
    if pergunta == "S":
        data_input_antes = input("Digite a data no formato DD-MM-YYYY: ")
        data_input = data_input_antes.format("%d-%m-%Y")
        data_input = datetime.strptime(data_input, "%d-%m-%Y").date()
    else:
        print("Data de referência: Ontem")
        data_input = None


    print(f"Data de referência: {data_input}")
    print("Starting main process...")
    execute_panel(database_url=pg.connurl, destinatarios=destinatarios, data_input=data_input, cc=cc)
    print("Main process finished.")


if __name__ == "__main__":
    main()
