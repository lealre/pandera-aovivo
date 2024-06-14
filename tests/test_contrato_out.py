import sys
import os
# Add the root directory of your project to sys.path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
import pandas as pd
import numpy as np
import pandera as pa
import pytest

from src.contrato import MetricasFinanceirasOut

def test_contrato_correto():
    df_test = pd.DataFrame({
        "setor_da_empresa": ["VND_A1B2C3", "REP_X7Y8Z9", "MNT_4D5E6F"],
        "receita_operacional": [1000,1000,1000],
        "data": ["2023-01-01", "2023-01-01", "2023-01-01"],
        "percentual_de_imposto": [0.1, 0.1, 0.1],
        "custo_operacionais": [200,200,200],
        "valor_do_imposto": [100,100,100],
        "custo_total": [300,300,300],
        "receita_liquida": [700,700,700],
        "margem_operacional": [700/1000,700/1000,700/1000]
    })

    MetricasFinanceirasOut.validate(df_test)

def test_contrato_correto_coluna_opcional():
    df_test = pd.DataFrame({
        "setor_da_empresa": ["VND_A1B2C3", "REP_X7Y8Z9", "MNT_4D5E6F"],
        "receita_operacional": [1000,1000,1000],
        "data": ["2023-01-01", "2023-01-01", "2023-01-01"],
        "percentual_de_imposto": [0.1, 0.1, 0.1],
        "custo_operacionais": [200,200,200],
        "valor_do_imposto": [100,100,100],
        "custo_total": [300,300,300],
        "receita_liquida": [700,700,700],
        "margem_operacional": [700/1000,700/1000,700/1000],
        "transformado_em": ["2024-01-01", "2024-01-01", "2024-01-01"]
    })

    MetricasFinanceirasOut.validate(df_test)

def test_coluna_em_falta():
    df_test = pd.DataFrame({
        "setor_da_empresa": ["VND_A1B2C3", "REP_X7Y8Z9", "MNT_4D5E6F"],
        "receita_operacional": [1000,1000,1000],
        "data": ["2023-01-01", "2023-01-01", "2023-01-01"],
        "percentual_de_imposto": [0.1, 0.1, 0.1],
        "custo_operacionais": [200,200,200],
        "valor_do_imposto": [100,100,100],
        "receita_liquida": [700,700,700],
        "margem_operacional": [700/1000,700/1000,700/1000]
    })

    with pytest.raises(pa.errors.SchemaError):
        MetricasFinanceirasOut.validate(df_test)

def test_coluna_adicional():
    df_test = pd.DataFrame({
        "setor_da_empresa": ["VND_A1B2C3", "REP_X7Y8Z9", "MNT_4D5E6F"],
        "receita_operacional": [1000,1000,1000],
        "data": ["2023-01-01", "2023-01-01", "2023-01-01"],
        "percentual_de_imposto": [0.1, 0.1, 0.1],
        "custo_operacionais": [200,200,200],
        "valor_do_imposto": [100,100,100],
        "custo_total": [300,300,300],
        "receita_liquida": [700,700,700],
        "margem_operacional": [700/1000,700/1000,700/1000],
        "coluna_adicional": [0,0,0]
    })


    with pytest.raises(pa.errors.SchemaError):
        MetricasFinanceirasOut.validate(df_test)

def test_valor_em_falta():
    df_test = pd.DataFrame({
        "setor_da_empresa": ["VND_A1B2C3", "REP_X7Y8Z9", "MNT_4D5E6F"],
        "receita_operacional": [1000,1000,1000],
        "data": ["2023-01-01", "2023-01-01", "2023-01-01"],
        "percentual_de_imposto": [0.1, 0.1, 0.1],
        "custo_operacionais": [200,200,200],
        "valor_do_imposto": [100,100,100],
        "custo_total": [300,300,300],
        "receita_liquida": [np.nan,700,700],
        "margem_operacional": [700/1000,700/1000,700/1000],
    })


    with pytest.raises(pa.errors.SchemaError):
        MetricasFinanceirasOut.validate(df_test)

def test_calculo_margem():
    df_test = pd.DataFrame({
        "setor_da_empresa": ["VND_A1B2C3", "REP_X7Y8Z9", "MNT_4D5E6F"],
        "receita_operacional": [1000,1000,1000],
        "data": ["2023-01-01", "2023-01-01", "2023-01-01"],
        "percentual_de_imposto": [0.1, 0.1, 0.1],
        "custo_operacionais": [200,200,200],
        "valor_do_imposto": [100,100,100],
        "custo_total": [300,300,300],
        "receita_liquida": [700,700,700],
        "margem_operacional": [50,700/1000,700/1000],
    })


    with pytest.raises(pa.errors.SchemaError):
        MetricasFinanceirasOut.validate(df_test)