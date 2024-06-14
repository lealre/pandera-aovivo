import pandera as pa
import pandas as pd
from pandera.typing import Series
from typing import Optional

class MetricasFinanceirasBase(pa.DataFrameModel):
    setor_da_empresa: Series[str]
    receita_operacional: Series[float] = pa.Field(ge=0)
    data: Series[pa.DateTime] 
    percentual_de_imposto: Series[float] = pa.Field(in_range= {"min_value": 0, "max_value": 1})
    custo_operacionais: Series[float] = pa.Field(ge=0)

    class Config:
        strict = True
        coerce = True
    
    @pa.check(
            "setor_da_empresa", 
            name = "Checagem código dos setores",
            error = "Cógido do setor da empresa é inválido")
    def checa_codigo_setor(cls, codigo: Series[str]) -> Series[bool]:
        return codigo.str[:4].isin(['REP_', 'MNT_', 'VND_'])

class MetricasFinanceirasOut(MetricasFinanceirasBase):
    valor_do_imposto: Series[float] = pa.Field(ge=0)
    custo_total: Series[float] = pa.Field(ge=0)
    receita_liquida: Series[float] = pa.Field(ge=0)
    margem_operacional: Series[float] = pa.Field(ge=0)
    transformado_em: Optional[pa.DateTime]

    @pa.dataframe_check
    def checa_margem_operacional(cls, df:pd.DataFrame) -> Series[bool]:
        return df["margem_operacional"] == (df["receita_liquida"] / df["receita_operacional"]) 
    

