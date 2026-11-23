# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# MARKDOWN ********************

# # nb_gold: monta uma tabela da camada Gold por vez
# Notebook **genérico**: o pipeline (ou quem roda) diz qual tabela montar pelos parâmetros, vindos do `db_control`.
# Tabela nova = linha nova no seed + uma função nova em `TRANSFORMS`; o resto não muda.
# A Gold padroniza os nomes (snake_case minúsculo) e entrega o modelo dimensional (estrela):
# `fato_vendas` + `dim_clientes`, `dim_produtos`, `dim_vendedores`, `dim_empresas`, `dim_data`.
# A prova de que ficou certo é o notebook `nb_gold_checks`.

# CELL ********************

%run nb_functions

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import datetime
import sempy.fabric as fabric
from pyspark.sql import functions as f

workspace_name = fabric.resolve_workspace_name()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### Parâmetros passados pelo pipeline (obtidos do db_control)

# PARAMETERS CELL ********************

# Parâmetros passados pelo pipeline (obtidos do db_control)
source_storage = "lh_silver"
source_table   = "fato_vendas"

target_storage = "lh_gold"
target_table   = "fato_vendas"
target_mode    = "merge"        # opção: merge ou overwrite
target_key     = "cod_venda"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### Transformações: uma função por tabela Gold

# CELL ********************

def renomear(df, mapa: dict):
    """Seleciona e renomeia colunas na ordem do mapa (falha se faltar coluna: melhor que errar em silêncio)."""
    return df.select([f.col(antigo).alias(novo) for antigo, novo in mapa.items()])


def t_fato_vendas(df):
    return renomear(df, {
        "Cod_Venda": "cod_venda", "N_Doc": "n_doc", "Cod_Empresa": "cod_empresa",
        "Cod_produto": "cod_produto", "Cod_Vendedor": "cod_vendedor", "cod_Cliente": "cod_cliente",
        "MOVIMENTO": "movimento", "Quantidade": "quantidade", "Venda_Bruta": "venda_bruta",
        "Desconto": "desconto", "Venda_Liquida": "venda_liquida", "DataCarga": "data_carga",
    })


def t_dim_clientes(df):
    return renomear(df, {
        "ENTIDADE": "cod_cliente", "Nome": "nome", "Nome_Fantasia": "nome_fantasia",
        "Nome_Classificacao": "nome_classificacao", "Cidade": "cidade", "Estado": "estado",
        "UF": "uf", "Data_Carga": "data_carga",
    })


def t_dim_produtos(df):
    return renomear(df, {
        "PRODUTO": "cod_produto", "DESCRICAO": "descricao", "DESCRICAO_REDUZIDA": "descricao_reduzida",
        "FAMILIA_PRODUTO": "familia_produto", "SECAO_PRODUTO": "secao_produto",
        "GRUPO_PRODUTO": "grupo_produto", "SUBGRUPO_PRODUTO": "subgrupo_produto",
        "MARCA": "marca", "DataCarga": "data_carga",
    })


def t_dim_vendedores(df):
    base = renomear(df, {"VENDEDOR": "cod_vendedor", "NOME": "nome", "DataCarga": "data_carga"})
    # Vendas sem vendedor entram com código 0 na fato. O membro 0 evita que elas sumam dos relatórios.
    tipo = base.schema["cod_vendedor"].dataType
    sem_vendedor = spark.range(1).select(
        f.lit(0).cast(tipo).alias("cod_vendedor"),
        f.lit("Sem vendedor").alias("nome"),
        f.current_timestamp().alias("data_carga"),
    )
    return base.filter(f.col("cod_vendedor") != 0).unionByName(sem_vendedor)


def t_dim_empresas(df):
    return renomear(df, {
        "CodigoEmpresa": "cod_empresa", "NOME": "nome", "NomeFantasia": "nome_fantasia", "DataCarga": "data_carga",
    })


MESES = ["Janeiro", "Fevereiro", "Março", "Abril", "Maio", "Junho",
         "Julho", "Agosto", "Setembro", "Outubro", "Novembro", "Dezembro"]
DIAS = ["Segunda-feira", "Terça-feira", "Quarta-feira", "Quinta-feira", "Sexta-feira", "Sábado", "Domingo"]


def t_dim_data(df):
    """Calendário gerado por Spark: anos inteiros, do 1º de janeiro do primeiro ano de venda ao 31 de dezembro do último.
    A origem (df) é a fato da Silver, usada só para descobrir o intervalo."""
    limites = df.agg(f.min("MOVIMENTO").alias("ini"), f.max("MOVIMENTO").alias("fim")).first()
    inicio = datetime.date(limites["ini"].year, 1, 1)
    fim = datetime.date(limites["fim"].year, 12, 31)
    dias = spark.sql(f"SELECT explode(sequence(to_date('{inicio}'), to_date('{fim}'), interval 1 day)) AS data")
    nomes_mes = f.array(*[f.lit(m) for m in MESES])
    nomes_dia = f.array(*[f.lit(d) for d in DIAS])
    dia_semana = ((f.dayofweek("data") + 5) % 7) + 1          # 1 = segunda ... 7 = domingo (ISO)
    return dias.select(
        "data",
        f.year("data").alias("ano"),
        f.quarter("data").alias("trimestre"),
        f.month("data").alias("mes"),
        f.element_at(nomes_mes, f.month("data")).alias("nome_mes"),
        f.dayofmonth("data").alias("dia"),
        dia_semana.alias("dia_semana"),
        f.element_at(nomes_dia, dia_semana).alias("nome_dia_semana"),
        f.weekofyear("data").alias("semana_iso"),
        (dia_semana >= 6).alias("fim_de_semana"),
        f.date_format("data", "yyyy-MM").alias("ano_mes"),
    )


TRANSFORMS = {
    "fato_vendas": t_fato_vendas,
    "dim_clientes": t_dim_clientes,
    "dim_produtos": t_dim_produtos,
    "dim_vendedores": t_dim_vendedores,
    "dim_empresas": t_dim_empresas,
    "dim_data": t_dim_data,
}

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### Leitura da Silver, transformação e gravação na Gold

# CELL ********************

if target_table not in TRANSFORMS:
    raise ValueError(f"Sem transformação para '{target_table}'. Tabelas conhecidas: {sorted(TRANSFORMS)}")

path_source = (
    f"abfss://{workspace_name}@onelake.dfs.fabric.microsoft.com/"
    f"{source_storage}.Lakehouse/Tables/dbo/{source_table}"
)
path_gold = (
    f"abfss://{workspace_name}@onelake.dfs.fabric.microsoft.com/"
    f"{target_storage}.Lakehouse/Tables/dbo/{target_table}"
)

df_source = spark.read.format("delta").load(path_source)
df_final = TRANSFORMS[target_table](df_source)

if target_mode == "merge":
    safe_merge(df_final, path_gold, target_key)
else:
    df_final.write.format("delta").mode(target_mode).option("overwriteSchema", "true").save(path_gold)

print(f"{target_table}: {spark.read.format('delta').load(path_gold).count()} linhas na Gold (modo {target_mode}).")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
