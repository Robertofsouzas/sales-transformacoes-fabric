# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# MARKDOWN ********************

# ####  Configurações da sessão spark

# CELL ********************

%run nb_functions

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# configurações da sessão spark
spark.conf.set("spark.sql.casesensitive",True)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### Bibliotecas Necessárias 

# CELL ********************

# Bibliotecas Necessárias 
import sempy.fabric as fabric
from pyspark.sql import functions as f

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### Obtenção do wokspace id e workspace name

# CELL ********************

# Obtenção do wokspace id e do workspace name

workspace_id  = fabric.get_notebook_workspace_id()
workspace_name = fabric.resolve_workspace_name()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### Parâmetros passados pelo pipeline ( Obtidos a partir do Orquestrador)

# PARAMETERS CELL ********************

# Parâmetros passados pelo pipeline ( Obtidos a partir do Orquestrador)
source_storage = "lh_bronze"
source_tables  = "PRODUTOS|GRUPOS_PRODUTOS|FAMILIAS_PRODUTOS|SECOES_PRODUTOS|SUBGRUPOS_PRODUTOS|MARCAS"


target_storage = "lh_silver"
target_table   = "PRODUTOS"
target_mode    = "merge"   # opção : overwrite , Append e Merge

target_key    = "ID_PRODUTO"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### Caminho absoluto para o destino ( tabela delta)

# CELL ********************

# Caminho absoluto para o destino ( tabela delta)

path_silver = (
    f"abfss://{workspace_name}@onelake.dfs.fabric.microsoft.com/"
    f"{target_storage}.Lakehouse/Tables/dbo/{target_table}"

)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### Leitura das tabelas

# CELL ********************

# Leitura das tabelas

# Split das tabelas em lista para interação
source_tables = source_tables.split("|")
df={}


# Iterador
for table in source_tables:
    path_bronze = (
        f"abfss://{workspace_name}@onelake.dfs.fabric.microsoft.com/"
        f"{source_storage}.Lakehouse/Tables/dbo/{table}"
    )
    # Faz a leitura e armazena cada tabela em seu respectivo Dataframe
    df[table] = spark.read.format("delta").load(path_bronze)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### seleção de colunas e linhas

# CELL ********************

df_product = (
    df["PRODUTOS"]
    .select(
        "PRODUTO",
        "DESCRICAO",
        "DESCRICAO_REDUZIDA",
        "FAMILIA_PRODUTO",
        "SECAO_PRODUTO",
        "GRUPO_PRODUTO",
        "SUBGRUPO_PRODUTO",
        "MARCA"
    )

    
)

df_group = (
    df["GRUPOS_PRODUTOS"]
    .select(
        "GRUPO_PRODUTO",
        f.col("DESCRICAO").alias("GRUPO_DESCRICAO")
    )
)

df_family = (
    df["FAMILIAS_PRODUTOS"]
    .select(
        "FAMILIA_PRODUTO",
        f.col("DESCRICAO").alias("FAMILIA_DESCRICAO")
    )
)




df_section = (
    df["SECOES_PRODUTOS"]
    .select(
        "SECAO_PRODUTO",
        f.col("DESCRICAO").alias("SECAO_DESCRICAO")
    )
)

df_subgroup = (
    df["SUBGRUPOS_PRODUTOS"]
    .select(
        "SUBGRUPO_PRODUTO",
        f.col("DESCRICAO").alias("SUBGRUPO_DESCRICAO")
    )
)


df_brand = (
    df["MARCAS"]
    .select(
        "MARCA",
        f.col("DESCRICAO").alias("MARCA_DESCRICAO")
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### Criação do Dataframe principal

# CELL ********************

# Criação do Dataframe principal

df_final = (
    df_product.alias("p")

    .join(
        df_group.alias("g"),
        f.col("p.GRUPO_PRODUTO") == f.col("g.GRUPO_PRODUTO"),
        "left"
    )

    .join(
        df_family.alias("f"),
        f.col("p.FAMILIA_PRODUTO") == f.col("f.FAMILIA_PRODUTO"),
        "left"
    )

    .join(
        df_section.alias("s"),
        f.col("p.SECAO_PRODUTO") == f.col("s.SECAO_PRODUTO"),
        "left"
    )

    .join(
        df_subgroup.alias("sg"),
        f.col("p.SUBGRUPO_PRODUTO") == f.col("sg.SUBGRUPO_PRODUTO"),
        "left"
    )

    .join(
        df_brand.alias("b"),
        f.col("p.MARCA") == f.col("b.MARCA"),
        "left"
    )

    .select(
        f.col("p.PRODUTO"),
        f.col("p.DESCRICAO"),
        f.col("p.DESCRICAO_REDUZIDA"),
        f.coalesce(
            f.col("f.FAMILIA_DESCRICAO"),
            f.lit("ADEFINIR")
        ).alias("FAMILIA_PRODUTO"),
        f.col("s.SECAO_DESCRICAO").alias("SECAO_PRODUTO"),
        f.col("g.GRUPO_DESCRICAO").alias("GRUPO_PRODUTO"),
        f.col("sg.SUBGRUPO_DESCRICAO").alias("SUBGRUPO_PRODUTO"),
        f.col("b.MARCA_DESCRICAO").alias("MARCA")
    )
).withColumn(
        "DataCarga",
        f.current_timestamp()
    )

 
    

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Gravação da tabela de acordo com o modo atribuído

# CELL ********************

# Gravação da tabela de acordo com o modo atribuído
if target_mode == "merge":
    safe_merge(df_final, path_silver,target_key)

else:
    df_final.write.format("delta").mode(target_mode).save(path)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
