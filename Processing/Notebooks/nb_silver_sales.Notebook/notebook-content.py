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
from pyspark.sql import Window as W
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
source_tables  = "VENDAS_ANALITICAS"


target_storage = "lh_silver"
target_table   = "fato_vendas"
target_mode    = "merge"   # opção : overwrite , Append e Merge

target_key = "VENDA_ANALITICA"
    

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

table = "VENDAS_ANALITICAS"

path_bronze = (
    f"abfss://{workspace_name}@onelake.dfs.fabric.microsoft.com/"
    f"{source_storage}.Lakehouse/Tables/dbo/{table}"
)

df_vendas_analiticas = spark.read.format("delta").load(path_bronze)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

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
# META   "language_group": "synapse_pyspark",
# META   "frozen": true,
# META   "editable": false
# META }

# MARKDOWN ********************

# #### seleção de colunas e linhas

# CELL ********************



df_final = (
    df_vendas_analiticas
    .select(
    
        f.col("VENDA_ANALITICA").alias("Cod_Venda"),
        f.coalesce(f.col("DOCUMENTO_NUMERO"), f.lit(0)).alias("N_Doc"),
        f.col("EMPRESA").alias("Cod_Empresa"),
        f.col("PRODUTO").alias("Cod_produto"),
        f.coalesce(f.col("VENDEDOR"), f.lit(0)).alias("Cod_Vendedor"),
        f.col("CLIENTE").alias("cod_Cliente"),
        f.col("MOVIMENTO"),
        f.col("QUANTIDADE").alias("Quantidade"),
        f.col("VENDA_BRUTA").alias("Venda_Bruta"),
        (
            f.coalesce(f.col("DESCONTO"), f.lit(0))
            + f.coalesce(f.col("DESCONTO_NEGOCIADO"), f.lit(0))
        ).alias("Desconto"),
        f.col("VENDA_LIQUIDA")
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



# 1. Transformação do DataFrame
df_final = (
    df_vendas_analiticas
    .select(
        f.col("VENDA_ANALITICA").alias("Cod_Venda"),
        f.coalesce(f.col("DOCUMENTO_NUMERO"), f.lit(0)).alias("N_Doc"),
        f.col("EMPRESA").alias("Cod_Empresa"),
        f.col("PRODUTO").alias("Cod_produto"),
        f.coalesce(f.col("VENDEDOR"), f.lit(0)).alias("Cod_Vendedor"),
        f.col("CLIENTE").alias("cod_Cliente"),
        f.col("MOVIMENTO"),
        f.col("QUANTIDADE").alias("Quantidade"),
        f.col("VENDA_BRUTA").alias("Venda_Bruta"),
        (
            f.coalesce(f.col("DESCONTO"), f.lit(0))
            + f.coalesce(f.col("DESCONTO_NEGOCIADO"), f.lit(0))
        ).alias("Desconto"),
        f.col("VENDA_LIQUIDA").alias("Venda_Liquida")
    )
    .withColumn("DataCarga", f.current_timestamp())
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
    df_final.write.format("delta").mode(target_mode).save(path_silver)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
