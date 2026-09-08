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
source_tables  = "ENTIDADES|CLASSIFICACOES_CLIENTES|ENDERECOS|ESTADOS"


target_storage = "lh_silver"
target_table   = "dim_entidades"
target_mode    = "merge"   # opção : overwrite , Append e Merge

target_key    = "ENTIDADE"

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

# seleção de colunas e linhas

df_ENTIDADES = ( 
    df['ENTIDADES']
    .select(
            "ENTIDADE",
            "NOME",
            "NOME_FANTASIA",
            "CLASSIFICACAO_CLIENTE"
            )

)

df_CLASSIFICACOES_CLIENTES = (
    df['CLASSIFICACOES_CLIENTES']
    .select(
            
            "CLASSIFICACAO_CLIENTE",
              "DESCRICAO"
           
    )
)

df_ENDERECOS = (
    df['ENDERECOS']
    .select(
        
        "CIDADE",
        "ESTADO",
        "ENTIDADE"
    ) 
)


df_ESTADOS = (
    df['ESTADOS']
    .select(
        
        
            "ESTADO",
             "NOME"

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

# Fazendo os joins


df_final = (
    df_ENTIDADES.alias("e")
    .join(
        df_ENDERECOS.alias("en"),
        f.col("e.ENTIDADE") == f.col("en.ENTIDADE"),
        "left"
    )
    .join(
        df_ESTADOS.alias("es"),
        f.col("en.ESTADO") == f.col("es.ESTADO"),
        "left"
    )
    .join(
        df_CLASSIFICACOES_CLIENTES.alias("cc"),
        f.col("e.CLASSIFICACAO_CLIENTE") == f.col("cc.CLASSIFICACAO_CLIENTE"),
        "left"
    )
    .select(
        f.col("e.ENTIDADE"),   
        f.initcap(f.col("e.NOME")).alias("Nome"),
        f.initcap(f.col("e.NOME_FANTASIA")).alias("Nome_Fantasia"),


        f.coalesce(
            f.initcap(f.col("cc.DESCRICAO")),
            f.lit("Não Informado")
        ).alias("Nome_Classificacao"),

        f.coalesce(
            f.initcap(f.col("en.CIDADE")),
            f.lit("Não Informado")    
        ).alias("Cidade"),

        f.coalesce(
            f.initcap(f.col("es.NOME")),
            f.lit("Não Informado"),
            ).alias("Estado"),


         f.coalesce(
            f.col("es.ESTADO"),
            f.lit("Não Informado"),
            ).alias("UF")
        
    ))
    #adicionar colunas de controle
df_final = (
    df_final
    .withColumn("Data_Carga", f.current_timestamp())
    
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
