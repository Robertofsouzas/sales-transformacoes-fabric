# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse_name": "",
# META       "default_lakehouse_workspace_id": ""
# META     }
# META   }
# META }

# CELL ********************

# Configuraões da sessão spark
spark.conf.set("spark.sql.caseSensitive",True)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# obtenção do workspace id e do workspace name

import sempy.fabric as fabric
worspace_id = fabric.get_workspace_id()
workspace_name = fabric.resolve_workspace_name()


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark",
# META   "frozen": false,
# META   "editable": true
# META }

# PARAMETERS CELL ********************

# Parâmetros passados pelo pipeline ( obtidos a partir do orquestrador)

      
      
source_storage= ""
source_folder=""
source_file=""

target_storage= ""
target_table= ""
target_mode= ""


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


# Caminho absoluto para a origem ( arquivos parquet)
path_staging=f"abfss://{workspace_name}@onelake.dfs.fabric.microsoft.com/{source_storage}.Lakehouse/Files/{source_folder}/{source_file}"

# Caminho absoluto para o detino (tabelas Deltas)
path_bronze = f"abfss://{workspace_name}@onelake.dfs.fabric.microsoft.com/{target_storage}.Lakehouse/Tables/dbo/{target_table}"


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Leitura do arquivo parquet na staging
df = spark.read.parquet(path_staging)


# Escrita da Tabela Delta na camada Bronze
df.write.format("delta").mode(target_mode).save(path_bronze)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
