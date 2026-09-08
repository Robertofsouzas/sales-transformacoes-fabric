# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "environment": {
# META       "environmentId": "cb602006-de0a-b904-437b-8bb8c37164df",
# META       "workspaceId": "00000000-0000-0000-0000-000000000000"
# META     }
# META   }
# META }

# CELL ********************

# Bibliotecas Necessárias
import sempy.fabric as fabric

import sempy_labs as labs
import sempy_labs.lakehouse as lake

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# obtençao do workspace
workspace = fabric.resolve_workspace_name()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Parametros

lakehouse = "lh_silver"
table  = "dbo/Sales"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Captura as analises da tabela Delta e armazena em Dataframes

x = labs.delta_analyzer(
    table_name =  table,
    lakehouse = lakehouse,
    workspace = workspace
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark",
# META   "frozen": true,
# META   "editable": false
# META }

# CELL ********************

# Exibe os resultados do Delta Aanalyzer
for name,df in x.items():
    print(name)
    display(df)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark",
# META   "frozen": true,
# META   "editable": false
# META }

# CELL ********************

# Executa Optimize em todas as tabelas do lakehouse

lake.optimize_lakehouse_tables(
    lakehouse = lakehouse,
    workspace = workspace
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Executa vacuum em todas as tabelas do lakehouse

lake.vacuum_lakehouse_tables(
    lakehouse = lakehouse,
    workspace = workspace
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Executa todas as otimizações Delta para uma tabela especifica
lake.run_table_maintenance(
    table_name = table,
    optimize= True,
    v_order= True,
    schema = "dbo",
    vacuum= True,
    lakehouse = lakehouse,
    workspace = workspace
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark",
# META   "frozen": true,
# META   "editable": false
# META }
