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

# Instação do Semnatic Labs
%pip install semantic-link-labs


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark",
# META   "frozen": true,
# META   "editable": false
# META }

# CELL ********************

# Biblioteca Necessarias
import sempy.fabric as fabric

import sempy_labs as labs
from sempy_labs import directlake

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

# PARAMETERS CELL ********************

# Parâmetros
semantic_model = "sm_solucoes-fabric_composite"
source_storage = "lh_gold"
source_type    = "lakehouse"
tables         = "DimCustomers;DimProducts;FactSales"
flag           = True


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Atualização da expressão de conexão

if flag:
    directlake.update_direct_lake_model_connection(
        dataset = semantic_model,
        workspace = workspace,
        source = source_storage,
        source_type = source_type,
        source_workspace = workspace,
        use_sql_endpoint = False,
        tables = tables.split(";")
    )
else:
    directlake.update_direct_lake_model_connection(
        dataset = semantic_model,
        workspace = workspace,
        source = source_storage,
        source_type = source_type,
        source_workspace = workspace,
        use_sql_endpoint = False
    )

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
