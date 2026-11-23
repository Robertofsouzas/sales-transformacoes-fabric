# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# MARKDOWN ********************

# # nb_delta_optimization: manutencao das tabelas Delta de um lakehouse
# # Para cada tabela do lakehouse: **OPTIMIZE** (junta arquivos pequenos) com **V-Order** (melhora a leitura do Direct Lake)
# e **VACUUM** (apaga arquivos antigos que nenhuma versao atual usa; retencao padrao de 7 dias).
# # Usa so Spark SQL nativo: nao depende de biblioteca extra nem de Environment publicado (funciona igual em qualquer ambiente).
# Uma tabela com problema nao impede as outras; no fim o notebook falha mostrando quais falharam.

# CELL ********************

import sempy.fabric as fabric

workspace = fabric.resolve_workspace_name()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# PARAMETERS CELL ********************

# Parametros passados pelo pipeline (o lakehouse a manter)
lakehouse = "lh_gold"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

pasta = f"abfss://{workspace}@onelake.dfs.fabric.microsoft.com/{lakehouse}.Lakehouse/Tables/dbo"
tabelas = sorted(item.name for item in notebookutils.fs.ls(pasta) if item.isDir)
print(f"{lakehouse}: {len(tabelas)} tabelas -> {tabelas}")


def detalhe(caminho):
    d = spark.sql(f"DESCRIBE DETAIL delta.`{caminho}`").first()
    return d["numFiles"], d["sizeInBytes"]


falhas = []
for nome in tabelas:
    caminho = f"{pasta}/{nome}"
    try:
        antes = detalhe(caminho)
        spark.sql(f"OPTIMIZE delta.`{caminho}` VORDER")
        spark.sql(f"VACUUM delta.`{caminho}`")
        depois = detalhe(caminho)
        print(f"OK    {nome}: arquivos {antes[0]} -> {depois[0]}, bytes {antes[1]} -> {depois[1]}")
    except Exception as erro:
        print(f"FALHA {nome}: {type(erro).__name__}: {erro}")
        falhas.append(nome)

assert not falhas, f"manutencao falhou em: {falhas}"
print("Manutencao concluida.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
