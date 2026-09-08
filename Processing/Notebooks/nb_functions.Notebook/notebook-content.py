# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# CELL ********************

# Função para validar existencia de tabelas delta
def table_exists(path: str) -> bool:
 from pyspark.sql.utils import AnalysisException
 try:
    spark.read.format("delta").load(path)
    return True
 except AnalysisException:
        return False


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark",
# META   "frozen": true,
# META   "editable": false
# META }

# CELL ********************

# Funçao para executar o merge de forma segura

def safe_merge(source,path, Key):

    if table_exists(path_silver):
        # Executar o merge
        print("Tabela encontrada, merge em execução")
        from delta.tables import DeltaTable
        
        # variáveis necessárias ao merge
        target = DeltaTable.forPath(spark, path_silver)
        columns = source.columns
        update_cols = {col_name: f"source.{col_name}" for col_name in columns}
        update_condition = "source.ModifiedDate > target.ModifiedDate"
        merge_condition = f"target.{Key} = source.{Key}"

        # instrução Merge

        (
            target.alias("target")
            .merge(source=  source.alias("source"),condition= merge_condition)
            .whenMatchedUpdate(condition= update_condition , set =update_cols)
            .whenNotMatchedInsert(values= update_cols)
            .execute()
        )

    else:
        # Executar o overwrite
        source.write.format("delta").mode("overwrite").save(path_silver)    

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark",
# META   "frozen": true,
# META   "editable": false
# META }

# CELL ********************

# Célula 1: Função de validação de tabela Delta
from delta.tables import DeltaTable

def table_exists(path: str) -> bool:
    """
    Verifica se o caminho informado corresponde a uma tabela Delta existente.
    """
    try:
        return DeltaTable.isDeltaTable(spark, path)
    except Exception:
        return False

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Célula 2: Função genérica para Merge ou Carga Inicial
from delta.tables import DeltaTable
from typing import Union, List

def safe_merge(source, path: str, key: Union[str, List[str]], date_col: str = None):
    """
    Executa MERGE em tabela Delta existente ou OVERWRITE se ela ainda não existir.
    
    :param source: DataFrame Spark com os novos dados
    :param path: Caminho OneLake da tabela Delta de destino
    :param key: Coluna(s) de negócio para chave do merge (str, ou str com vírgulas
                para chave composta vinda do pipeline, ou list de str)
    :param date_col: (Opcional) Nome da coluna de timestamp/data para validar se houve atualização
    """
    # Normaliza: parâmetro vindo do pipeline sempre chega como string.
    # Se vier com vírgula ("N_Doc,Cod_Empresa"), transforma em lista de verdade.
    if isinstance(key, str):
        key = [k.strip() for k in key.split(",")]

    if table_exists(path):
        print(f"Tabela encontrada em '{path}'. Executando merge...")
        target = DeltaTable.forPath(spark, path)

        # Monta a condição de junção para chave simples ou composta
        merge_condition = " AND ".join([f"target.{k} = source.{k}" for k in key])

        # Mapeia colunas da origem para destino
        columns = source.columns
        update_cols = {col_name: f"source.{col_name}" for col_name in columns}

        # Constrói o comando de merge
        builder = (
            target.alias("target")
            .merge(source=source.alias("source"), condition=merge_condition)
        )

        # Se você informar uma coluna de data e ela existir no DataFrame, usa na condição
        if date_col and date_col in columns:
            update_cond = f"source.{date_col} > target.{date_col}"
            builder = builder.whenMatchedUpdate(condition=update_cond, set=update_cols)
        else:
            # Caso contrário, atualiza normalmente sem exigir coluna de data
            builder = builder.whenMatchedUpdate(set=update_cols)

        (
            builder
            .whenNotMatchedInsert(values=update_cols)
            .execute()
        )
        print("Merge concluído com sucesso!")

    else:
        print(f"Tabela não encontrada em '{path}'. Executando overwrite inicial...")
        source.write.format("delta").mode("overwrite").save(path)
        print("Tabela inicial criada com sucesso!")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
