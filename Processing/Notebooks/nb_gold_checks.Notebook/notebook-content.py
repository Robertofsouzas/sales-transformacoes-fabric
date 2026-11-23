# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# MARKDOWN ********************

# # nb_gold_checks: a "prova" da camada Gold
# Este notebook diz **como é uma Gold boa**. Ele foi escrito antes do Gold existir (como escrever a prova antes de estudar).
# Não tem parâmetros e funciona em qualquer ambiente (dev, feature, prod): descobre o workspace sozinho.
# Roda todas as verificações, mostra o resultado de cada uma e, no fim, **falha com `AssertionError`** se qualquer uma falhar.
# Verificações:
# 1. As 6 tabelas Gold existem e têm linhas.
# 2. Cada chave (fato e dimensões) não tem nulos nem repetições.
# 3. `fato_vendas` da Gold tem as mesmas linhas e o mesmo total de `venda_liquida` que a Silver (reconciliação).
# 4. Nenhuma venda "órfã": todo código do fato existe na dimensão correspondente.
# 5. `dim_data` cobre todas as datas das vendas e não tem buracos.

# CELL ********************

import sempy.fabric as fabric
from pyspark.sql import functions as f

workspace_name = fabric.resolve_workspace_name()

# Tabela Gold -> coluna chave
TABELAS_GOLD = {
    "fato_vendas": "cod_venda",
    "dim_clientes": "cod_cliente",
    "dim_produtos": "cod_produto",
    "dim_vendedores": "cod_vendedor",
    "dim_empresas": "cod_empresa",
    "dim_data": "data",
}

# Coluna do fato -> dimensão que deve conter o código
RELACOES = [
    ("cod_cliente", "dim_clientes"),
    ("cod_produto", "dim_produtos"),
    ("cod_vendedor", "dim_vendedores"),
    ("cod_empresa", "dim_empresas"),
]


def ler(lakehouse: str, tabela: str):
    caminho = (
        f"abfss://{workspace_name}@onelake.dfs.fabric.microsoft.com/"
        f"{lakehouse}.Lakehouse/Tables/dbo/{tabela}"
    )
    return spark.read.format("delta").load(caminho)


falhas = []


def registrar(ok: bool, mensagem: str):
    print(("OK    " if ok else "FALHA ") + mensagem)
    if not ok:
        falhas.append(mensagem)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### 1. As tabelas existem e têm linhas

# CELL ********************

gold = {}
for nome in TABELAS_GOLD:
    try:
        df = ler("lh_gold", nome)
        total = df.count()
        registrar(total > 0, f"{nome}: existe e tem linhas ({total})")
        if total > 0:
            gold[nome] = df
    except Exception as erro:
        registrar(False, f"{nome}: tabela inexistente ou ilegível ({type(erro).__name__})")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### 2. Chaves sem nulos e sem repetições

# CELL ********************

for nome, chave in TABELAS_GOLD.items():
    if nome not in gold:
        continue
    df = gold[nome]
    if chave not in df.columns:
        registrar(False, f"{nome}: coluna chave '{chave}' não existe")
        continue
    nulos = df.filter(f.col(chave).isNull()).count()
    repetidas = df.groupBy(chave).count().filter("count > 1").count()
    registrar(nulos == 0, f"{nome}.{chave}: sem nulos (encontrados: {nulos})")
    registrar(repetidas == 0, f"{nome}.{chave}: sem repetições (chaves repetidas: {repetidas})")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### 3. Reconciliação com a Silver (nada se perde no caminho)

# CELL ********************

if "fato_vendas" in gold:
    try:
        silver = ler("lh_silver", "fato_vendas")
        g = gold["fato_vendas"]
        registrar(
            g.count() == silver.count(),
            f"fato_vendas: linhas Gold ({g.count()}) = linhas Silver ({silver.count()})",
        )
        soma_silver = silver.agg(f.sum("Venda_Liquida")).first()[0]
        soma_gold = g.agg(f.sum("venda_liquida")).first()[0]
        registrar(
            soma_silver == soma_gold,
            f"fato_vendas: soma de venda_liquida Gold ({soma_gold}) = Silver ({soma_silver})",
        )
    except Exception as erro:
        registrar(False, f"reconciliação com a Silver não pôde rodar ({type(erro).__name__}: {erro})")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### 4. Nenhuma venda órfã (todo código existe na dimensão)
# Vendas sem vendedor entram com código 0; por isso `dim_vendedores` precisa ter o membro 0 ("Sem vendedor").

# CELL ********************

if "fato_vendas" in gold:
    fato = gold["fato_vendas"]
    for coluna, dimensao in RELACOES:
        if dimensao not in gold or coluna not in fato.columns:
            registrar(False, f"fato_vendas.{coluna}: não dá para verificar ({dimensao} ou a coluna não existe)")
            continue
        orfas = fato.join(gold[dimensao].select(coluna).distinct(), on=coluna, how="left_anti").count()
        registrar(orfas == 0, f"fato_vendas.{coluna} -> {dimensao}: vendas órfãs = {orfas}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### 5. O calendário cobre todas as vendas e não tem buracos

# CELL ********************

if "dim_data" in gold and "fato_vendas" in gold:
    mov = gold["fato_vendas"].select(f.to_date("movimento").alias("d")).agg(f.min("d"), f.max("d")).first()
    cal = gold["dim_data"].agg(f.min("data"), f.max("data"), f.count("data")).first()
    registrar(cal[0] <= mov[0], f"dim_data começa ({cal[0]}) antes ou na primeira venda ({mov[0]})")
    registrar(cal[1] >= mov[1], f"dim_data termina ({cal[1]}) depois ou na última venda ({mov[1]})")
    dias_esperados = (cal[1] - cal[0]).days + 1
    registrar(cal[2] == dias_esperados, f"dim_data sem buracos: {cal[2]} dias para um intervalo de {dias_esperados}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Resultado final

# CELL ********************

print(f"\n{len(falhas)} verificação(ões) com falha.")
assert not falhas, f"{len(falhas)} verificação(ões) falharam:\n- " + "\n- ".join(falhas)
print("Todas as verificações passaram.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
