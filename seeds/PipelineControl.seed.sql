-- Seed repetível do db_control (dbo.PipelineControl).
-- Rodar quantas vezes quiser: o MERGE atualiza o que existe e cria o que falta, sem duplicar.
-- A chave é (PipelineLayer, EntityName). $(SourceDatabase) é trocado por tools/run_seed.py na hora de rodar;
-- o nome real do banco de origem NÃO fica no repositório.
SET NOCOUNT ON;

MERGE dbo.PipelineControl AS t
USING (VALUES
    (N'Staging', N'ENTIDADES', N'$(SourceDatabase)', N'dbo', N'ENTIDADES', N'SELECT * FROM dbo.ENTIDADES', N'lh_bronze', N'ENTIDADES|ENDERECOS|ESTADOS|CLASSIFICACOES_CLIENTES', N'Staging', N'entidades', N'.parquet', NULL, NULL, NULL, NULL, 1, NULL, NULL),
    (N'Staging', N'ENDERECOS', N'$(SourceDatabase)', N'dbo', N'ENDERECOS', N'SELECT * FROM dbo.ENDERECOS', N'lh_bronze', N'ENTIDADES|ENDERECOS|ESTADOS|CLASSIFICACOES_CLIENTES', N'Staging', N'enderecos', N'.parquet', NULL, NULL, NULL, NULL, 1, NULL, NULL),
    (N'Staging', N'ESTADOS', N'$(SourceDatabase)', N'dbo', N'ESTADOS', N'SELECT * FROM dbo.ESTADOS', N'lh_bronze', N'ENTIDADES|ENDERECOS|ESTADOS|CLASSIFICACOES_CLIENTES', N'Staging', N'estados', N'.parquet', NULL, NULL, NULL, NULL, 1, NULL, NULL),
    (N'Staging', N'CLASSIFICACOES_CLIENTES', N'$(SourceDatabase)', N'dbo', N'CLASSIFICACOES_CLIENTES', N'SELECT * FROM dbo.CLASSIFICACOES_CLIENTES', N'lh_bronze', N'ENTIDADES|ENDERECOS|ESTADOS|CLASSIFICACOES_CLIENTES', N'Staging', N'classificacoes_clientes', N'.parquet', NULL, NULL, NULL, NULL, 1, NULL, NULL),
    (N'Staging', N'VENDEDORES', N'$(SourceDatabase)', N'dbo', N'VENDEDORES', N'SELECT * FROM dbo.VENDEDORES', N'lh_bronze', NULL, N'Staging', N'vendedores', N'.parquet', NULL, NULL, NULL, NULL, 1, NULL, NULL),
    (N'Staging', N'PRODUTOS', N'$(SourceDatabase)', N'dbo', N'PRODUTOS', N'SELECT * FROM dbo.PRODUTOS', N'lh_bronze', N'PRODUTOS|GRUPOS_PRODUTOS|FAMILIAS_PRODUTOS|SECOES_PRODUTOS|SUBGRUPOS_PRODUTOS|MARCAS', N'Staging', N'produtos', N'.parquet', NULL, NULL, NULL, NULL, 1, NULL, NULL),
    (N'Staging', N'EMPRESAS_USUARIAS', N'$(SourceDatabase)', N'dbo', N'EMPRESAS_USUARIAS', N'SELECT * FROM dbo.EMPRESAS_USUARIAS', N'lh_bronze', NULL, N'Staging', N'empresas_usuarias', N'.parquet', NULL, NULL, NULL, NULL, 1, NULL, NULL),
    (N'Staging', N'VENDAS_ANALITICAS', N'$(SourceDatabase)', N'dbo', N'VENDAS_ANALITICAS', N'SELECT * FROM dbo.VENDAS_ANALITICAS', N'lh_bronze', NULL, N'Staging', N'vendas_analiticas', N'.parquet', NULL, NULL, NULL, NULL, 1, NULL, NULL),
    (N'Bronze', N'ENTIDADES', NULL, NULL, NULL, NULL, N'lh_bronze', NULL, N'lh_bronze', N'entidades.parquet', NULL, N'ENTIDADES', N'overwrite', NULL, NULL, 1, N'entidades.parquet', N'Staging'),
    (N'Bronze', N'ENDERECOS', NULL, NULL, NULL, NULL, N'lh_bronze', NULL, N'lh_bronze', N'enderecos.parquet', NULL, N'ENDERECOS', N'overwrite', NULL, NULL, 1, N'enderecos.parquet', N'Staging'),
    (N'Bronze', N'ESTADOS', NULL, NULL, NULL, NULL, N'lh_bronze', NULL, N'lh_bronze', N'estados.parquet', NULL, N'ESTADOS', N'overwrite', NULL, NULL, 1, N'estados.parquet', N'Staging'),
    (N'Bronze', N'CLASSIFICACOES_CLIENTES', NULL, NULL, NULL, NULL, N'lh_bronze', NULL, N'lh_bronze', N'classificacoes_clientes.parquet', NULL, N'CLASSIFICACOES_CLIENTES', N'overwrite', NULL, NULL, 1, N'classificacoes_clientes.parquet', N'Staging'),
    (N'Bronze', N'VENDEDORES', NULL, NULL, NULL, NULL, N'lh_bronze', NULL, N'lh_bronze', N'vendedores.parquet', NULL, N'VENDEDORES', N'overwrite', NULL, NULL, 1, N'vendedores.parquet', N'Staging'),
    (N'Bronze', N'PRODUTOS', NULL, NULL, NULL, NULL, N'lh_bronze', NULL, N'lh_bronze', N'produtos.parquet', NULL, N'PRODUTOS', N'overwrite', NULL, NULL, 1, N'produtos.parquet', N'Staging'),
    (N'Bronze', N'EMPRESAS_USUARIAS', NULL, NULL, NULL, NULL, N'lh_bronze', NULL, N'lh_bronze', N'empresas_usuarias.parquet', NULL, N'EMPRESAS_USUARIAS', N'overwrite', NULL, NULL, 1, N'empresas_usuarias.parquet', N'Staging'),
    (N'Bronze', N'VENDAS_ANALITICAS', NULL, NULL, NULL, NULL, N'lh_bronze', NULL, N'lh_bronze', N'vendas_analiticas.parquet', NULL, N'VENDAS_ANALITICAS', N'overwrite', NULL, NULL, 1, N'vendas_analiticas.parquet', N'Staging'),
    (N'Silver', N'ENTIDADES', NULL, NULL, N'ENTIDADES', NULL, N'lh_bronze', N'ENTIDADES|ENDERECOS|ESTADOS|CLASSIFICACOES_CLIENTES', N'lh_silver', NULL, NULL, N'dim_entidades', N'merge', N'ENTIDADE', 1, 1, NULL, NULL),
    (N'Silver', N'ENDERECOS', NULL, NULL, N'ENDERECOS', NULL, N'lh_bronze', N'ENTIDADES|ENDERECOS|ESTADOS|CLASSIFICACOES_CLIENTES', N'lh_silver', NULL, NULL, N'dim_enderecos', N'merge', N'ID_ENDERECO', 0, 1, NULL, NULL),
    (N'Silver', N'ESTADOS', NULL, NULL, N'ESTADOS', NULL, N'lh_bronze', N'ENTIDADES|ENDERECOS|ESTADOS|CLASSIFICACOES_CLIENTES', N'lh_silver', NULL, NULL, N'dim_estados', N'merge', N'ID_ESTADO', 0, 1, NULL, NULL),
    (N'Silver', N'CLASSIFICACOES_CLIENTES', NULL, NULL, N'CLASSIFICACOES_CLIENTES', NULL, N'lh_bronze', N'ENTIDADES|ENDERECOS|ESTADOS|CLASSIFICACOES_CLIENTES', N'lh_silver', NULL, NULL, N'dim_classificacoes_clientes', N'merge', N'ID_CLASSIFICACAO', 0, 1, NULL, NULL),
    (N'Silver', N'VENDEDORES', NULL, NULL, N'VENDEDORES', NULL, N'lh_bronze', NULL, N'lh_silver', NULL, NULL, N'dim_vendedores', N'merge', N'VENDEDOR', 1, 1, NULL, NULL),
    (N'Silver', N'PRODUTOS', NULL, NULL, N'PRODUTOS', NULL, N'lh_bronze', N'PRODUTOS|GRUPOS_PRODUTOS|FAMILIAS_PRODUTOS|SECOES_PRODUTOS|SUBGRUPOS_PRODUTOS|MARCAS', N'lh_silver', NULL, NULL, N'dim_produtos', N'merge', N'PRODUTO', 1, 1, NULL, NULL),
    (N'Silver', N'EMPRESAS_USUARIAS', NULL, NULL, N'EMPRESAS_USUARIAS', NULL, N'lh_bronze', NULL, N'lh_silver', NULL, NULL, N'dim_empresas_usuarias', N'merge', N'CodigoEmpresa', 0, 1, NULL, NULL),
    (N'Silver', N'VENDAS_ANALITICAS', NULL, NULL, N'VENDAS_ANALITICAS', NULL, N'lh_bronze', NULL, N'lh_silver', NULL, NULL, N'fato_vendas', N'merge', N'Cod_Venda', 1, 1, NULL, NULL),
    (N'Staging', N'FAMILIAS_PRODUTOS', N'$(SourceDatabase)', N'dbo', N'FAMILIAS_PRODUTOS', N'SELECT * FROM dbo.FAMILIAS_PRODUTOS', N'lh_bronze', N'PRODUTOS|GRUPOS_PRODUTOS|FAMILIAS_PRODUTOS|SECOES_PRODUTOS|SUBGRUPOS_PRODUTOS|MARCAS', N'Staging', N'familias_produtos', N'.parquet', NULL, NULL, NULL, NULL, 1, NULL, NULL),
    (N'Staging', N'SECOES_PRODUTOS', N'$(SourceDatabase)', N'dbo', N'SECOES_PRODUTOS', N'SELECT * FROM dbo.SECOES_PRODUTOS', N'lh_bronze', N'PRODUTOS|GRUPOS_PRODUTOS|FAMILIAS_PRODUTOS|SECOES_PRODUTOS|SUBGRUPOS_PRODUTOS|MARCAS', N'Staging', N'secoes_produtos', N'.parquet', NULL, NULL, NULL, NULL, 1, NULL, NULL),
    (N'Staging', N'GRUPOS_PRODUTOS', N'$(SourceDatabase)', N'dbo', N'GRUPOS_PRODUTOS', N'SELECT * FROM dbo.GRUPOS_PRODUTOS', N'lh_bronze', N'PRODUTOS|GRUPOS_PRODUTOS|FAMILIAS_PRODUTOS|SECOES_PRODUTOS|SUBGRUPOS_PRODUTOS|MARCAS', N'Staging', N'grupos_produtos', N'.parquet', NULL, NULL, NULL, NULL, 1, NULL, NULL),
    (N'Staging', N'SUBGRUPOS_PRODUTOS', N'$(SourceDatabase)', N'dbo', N'SUBGRUPOS_PRODUTOS', N'SELECT * FROM dbo.SUBGRUPOS_PRODUTOS', N'lh_bronze', N'PRODUTOS|GRUPOS_PRODUTOS|FAMILIAS_PRODUTOS|SECOES_PRODUTOS|SUBGRUPOS_PRODUTOS|MARCAS', N'Staging', N'subgrupos_produtos', N'.parquet', NULL, NULL, NULL, NULL, 1, NULL, NULL),
    (N'Staging', N'MARCAS', N'$(SourceDatabase)', N'dbo', N'MARCAS', N'SELECT * FROM dbo.MARCAS', N'lh_bronze', N'PRODUTOS|GRUPOS_PRODUTOS|FAMILIAS_PRODUTOS|SECOES_PRODUTOS|SUBGRUPOS_PRODUTOS|MARCAS', N'Staging', N'marcas', N'.parquet', NULL, NULL, NULL, NULL, 1, NULL, NULL),
    (N'Bronze', N'FAMILIAS_PRODUTOS', NULL, NULL, NULL, NULL, N'lh_bronze', NULL, N'lh_bronze', N'familias_produtos.parquet', NULL, N'FAMILIAS_PRODUTOS', N'overwrite', NULL, NULL, 1, N'familias_produtos.parquet', N'Staging'),
    (N'Bronze', N'SECOES_PRODUTOS', NULL, NULL, NULL, NULL, N'lh_bronze', NULL, N'lh_bronze', N'secoes_produtos.parquet', NULL, N'SECOES_PRODUTOS', N'overwrite', NULL, NULL, 1, N'secoes_produtos.parquet', N'Staging'),
    (N'Bronze', N'GRUPOS_PRODUTOS', NULL, NULL, NULL, NULL, N'lh_bronze', NULL, N'lh_bronze', N'grupos_produtos.parquet', NULL, N'GRUPOS_PRODUTOS', N'overwrite', NULL, NULL, 1, N'grupos_produtos.parquet', N'Staging'),
    (N'Bronze', N'SUBGRUPOS_PRODUTOS', NULL, NULL, NULL, NULL, N'lh_bronze', NULL, N'lh_bronze', N'subgrupos_produtos.parquet', NULL, N'SUBGRUPOS_PRODUTOS', N'overwrite', NULL, NULL, 1, N'subgrupos_produtos.parquet', N'Staging'),
    (N'Bronze', N'MARCAS', NULL, NULL, NULL, NULL, N'lh_bronze', NULL, N'lh_bronze', N'marcas.parquet', NULL, N'MARCAS', N'overwrite', NULL, NULL, 1, N'marcas.parquet', N'Staging'),
    (N'Gold', N'dim_clientes', NULL, NULL, N'dim_entidades', NULL, N'lh_silver', NULL, N'lh_gold', NULL, NULL, N'dim_clientes', N'merge', N'cod_cliente', 1, 1, NULL, NULL),
    (N'Gold', N'dim_produtos', NULL, NULL, N'dim_produtos', NULL, N'lh_silver', NULL, N'lh_gold', NULL, NULL, N'dim_produtos', N'merge', N'cod_produto', 1, 1, NULL, NULL),
    (N'Gold', N'dim_vendedores', NULL, NULL, N'dim_vendedores', NULL, N'lh_silver', NULL, N'lh_gold', NULL, NULL, N'dim_vendedores', N'merge', N'cod_vendedor', 1, 1, NULL, NULL),
    (N'Gold', N'dim_empresas', NULL, NULL, N'dim_empresas_usuarias', NULL, N'lh_silver', NULL, N'lh_gold', NULL, NULL, N'dim_empresas', N'merge', N'cod_empresa', 1, 1, NULL, NULL),
    (N'Gold', N'fato_vendas', NULL, NULL, N'fato_vendas', NULL, N'lh_silver', NULL, N'lh_gold', NULL, NULL, N'fato_vendas', N'merge', N'cod_venda', 1, 1, NULL, NULL),
    (N'Gold', N'dim_data', NULL, NULL, N'fato_vendas', NULL, N'lh_silver', NULL, N'lh_gold', NULL, NULL, N'dim_data', N'overwrite', N'data', 1, 1, NULL, NULL)
) AS s (PipelineLayer, EntityName, SourceDatabase, SourceSchema, SourceTable, SourceQuery, SourceStorage, SourceTables, TargetStorage, TargetFile, TargetFormat, TargetTable, TargetMode, TargetKey, Flag, IsActive, SourceFile, SourceFolder)
ON t.PipelineLayer = s.PipelineLayer AND t.EntityName = s.EntityName
WHEN MATCHED THEN UPDATE SET
    t.SourceDatabase = s.SourceDatabase,
    t.SourceSchema = s.SourceSchema,
    t.SourceTable = s.SourceTable,
    t.SourceQuery = s.SourceQuery,
    t.SourceStorage = s.SourceStorage,
    t.SourceTables = s.SourceTables,
    t.TargetStorage = s.TargetStorage,
    t.TargetFile = s.TargetFile,
    t.TargetFormat = s.TargetFormat,
    t.TargetTable = s.TargetTable,
    t.TargetMode = s.TargetMode,
    t.TargetKey = s.TargetKey,
    t.Flag = s.Flag,
    t.IsActive = s.IsActive,
    t.SourceFile = s.SourceFile,
    t.SourceFolder = s.SourceFolder,
    t.DataAtualizacao = SYSUTCDATETIME()
WHEN NOT MATCHED BY TARGET THEN INSERT (PipelineLayer, EntityName, SourceDatabase, SourceSchema, SourceTable, SourceQuery, SourceStorage, SourceTables, TargetStorage, TargetFile, TargetFormat, TargetTable, TargetMode, TargetKey, Flag, IsActive, SourceFile, SourceFolder)
    VALUES (s.PipelineLayer, s.EntityName, s.SourceDatabase, s.SourceSchema, s.SourceTable, s.SourceQuery, s.SourceStorage, s.SourceTables, s.TargetStorage, s.TargetFile, s.TargetFormat, s.TargetTable, s.TargetMode, s.TargetKey, s.Flag, s.IsActive, s.SourceFile, s.SourceFolder);
