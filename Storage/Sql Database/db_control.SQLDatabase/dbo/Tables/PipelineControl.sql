CREATE TABLE [dbo].[PipelineControl] (
    [ControlID]       INT            IDENTITY (1, 1) NOT NULL,
    [PipelineLayer]   VARCHAR (50)   NOT NULL,
    [EntityName]      VARCHAR (100)  NOT NULL,
    [SourceDatabase]  VARCHAR (200)  NULL,
    [SourceSchema]    VARCHAR (50)   NULL,
    [SourceTable]     VARCHAR (200)  NULL,
    [SourceQuery]     VARCHAR (4000) NULL,
    [SourceStorage]   VARCHAR (200)  NULL,
    [SourceTables]    VARCHAR (500)  NULL,
    [TargetStorage]   VARCHAR (200)  NULL,
    [TargetFile]      VARCHAR (200)  NULL,
    [TargetFormat]    VARCHAR (20)   NULL,
    [TargetTable]     VARCHAR (200)  NULL,
    [TargetMode]      VARCHAR (50)   NULL,
    [TargetKey]       VARCHAR (200)  NULL,
    [Flag]            INT            NULL,
    [IsActive]        BIT            DEFAULT ((1)) NULL,
    [DataAtualizacao] DATETIME2 (7)  DEFAULT (sysutcdatetime()) NULL,
    [SourceFile]      VARCHAR (200)  NULL,
    [SourceFolder]    VARCHAR (500)  NULL,
    PRIMARY KEY CLUSTERED ([ControlID] ASC)
);


GO

