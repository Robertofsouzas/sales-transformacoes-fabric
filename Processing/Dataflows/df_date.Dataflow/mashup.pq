[StagingDefinition = [Kind = "FastCopy"]]
section Section1;
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "Date_DataDestination", IsNewTarget = true], Settings = [Kind = "Automatic", TypeSettings = [Kind = "Table"]]]}]
shared Date = let
  Origem = List.Dates(StartDate,Duration.TotalDays(EndDate-StartDate)+1,#duration(1,0,0,0)),
  Table = Table.FromList(Origem, Splitter.SplitByNothing(),type table[Date = Date.Type],null,ExtraValues.Error),
  Year = Table.AddColumn(Table, "Ano", each Date.Year([Date]),Int64.Type),
  Quarter = Table.AddColumn(Year, "Trimestre", each Date.QuarterOfYear([Date]), Int64.Type),
  QuaterName = Table.AddColumn(Quarter, "QuarterName", each "Q" & Text.From([Trimestre]), type text ),
  Month = Table.AddColumn(QuaterName, "Mês", each Date.Month([Date]), Int64.Type),
  #"Nome do mês" = Table.AddColumn(Month, "Nome do mês", each Date.MonthName([Date]), type text),
  MerAbreviado = Table.AddColumn(#"Nome do mês", "Mês abrev", each Text.Start(Text.Proper([Nome do mês]), 3), type text),
  #"Dia inserido" = Table.AddColumn(MerAbreviado, "Dia", each Date.Day([Date]), Int64.Type),
  #"Dia da semana inserido" = Table.AddColumn(#"Dia inserido", "Dia da semana", each Date.DayOfWeek([Date]), Int64.Type),
  #"Nome do dia inserido" = Table.AddColumn(#"Dia da semana inserido", "Nome do dia", each Date.DayOfWeekName([Date]), type text)
in
  #"Nome do dia inserido";
shared StartDate = #date(2008, 1, 1) meta [IsParameterQuery = true, IsParameterQueryRequired = false, Type = type date, DefaultValue = null];
shared EndDate = #date(2008, 12, 31) meta [IsParameterQuery = true, IsParameterQueryRequired = false, Type = type date, DefaultValue = null];
shared Date_DataDestination = let
  Pattern = Lakehouse.Contents([CreateNavigationProperties = false, EnableFolding = false, HierarchicalNavigation = null, EnableVorder = true, OutputMetadataRefresh = true]),
  Navigation_1 = Pattern{[workspaceId = "8d2be10a-d856-475a-a75b-a482c808d9c0"]}[Data],
  Navigation_2 = Navigation_1{[lakehouseId = "e6db21e6-2469-49d9-8948-f719427a7090"]}[Data],
  TableNavigation = Navigation_2{[Id = "DimDate", ItemKind = "Table"]}?[Data]?
in
  TableNavigation;
