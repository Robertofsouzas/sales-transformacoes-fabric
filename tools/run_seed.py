"""Aplica o seed do db_control (dbo.PipelineControl) de forma repetível.

Uso:
    from tools.run_seed import run_seed
    total = run_seed("ws_sales_feature_roberto", "<nome do banco de origem>")

- Lê seeds/PipelineControl.seed.sql e troca $(SourceDatabase) pelo banco de origem informado.
- Autentica com o token do Azure CLI (https://database.windows.net/); o token nunca é impresso.
- O db_control pausa sozinho: a conexão é repetida enquanto ele acorda.
- Devolve o total de linhas de dbo.PipelineControl depois do seed.
"""
import json
import re
import struct
import subprocess
import time
import urllib.request
from pathlib import Path

AZ = r"C:\Program Files (x86)\Microsoft SDKs\Azure\CLI2\wbin\az.cmd"
SEED = Path(__file__).resolve().parent.parent / "seeds" / "PipelineControl.seed.sql"
FABRIC = "https://api.fabric.microsoft.com/v1"


def _token(resource: str) -> str:
    return subprocess.check_output(
        [AZ, "account", "get-access-token", "--resource", resource, "--query", "accessToken", "-o", "tsv"],
        text=True,
    ).strip()


def _api(path: str, tok: str) -> dict:
    req = urllib.request.Request(FABRIC + path, headers={"Authorization": "Bearer " + tok})
    return json.load(urllib.request.urlopen(req))


def _connect(workspace_name: str):
    import pyodbc  # importado aqui para o módulo carregar mesmo sem pyodbc

    ftok = _token("https://api.fabric.microsoft.com")
    wid = next((w["id"] for w in _api("/workspaces", ftok)["value"] if w["displayName"] == workspace_name), None)
    if wid is None:
        raise ValueError(f"Workspace '{workspace_name}' não encontrado")
    items = _api(f"/workspaces/{wid}/items", ftok)["value"]
    db = next((i for i in items if i["displayName"] == "db_control" and i["type"] == "SQLDatabase"), None)
    if db is None:
        raise ValueError(f"db_control não encontrado em '{workspace_name}'")
    props = _api(f"/workspaces/{wid}/sqlDatabases/{db['id']}", ftok)["properties"]

    raw = _token("https://database.windows.net/").encode("utf-16-le")
    tokstruct = struct.pack("<I", len(raw)) + raw
    cs = (f"Driver={{ODBC Driver 18 for SQL Server}};Server={props['serverFqdn']};"
          f"Database={props['databaseName']};Encrypt=yes;TrustServerCertificate=no")
    for tentativa in range(1, 9):
        try:
            return pyodbc.connect(cs, attrs_before={1256: tokstruct}, timeout=60)
        except Exception as e:  # o banco pausado responde "not currently available"
            if "not currently available" not in str(e) or tentativa == 8:
                raise
            time.sleep(20)


def run_seed(workspace_name: str, source_database: str) -> int:
    if not re.fullmatch(r"[A-Za-z0-9_\-]+", source_database):
        raise ValueError("source_database deve ter só letras, números, _ ou -")
    sql = SEED.read_text(encoding="utf-8").replace("$(SourceDatabase)", source_database)
    cn = _connect(workspace_name)
    try:
        cur = cn.cursor()
        cur.execute(sql)
        cn.commit()
        cur.execute("SELECT COUNT(*) FROM dbo.PipelineControl")
        return int(cur.fetchone()[0])
    finally:
        cn.close()
