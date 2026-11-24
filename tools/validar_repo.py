"""Verificador do repositório: roda em todo pull request (workflow validar-pr).

Cada regra recebe a raiz do repositório e devolve uma lista de problemas no
formato "R<n> <caminho com />:<linha>: <mensagem>" (linha 0 = arquivo inteiro).

Uso:
    python tools/validar_repo.py .          # sai com 1 se houver problema
"""
from __future__ import annotations

import json
import re
import sys
from fnmatch import fnmatch
from pathlib import Path
from typing import Callable, Iterator

GUID_ZERADO = "00000000-0000-0000-0000-000000000000"
PASTAS_IGNORADAS = {".git", ".superpowers", "__pycache__", ".venv", "node_modules"}
EXTENSOES_BINARIAS = {".png", ".jpg", ".jpeg", ".gif", ".ico", ".pdf", ".zip", ".pyc", ".parquet"}


# ---------------------------------------------------------------- utilidades
def _arquivos(raiz: Path) -> Iterator[Path]:
    for caminho in sorted(raiz.rglob("*")):
        if not caminho.is_file():
            continue
        if any(parte in PASTAS_IGNORADAS for parte in caminho.relative_to(raiz).parts):
            continue
        yield caminho


def _arquivos_texto(raiz: Path) -> Iterator[tuple[Path, str]]:
    for caminho in _arquivos(raiz):
        if caminho.suffix.lower() in EXTENSOES_BINARIAS:
            continue
        try:
            yield caminho, caminho.read_text(encoding="utf-8-sig")
        except (UnicodeDecodeError, OSError):
            continue  # binário sem extensão conhecida


def _rel(raiz: Path, caminho: Path) -> str:
    return caminho.relative_to(raiz).as_posix()


def _linha(texto: str, posicao: int) -> int:
    return texto.count("\n", 0, posicao) + 1


def _problema(regra: str, raiz: Path, caminho: Path, linha: int, mensagem: str) -> str:
    return f"{regra} {_rel(raiz, caminho)}:{linha}: {mensagem}"


# ------------------------------------------------------------ R1: segredos
# Cada padrão exige um VALOR depois da palavra, para não reprovar um texto que
# só menciona "password" ou "token" (como o README).
PADROES_SEGREDO: list[tuple[str, re.Pattern]] = [
    ("client secret", re.compile(r"client[_-]?secret\s*[=:]\s*['\"]?[^\s'\"]{8,}", re.I)),
    ("senha", re.compile(r"\b(?:password|pwd)\s*=\s*['\"]?[^\s;'\"$]{6,}", re.I)),
    ("token Bearer", re.compile(r"\bBearer\s+[A-Za-z0-9._~+/-]{20,}")),
    ("chave de conta de armazenamento", re.compile(r"AccountKey=[A-Za-z0-9+/=]{20,}")),
    ("assinatura SAS", re.compile(r"SharedAccessSignature\s*=|[?&]sig=[A-Za-z0-9%+/=]{16,}")),
    ("chave privada", re.compile(r"-----BEGIN (?:[A-Z]+ )?PRIVATE KEY-----")),
]


def regra_r1(raiz: Path) -> list[str]:
    problemas = []
    for caminho, texto in _arquivos_texto(raiz):
        for nome, padrao in PADROES_SEGREDO:
            for achado in padrao.finditer(texto):
                problemas.append(_problema("R1", raiz, caminho, _linha(texto, achado.start()),
                                           f"possível {nome} no arquivo (nunca commitar segredo)"))
    return problemas


# ------------------------------------- R5: seed sem nome real do banco de origem
TUPLA_STAGING = re.compile(r"\(\s*N'Staging'\s*,\s*N'[^']*'\s*,\s*(N'[^']*'|NULL)", re.I)
MARCADOR_BANCO = "N'$(SourceDatabase)'"


def regra_r5(raiz: Path) -> list[str]:
    problemas = []
    pasta = raiz / "seeds"
    if not pasta.is_dir():
        return problemas
    for caminho in sorted(pasta.glob("*.sql")):
        texto = caminho.read_text(encoding="utf-8-sig")
        for achado in TUPLA_STAGING.finditer(texto):
            valor = achado.group(1)
            if valor != MARCADOR_BANCO and valor.upper() != "NULL":
                problemas.append(_problema("R5", raiz, caminho, _linha(texto, achado.start()),
                                           f"o banco de origem deve ser {MARCADOR_BANCO} (o nome real entra só ao rodar)"))
    return problemas


# ------------------------------------------------------- R2: JSON válido
def _eh_json(caminho: Path) -> bool:
    return caminho.name == ".platform" or caminho.suffix.lower() in {".json", ".pbir", ".pbism"}


def regra_r2(raiz: Path) -> list[str]:
    problemas = []
    for caminho in _arquivos(raiz):
        if not _eh_json(caminho):
            continue
        try:
            json.loads(caminho.read_text(encoding="utf-8-sig"))
        except (json.JSONDecodeError, UnicodeDecodeError) as erro:
            linha = getattr(erro, "lineno", 0)
            problemas.append(_problema("R2", raiz, caminho, linha, f"JSON inválido ({erro.__class__.__name__})"))
    return problemas


# --------------------------------------- R3: workspaceId zerado nos pipelines
def _valores_da_chave(obj, chave: str) -> Iterator[str]:
    if isinstance(obj, dict):
        for k, v in obj.items():
            if k == chave and isinstance(v, str):
                yield v
            yield from _valores_da_chave(v, chave)
    elif isinstance(obj, list):
        for item in obj:
            yield from _valores_da_chave(item, chave)


def regra_r3(raiz: Path) -> list[str]:
    problemas = []
    for caminho in raiz.rglob("pipeline-content.json"):
        if not caminho.parent.name.endswith(".DataPipeline"):
            continue
        texto = caminho.read_text(encoding="utf-8-sig")
        try:
            dados = json.loads(texto)
        except json.JSONDecodeError:
            continue  # R2 já aponta
        for valor in _valores_da_chave(dados, "workspaceId"):
            if valor != GUID_ZERADO:
                linha = _linha(texto, max(texto.find(valor), 0))
                problemas.append(_problema("R3", raiz, caminho, linha,
                                           "workspaceId deve ser o GUID zerado (o Fabric resolve no próprio workspace)"))
    return problemas


# ------------------------------------------- R4: GUID só onde é permitido
# Um GUID solto no código costuma ser o ID de um workspace ou item de UM
# ambiente: ao promover, ele aponta para o lugar errado. Só passa como valor de
# uma destas chaves (IDs lógicos do Git ou referências que o Fabric remapeia)...
CHAVES_GUID_PERMITIDAS: set[str] = {
    "logicalId", "lineageTag", "sourceLineageTag", "artifactId",
    "notebookId", "pipelineId", "connection",
}
# ...ou dentro destes arquivos (glob no caminho relativo -> justificativa).
ARQUIVOS_GUID_PERMITIDOS: dict[str, str] = {
    "*.SemanticModel/definition/expressions.tmdl": "parâmetros M do Direct Lake, de propósito",
    "*.SemanticModel/definition/relationships.tmdl": "nome interno do relacionamento, igual em todo ambiente",
    "*.VariableLibrary/variables.json": "valores padrão da Variable Library (IDs de conexão, não são segredo)",
    "*.VariableLibrary/valueSets/*.json": "conjuntos de valores por ambiente da Variable Library",
    "*.SQLDatabase/*.sqlproj": "identificador do projeto SQL gerado pelo Fabric",
    "tools/test_validar_repo.py": "GUIDs fictícios usados nos testes deste verificador",
    # Legado do projeto original: sai na etapa C4 (aposentar sm_solucoes-*, nb_semantic_models, df_date).
    "Processing/Dataflows/df_date.Dataflow/*": "legado (C4): destino fixo no lakehouse antigo",
    "Processing/Notebooks/nb_semantic_models.Notebook/*": "legado (C4): Environment do projeto antigo",
}
GUID = re.compile(r"\b[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}\b")
_ANTES_DA_CHAVE = re.compile(r"[\"']?([A-Za-z_][\w]*)[\"']?\s*[:=]\s*[\"']?$")


def _arquivo_guid_permitido(relativo: str) -> bool:
    return any(fnmatch(relativo, padrao) for padrao in ARQUIVOS_GUID_PERMITIDOS)


def regra_r4(raiz: Path) -> list[str]:
    problemas = []
    for caminho, texto in _arquivos_texto(raiz):
        if _arquivo_guid_permitido(_rel(raiz, caminho)):
            continue
        for achado in GUID.finditer(texto):
            if achado.group(0) == GUID_ZERADO:
                continue
            inicio_linha = texto.rfind("\n", 0, achado.start()) + 1
            chave = _ANTES_DA_CHAVE.search(texto[inicio_linha:achado.start()])
            if chave and chave.group(1) in CHAVES_GUID_PERMITIDAS:
                continue
            problemas.append(_problema("R4", raiz, caminho, _linha(texto, achado.start()),
                                       f"GUID fixo {achado.group(0)[:8]}… (use parâmetro, Variable Library ou ID lógico)"))
    return problemas


# --------------------------------------------- R6: lições já aprendidas no TMDL
PALAVRAS_RESERVADAS_VAR: set[str] = {"top"}
_MEDIDA = re.compile(r"^\s*measure\s+(?:'([^']+)'|(\S+))", re.M)
_COLUNA = re.compile(r"^\s*column\s+(?:'([^']+)'|(\S+))", re.M)
_VAR = re.compile(r"\bVAR\s+([A-Za-z_]\w*)", re.I)


def regra_r6(raiz: Path) -> list[str]:
    problemas = []
    for caminho in sorted(raiz.rglob("*.tmdl")):
        if any(parte in PASTAS_IGNORADAS for parte in caminho.relative_to(raiz).parts):
            continue
        texto = caminho.read_text(encoding="utf-8-sig")
        colunas = {(a or b).lower() for a, b in _COLUNA.findall(texto)}
        for achado in _MEDIDA.finditer(texto):
            nome = achado.group(1) or achado.group(2)
            if nome.lower() in colunas:
                problemas.append(_problema("R6", raiz, caminho, _linha(texto, achado.start()),
                                           f"medida '{nome}' tem o mesmo nome de uma coluna da tabela"))
        for achado in _VAR.finditer(texto):
            if achado.group(1).lower() in PALAVRAS_RESERVADAS_VAR:
                problemas.append(_problema("R6", raiz, caminho, _linha(texto, achado.start()),
                                           f"VAR '{achado.group(1)}' é palavra reservada do DAX"))
    return problemas


# ------------------------------------------------------------------ execução
REGRAS: list[Callable[[Path], list[str]]] = [regra_r1, regra_r2, regra_r3, regra_r4, regra_r5, regra_r6]


def _chave_ordem(problema: str) -> tuple[str, int, str]:
    _, resto = problema.split(" ", 1)
    caminho, linha, _ = resto.split(":", 2)
    return caminho, int(linha), problema


def validar_repo(raiz: str) -> list[str]:
    base = Path(raiz).resolve()
    problemas = [p for regra in REGRAS for p in regra(base)]
    return sorted(problemas, key=_chave_ordem)


def main(argv: list[str] | None = None) -> int:
    args = sys.argv[1:] if argv is None else argv
    raiz = args[0] if args else "."
    problemas = validar_repo(raiz)
    for problema in problemas:
        print(problema)
    if problemas:
        print(f"\n{len(problemas)} problema(s) encontrado(s).", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8")  # acentos no console do Windows
    sys.exit(main())
