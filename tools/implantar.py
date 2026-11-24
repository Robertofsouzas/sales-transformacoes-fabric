"""Deploy dev -> prod pelo pipeline de implantação pl_deploy_sales.

Chamado pelo workflow implantar-prod.yml (botão no GitHub, com aprovação).
Só biblioteca padrão. O token do Fabric vem da variável FABRIC_TOKEN e nunca
é impresso.
"""
from __future__ import annotations

import argparse
import json
import os
import sys
import time
import urllib.error
import urllib.request
from typing import Callable, NamedTuple, Protocol

BASE = "https://api.fabric.microsoft.com/v1"
PIPELINE = "pl_deploy_sales"
ESTAGIO_ORIGEM, ESTAGIO_DESTINO = "Dev", "Prod"
TENTATIVAS_429 = 3
SONECA_PADRAO = 5          # segundos entre consultas da operação
LIMITE_CONSULTAS = 120     # 120 x 5 s = 10 min no máximo

# Tipos aceitos em "itens". Dataflow fica de fora de propósito: a API de deploy
# não aceita identidade de serviço para ele.
TIPOS_VALIDOS: set[str] = {
    "Notebook", "DataPipeline", "SemanticModel", "Report",
    "Lakehouse", "VariableLibrary", "SQLDatabase", "Environment",
}
# Tipos cujo Update Definition documenta validateOnly (conferido em 24/11/2026:
# o exemplo oficial é Report; a página de Notebook não documenta a opção).
SIMULAR_PERMITIDO: set[str] = {"Report"}

LIMITE_NOTA = 1024


def ler_itens(texto: str) -> list[tuple[str, str]]:
    """Converte 'nome:Tipo,nome:Tipo' em [(nome, tipo), ...]."""
    itens = []
    for pedaco in texto.split(","):
        pedaco = pedaco.strip()
        if not pedaco:
            continue
        nome, sep, tipo = (parte.strip() for parte in pedaco.partition(":"))
        if not sep or not nome or not tipo:
            raise ValueError(f"item '{pedaco}' deve ter o formato nome:Tipo")
        if tipo == "Dataflow":
            raise ValueError("Dataflow não pode ser implantado por identidade de serviço")
        if tipo not in TIPOS_VALIDOS:
            raise ValueError(f"tipo '{tipo}' desconhecido; use um de {sorted(TIPOS_VALIDOS)} (maiúsculas exatas)")
        itens.append((nome, tipo))
    if not itens:
        raise ValueError("informe ao menos um item no formato nome:Tipo")
    return itens


def checar_modo(modo: str, itens: list[tuple[str, str]]) -> None:
    if modo not in {"simular", "implantar"}:
        raise ValueError(f"modo '{modo}' inválido; use simular ou implantar")
    if modo == "simular":
        recusados = [f"{n}:{t}" for n, t in itens if t not in SIMULAR_PERMITIDO]
        if recusados:
            raise ValueError("simular (validateOnly) só está documentado para "
                             f"{sorted(SIMULAR_PERMITIDO)}; recusado: {', '.join(recusados)}")


def montar_nota(motivo: str, sha: str, url_execucao: str, disparou: str, aprovou: str) -> str:
    """Nota do deploy (até 1.024 caracteres). Corta só o motivo; a proveniência fica inteira."""
    proveniencia = (f"commit da main no disparo: {sha}; conteúdo vem do workspace de dev"
                    f" | disparou: {disparou}; aprovou: {aprovou or 'n/d'} | execução: {url_execucao}")
    reservado = len(proveniencia) + len("\n")
    sobra = max(LIMITE_NOTA - reservado, 0)
    if len(motivo) > sobra:
        motivo = motivo[:max(sobra - 1, 0)] + "…"
    return f"{motivo}\n{proveniencia}"[:LIMITE_NOTA]


# ------------------------------------------------------------------ API
class Resposta(NamedTuple):
    status: int
    corpo: dict
    cabecalhos: dict


class Api(Protocol):
    def chamar(self, metodo: str, url: str, corpo: dict | None = None) -> Resposta: ...


def _caminho(url: str) -> str:
    """Troca a URL absoluta da API por caminho relativo (o Location vem absoluto)."""
    return url[len(BASE):] if url.startswith(BASE) else url


def _cabecalho(resposta: Resposta, nome: str, padrao: str = "") -> str:
    for chave, valor in resposta.cabecalhos.items():
        if chave.lower() == nome.lower():
            return valor
    return padrao


class ApiFabric:
    """Cliente HTTP mínimo. O token vem de FABRIC_TOKEN e só vai para api.fabric.microsoft.com."""

    def __init__(self, token: str):
        if not token:
            raise RuntimeError("FABRIC_TOKEN vazio: o login OIDC não rodou")
        self._token = token

    def chamar(self, metodo: str, url: str, corpo: dict | None = None) -> Resposta:
        alvo = url if url.startswith("http") else BASE + url
        if not alvo.startswith(BASE):
            raise ValueError("recusado: URL fora de api.fabric.microsoft.com")
        dados = json.dumps(corpo).encode("utf-8") if corpo is not None else None
        pedido = urllib.request.Request(alvo, data=dados, method=metodo, headers={
            "Authorization": f"Bearer {self._token}", "Content-Type": "application/json"})
        try:
            with urllib.request.urlopen(pedido, timeout=60) as r:
                texto = r.read().decode("utf-8")
                return Resposta(r.status, json.loads(texto) if texto else {}, dict(r.headers))
        except urllib.error.HTTPError as erro:
            texto = erro.read().decode("utf-8", "replace")
            try:
                corpo_erro = json.loads(texto) if texto else {}
            except json.JSONDecodeError:
                corpo_erro = {"message": texto[:300]}
            return Resposta(erro.code, corpo_erro, dict(erro.headers))


def chamar_com_retry(api: Api, metodo: str, url: str, corpo: dict | None = None,
                     tentativas: int = TENTATIVAS_429, dormir: Callable[[float], None] = time.sleep) -> Resposta:
    """429 = limite de chamadas: espera o Retry-After e tenta de novo, no máximo `tentativas` vezes."""
    for numero in range(1, tentativas + 1):
        resposta = api.chamar(metodo, url, corpo)
        if resposta.status != 429:
            return resposta
        if numero < tentativas:
            dormir(int(_cabecalho(resposta, "Retry-After", "5") or 5))
    raise RuntimeError(f"a API respondeu 429 (limite de chamadas) {tentativas} vezes seguidas; tente em alguns minutos")


def _listar(api: Api, url: str) -> list[dict]:
    resposta = chamar_com_retry(api, "GET", url)
    if resposta.status != 200:
        raise RuntimeError(f"GET {url} falhou: HTTP {resposta.status} {resposta.corpo.get('errorCode', '')}")
    return resposta.corpo.get("value", [])


def resolver(api: Api, pipeline_nome: str, itens: list[tuple[str, str]]) -> tuple[str, str, str, list[dict]]:
    """Descobre pipeline, estágios Dev/Prod e o ID de cada item (por nome e tipo) no workspace de dev."""
    pipelines = [p for p in _listar(api, "/deploymentPipelines") if p.get("displayName") == pipeline_nome]
    if not pipelines:
        raise LookupError(f"pipeline '{pipeline_nome}' não encontrado (a identidade é Admin dele?)")
    pid = pipelines[0]["id"]
    estagios = {e["displayName"]: e for e in _listar(api, f"/deploymentPipelines/{pid}/stages")}
    for nome in (ESTAGIO_ORIGEM, ESTAGIO_DESTINO):
        if nome not in estagios:
            raise LookupError(f"estágio '{nome}' não encontrado no pipeline")
    origem, destino = estagios[ESTAGIO_ORIGEM], estagios[ESTAGIO_DESTINO]
    existentes = _listar(api, f"/workspaces/{origem['workspaceId']}/items")
    pedidos = []
    for nome, tipo in itens:
        achado = next((i for i in existentes if i["displayName"] == nome and i["type"] == tipo), None)
        if not achado:
            raise LookupError(f"item '{nome}' do tipo {tipo} não existe no workspace de dev")
        pedidos.append({"sourceItemId": achado["id"], "itemType": tipo})
    return pid, origem["id"], destino["id"], pedidos


def acompanhar(api: Api, url_operacao: str, limite: int = LIMITE_CONSULTAS,
               dormir: Callable[[float], None] = time.sleep) -> dict:
    """Consulta a operação de longa duração até Succeeded/Failed; TimeoutError se passar do limite."""
    for _ in range(limite):
        resposta = chamar_com_retry(api, "GET", _caminho(url_operacao), dormir=dormir)
        if resposta.corpo.get("status") in {"Succeeded", "Failed"}:
            return resposta.corpo
        dormir(SONECA_PADRAO)
    raise TimeoutError(f"a operação não terminou em {limite} consultas")


def _resumo(corpo: dict) -> list[str]:
    linhas = []
    for passo in corpo.get("executionPlan", {}).get("steps", []):
        nome = passo.get("sourceAndTarget", {}).get("sourceItemDisplayName", "?")
        erro = passo.get("error", {}).get("message", "")
        linhas.append(f"  - {nome}: {passo.get('status', '?')}" + (f" ({erro})" if erro else ""))
    return linhas


def implantar(api: Api, pipeline_nome: str, itens: list[tuple[str, str]], modo: str, nota: str,
              dormir: Callable[[float], None] = time.sleep) -> int:
    """Retorna 0 se a operação terminou em Succeeded; 1 em qualquer falha."""
    try:
        pid, origem, destino, pedidos = resolver(api, pipeline_nome, itens)
    except (LookupError, RuntimeError) as erro:
        print(f"ERRO ao resolver itens: {erro}")
        return 1
    pedido = {"sourceStageId": origem, "targetStageId": destino, "items": pedidos, "note": nota}
    if modo == "simular":
        pedido["options"] = {"itemOptionsBySourceItemId": [
            {"sourceItemId": p["sourceItemId"], "options": {"validateOnly": True}} for p in pedidos]}
    try:
        resposta = chamar_com_retry(api, "POST", f"/deploymentPipelines/{pid}/deploy", pedido, dormir=dormir)
        if resposta.status not in (200, 202):
            codigo = resposta.corpo.get("errorCode", "?")
            print(f"ERRO no deploy: HTTP {resposta.status} {codigo}: {resposta.corpo.get('message', '')}")
            if codigo == "PrincipalTypeNotSupported":
                print("A identidade de serviço não é aceita para algum item: " + ", ".join(f"{n}:{t}" for n, t in itens))
            return 1
        if resposta.status == 202:
            local = _cabecalho(resposta, "Location")
            if not local:
                print("ERRO: a API aceitou o deploy mas não devolveu o Location da operação")
                return 1
            final = acompanhar(api, local, dormir=dormir)
            detalhe = chamar_com_retry(api, "GET", _caminho(local) + "/result", dormir=dormir)
            if detalhe.status == 200:
                final = {**final, **detalhe.corpo}
        else:
            final = resposta.corpo
    except (RuntimeError, TimeoutError) as erro:
        print(f"ERRO: {erro}")
        return 1
    status = final.get("status")
    print(f"Operação: {status} (modo: {modo})")
    for linha in _resumo(final):
        print(linha)
    return 0 if status == "Succeeded" else 1


# ------------------------------------------------------------------ CLI
def main(argv: list[str] | None = None) -> int:
    analisador = argparse.ArgumentParser(description="Deploy Dev -> Prod pelo pl_deploy_sales")
    analisador.add_argument("--itens", required=True, help="nome:Tipo,nome:Tipo")
    analisador.add_argument("--motivo", required=True)
    analisador.add_argument("--modo", default="simular", choices=["simular", "implantar"])
    args = analisador.parse_args(argv)
    try:
        itens = ler_itens(args.itens)
        checar_modo(args.modo, itens)
    except ValueError as erro:
        print(f"ERRO de entrada: {erro}")
        return 1
    env = os.environ
    servidor = env.get("GITHUB_SERVER_URL", "https://github.com")
    url = f"{servidor}/{env.get('GITHUB_REPOSITORY', '?')}/actions/runs/{env.get('GITHUB_RUN_ID', '?')}"
    nota = montar_nota(args.motivo, env.get("GITHUB_SHA", "?")[:7], url,
                       env.get("GITHUB_ACTOR", "?"), env.get("APROVADOR", ""))
    try:
        api = ApiFabric(env.get("FABRIC_TOKEN", ""))
    except RuntimeError as erro:
        print(f"ERRO: {erro}")
        return 1
    return implantar(api, PIPELINE, itens, args.modo, nota)


if __name__ == "__main__":
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8")
    sys.exit(main())
