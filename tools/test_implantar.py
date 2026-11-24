"""Testes do tools/implantar.py (deploy dev -> prod pelo pl_deploy_sales).

Rodar a partir da raiz do repositório:

    python -m unittest tools.test_implantar -v
"""
import unittest

from tools.implantar import (Resposta, acompanhar, chamar_com_retry, checar_modo, implantar,
                             ler_itens, montar_nota, resolver)

PROVENIENCIA = "commit da main no disparo: abc1234; conteúdo vem do workspace de dev"


class TesteItens(unittest.TestCase):
    def test_ler_itens_basico(self):
        self.assertEqual(ler_itens("nb_gold_checks:Notebook,rpt_vendas:Report"),
                         [("nb_gold_checks", "Notebook"), ("rpt_vendas", "Report")])

    def test_ler_itens_tolera_espacos(self):
        self.assertEqual(ler_itens(" nb_gold_checks : Notebook , ,"), [("nb_gold_checks", "Notebook")])

    def test_ler_itens_recusa_dataflow(self):
        with self.assertRaisesRegex(ValueError, "Dataflow"):
            ler_itens("df_date:Dataflow")

    def test_ler_itens_tipo_desconhecido(self):
        with self.assertRaisesRegex(ValueError, "notebook"):
            ler_itens("nb_x:notebook")

    def test_ler_itens_vazio(self):
        with self.assertRaises(ValueError):
            ler_itens("  ")


class TesteModo(unittest.TestCase):
    def test_simular_recusa_tipo_nao_documentado(self):
        with self.assertRaisesRegex(ValueError, "validateOnly"):
            checar_modo("simular", [("nb_gold_checks", "Notebook")])

    def test_simular_aceita_report(self):
        checar_modo("simular", [("rpt_vendas", "Report")])

    def test_implantar_aceita_qualquer_tipo_valido(self):
        checar_modo("implantar", [("nb_gold_checks", "Notebook")])

    def test_modo_invalido(self):
        with self.assertRaises(ValueError):
            checar_modo("forcar", [("rpt_vendas", "Report")])


class TesteNota(unittest.TestCase):
    def test_nota_tem_proveniencia(self):
        n = montar_nota("corrige X", "abc1234", "https://github.com/x/runs/1", "roberto", "roberto")
        self.assertIn(PROVENIENCIA, n)
        self.assertIn("corrige X", n)

    def test_nota_corta_motivo_e_preserva_proveniencia(self):
        n = montar_nota("m" * 5000, "abc1234", "https://github.com/x/runs/1", "roberto", "roberto")
        self.assertLessEqual(len(n), 1024)
        self.assertIn(PROVENIENCIA, n)


class ApiFalsa:
    """API em memória: respostas roteiradas por (método, caminho); registra as chamadas."""

    def __init__(self, rotas=None, sequencia=None):
        self.rotas = rotas or {}
        self.sequencia = list(sequencia or [])   # respostas consumidas em ordem, para qualquer chamada
        self.chamadas = []

    def chamar(self, metodo, url, corpo=None):
        self.chamadas.append((metodo, url, corpo))
        if self.sequencia:
            return self.sequencia.pop(0) if len(self.sequencia) > 1 else self.sequencia[0]
        resposta = self.rotas[(metodo, url)]
        return resposta.pop(0) if isinstance(resposta, list) and len(resposta) > 1 else (
            resposta[0] if isinstance(resposta, list) else resposta)

    def posts_de_deploy(self):
        return [c for c in self.chamadas if c[0] == "POST" and c[1].endswith("/deploy")]


PID, DEV, PRD, WS_DEV = "pipe-1", "stg-dev", "stg-prd", "ws-dev"
ID_NB, ID_RPT = "id-nb-gold-checks", "id-rpt-vendas"


def api_padrao(deploy_final="Succeeded", erro_deploy=None, passos=None):
    rotas = {
        ("GET", "/deploymentPipelines"): Resposta(200, {"value": [{"id": PID, "displayName": "pl_deploy_sales"}]}, {}),
        ("GET", f"/deploymentPipelines/{PID}/stages"): Resposta(200, {"value": [
            {"id": DEV, "displayName": "Dev", "order": 0, "workspaceId": WS_DEV},
            {"id": PRD, "displayName": "Prod", "order": 1, "workspaceId": "ws-prd"}]}, {}),
        ("GET", f"/workspaces/{WS_DEV}/items"): Resposta(200, {"value": [
            {"id": ID_NB, "displayName": "nb_gold_checks", "type": "Notebook"},
            {"id": ID_RPT, "displayName": "rpt_vendas", "type": "Report"}]}, {}),
        ("GET", "/operations/op-1"): Resposta(200, {"status": deploy_final}, {}),
        ("GET", "/operations/op-1/result"): Resposta(200, {
            "status": deploy_final,
            "executionPlan": {"steps": passos or [
                {"index": 0, "status": deploy_final, "sourceAndTarget": {"sourceItemDisplayName": "nb_gold_checks"}}]}}, {}),
    }
    if erro_deploy:
        codigo, status = erro_deploy
        rotas[("POST", f"/deploymentPipelines/{PID}/deploy")] = Resposta(
            status, {"errorCode": codigo, "message": "falhou"}, {})
    else:
        rotas[("POST", f"/deploymentPipelines/{PID}/deploy")] = Resposta(
            202, {}, {"Location": "https://api.fabric.microsoft.com/v1/operations/op-1", "Retry-After": "0"})
    return ApiFalsa(rotas)


class TesteCliente(unittest.TestCase):
    def test_resolver_acha_por_nome(self):
        pid, dev, prd, itens = resolver(api_padrao(), "pl_deploy_sales", [("nb_gold_checks", "Notebook")])
        self.assertEqual((pid, dev, prd), (PID, DEV, PRD))
        self.assertEqual(itens, [{"sourceItemId": ID_NB, "itemType": "Notebook"}])

    def test_resolver_item_inexistente(self):
        with self.assertRaisesRegex(LookupError, "nao_existe"):
            resolver(api_padrao(), "pl_deploy_sales", [("nao_existe", "Notebook")])

    def test_resolver_tipo_diferente_nao_serve(self):
        with self.assertRaisesRegex(LookupError, "rpt_vendas"):
            resolver(api_padrao(), "pl_deploy_sales", [("rpt_vendas", "Notebook")])

    def test_resolver_pipeline_inexistente(self):
        with self.assertRaisesRegex(LookupError, "pl_outro"):
            resolver(api_padrao(), "pl_outro", [("nb_gold_checks", "Notebook")])

    def test_retry_respeita_retry_after(self):
        esperas = []
        api = ApiFalsa(sequencia=[Resposta(429, {}, {"Retry-After": "2"}), Resposta(200, {"ok": 1}, {})])
        r = chamar_com_retry(api, "GET", "/x", dormir=esperas.append)
        self.assertEqual((r.status, esperas), (200, [2]))

    def test_retry_desiste_apos_3(self):
        api = ApiFalsa(sequencia=[Resposta(429, {}, {"Retry-After": "1"})])
        with self.assertRaises(RuntimeError):
            chamar_com_retry(api, "GET", "/x", dormir=lambda s: None)
        self.assertEqual(len(api.chamadas), 3)

    def test_acompanhar_tem_limite(self):
        api = ApiFalsa(sequencia=[Resposta(200, {"status": "Running"}, {})])
        with self.assertRaises(TimeoutError):
            acompanhar(api, "/op", limite=5, dormir=lambda s: None)

    def test_acompanhar_devolve_ao_terminar(self):
        api = ApiFalsa(sequencia=[Resposta(200, {"status": "Running"}, {}), Resposta(200, {"status": "Succeeded"}, {})])
        self.assertEqual(acompanhar(api, "/op", dormir=lambda s: None)["status"], "Succeeded")

    def test_implantar_simular_envia_validateonly(self):
        api = api_padrao()
        rc = implantar(api, "pl_deploy_sales", [("rpt_vendas", "Report")], "simular", "n", dormir=lambda s: None)
        self.assertEqual(rc, 0)
        corpo = api.posts_de_deploy()[0][2]
        self.assertEqual(corpo["options"]["itemOptionsBySourceItemId"],
                         [{"sourceItemId": ID_RPT, "options": {"validateOnly": True}}])
        self.assertEqual((corpo["note"], corpo["sourceStageId"], corpo["targetStageId"]), ("n", DEV, PRD))

    def test_implantar_real_nao_envia_validateonly(self):
        api = api_padrao()
        implantar(api, "pl_deploy_sales", [("nb_gold_checks", "Notebook")], "implantar", "n", dormir=lambda s: None)
        self.assertNotIn("options", api.posts_de_deploy()[0][2])

    def test_implantar_falha_devolve_1(self):
        api = api_padrao(deploy_final="Failed")
        rc = implantar(api, "pl_deploy_sales", [("nb_gold_checks", "Notebook")], "implantar", "n", dormir=lambda s: None)
        self.assertEqual(rc, 1)

    def test_implantar_principal_nao_suportado(self):
        api = api_padrao(erro_deploy=("PrincipalTypeNotSupported", 400))
        rc = implantar(api, "pl_deploy_sales", [("nb_gold_checks", "Notebook")], "implantar", "n", dormir=lambda s: None)
        self.assertEqual(rc, 1)


if __name__ == "__main__":
    unittest.main()
