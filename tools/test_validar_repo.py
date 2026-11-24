"""Testes do verificador do repositório (tools/validar_repo.py).

Cada teste monta um repositório de exemplo numa pasta temporária e confere
o que o verificador encontra. Rodar a partir da raiz do repositório:

    python -m unittest tools.test_validar_repo -v
"""
import tempfile
import unittest
from pathlib import Path

from tools.validar_repo import main, validar_repo


def escrever(raiz: str, caminho: str, texto: str) -> None:
    arquivo = Path(raiz) / caminho
    arquivo.parent.mkdir(parents=True, exist_ok=True)
    arquivo.write_text(texto, encoding="utf-8")


class TesteBase(unittest.TestCase):
    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.raiz = self._tmp.name

    def tearDown(self):
        self._tmp.cleanup()


class TesteR2R3(TesteBase):
    def test_repo_vazio_sem_problemas(self):
        self.assertEqual(validar_repo(self.raiz), [])

    def test_r2_json_invalido(self):
        escrever(self.raiz, "Processing/Pipelines/pl_x.DataPipeline/pipeline-content.json", "{ruim")
        probs = validar_repo(self.raiz)
        self.assertEqual(len(probs), 1)
        self.assertTrue(probs[0].startswith("R2 Processing/Pipelines/pl_x.DataPipeline/pipeline-content.json:"))

    def test_r2_cobre_platform_pbir_pbism(self):
        for nome in (".platform", "definition.pbir", "definition.pbism"):
            escrever(self.raiz, f"Visualization/Reports/r.Report/{nome}", "nao e json")
        self.assertEqual(sum(p.startswith("R2 ") for p in validar_repo(self.raiz)), 3)

    def test_r2_json_com_bom_passa(self):
        escrever(self.raiz, "a/b.json", "﻿{\"ok\": 1}")
        self.assertEqual(validar_repo(self.raiz), [])

    def test_r3_workspaceid_real_no_pipeline(self):
        escrever(self.raiz, "Processing/Pipelines/pl_x.DataPipeline/pipeline-content.json",
                 '{"a": {"workspaceId": "4dca1927-b150-4fa8-a815-bb0b06baca41"}}')
        self.assertTrue(any(p.startswith("R3 ") for p in validar_repo(self.raiz)))

    def test_r3_workspaceid_zerado_passa(self):
        escrever(self.raiz, "Processing/Pipelines/pl_x.DataPipeline/pipeline-content.json",
                 '{"a": {"workspaceId": "00000000-0000-0000-0000-000000000000"}}')
        self.assertEqual(validar_repo(self.raiz), [])

    def test_main_codigo_de_saida(self):
        self.assertEqual(main([self.raiz]), 0)
        escrever(self.raiz, "x.json", "{")
        self.assertEqual(main([self.raiz]), 1)


class TesteR1R5(TesteBase):
    # Os textos são montados por concatenação para não plantar um "segredo"
    # de verdade neste arquivo (o próprio verificador o reprovaria).
    def test_r1_padroes(self):
        casos = {
            "a.py": "client" + "_secret = 'abcd1234efgh5678'",
            "b.ps1": "pass" + "word=Segredo123",
            "c.txt": "Authorization: Bear" + "er eyJ" + "a" * 40,
            "d.cfg": "Account" + "Key=" + "Q" * 40,
            "e.txt": "sv=2024&s" + "ig=" + "A" * 30,
            "f.pem": "-----BEGIN RSA " + "PRIVATE KEY-----",
        }
        for nome, texto in casos.items():
            escrever(self.raiz, nome, texto)
        self.assertEqual(sum(p.startswith("R1 ") for p in validar_repo(self.raiz)), 6)

    def test_r1_palavra_sem_valor_passa(self):
        escrever(self.raiz, "README.md", "Sem senhas, tokens ou password no repositório.")
        self.assertEqual(validar_repo(self.raiz), [])

    def test_r5_seed_com_marcador_passa(self):
        escrever(self.raiz, "seeds/PipelineControl.seed.sql",
                 "(N'Staging', N'ENTIDADES', N'$(SourceDatabase)', N'dbo')")
        self.assertEqual(validar_repo(self.raiz), [])

    def test_r5_seed_com_banco_real(self):
        escrever(self.raiz, "seeds/PipelineControl.seed.sql",
                 "(N'Staging', N'ENTIDADES', N'BANCO_REAL', N'dbo')")
        self.assertTrue(any(p.startswith("R5 seeds/") for p in validar_repo(self.raiz)))


class TesteR4R6(TesteBase):
    def test_r4_guid_solto_reprova(self):
        escrever(self.raiz, "Processing/Notebooks/n.Notebook/notebook-content.py",
                 "caminho = 'abfss://4dca1927-b150-4fa8-a815-bb0b06baca41@onelake'")
        self.assertTrue(any(p.startswith("R4 Processing/Notebooks/") for p in validar_repo(self.raiz)))

    def test_r4_logicalid_passa(self):
        escrever(self.raiz, "x/.platform", '{"config": {"logicalId": "4dca1927-b150-4fa8-a815-bb0b06baca41"}}')
        self.assertEqual(validar_repo(self.raiz), [])

    def test_r4_expressions_tmdl_permitido(self):
        escrever(self.raiz, "Storage/Semantic Models/m.SemanticModel/definition/expressions.tmdl",
                 'expression p_workspace_id = "4dca1927-b150-4fa8-a815-bb0b06baca41"')
        self.assertEqual(validar_repo(self.raiz), [])

    def test_r4_dois_guids_na_mesma_linha(self):
        escrever(self.raiz, "a.txt", "https://onelake/4dca1927-b150-4fa8-a815-bb0b06baca41/"
                                      "fed20162-ccb5-4dd1-887a-41cba4f7cdd8")
        self.assertEqual(sum(p.startswith("R4 a.txt:1:") for p in validar_repo(self.raiz)), 2)

    def test_r6_medida_com_nome_de_coluna(self):
        escrever(self.raiz, "m.SemanticModel/definition/tables/t.tmdl",
                 "table t\n\tmeasure 'Quantidade' = SUM(t[quantidade])\n\tcolumn quantidade\n")
        self.assertTrue(any(p.startswith("R6 ") for p in validar_repo(self.raiz)))

    def test_r6_var_reservada(self):
        escrever(self.raiz, "m.SemanticModel/definition/tables/t.tmdl",
                 "table t\n\tmeasure 'X' =\n\t\t\tVAR top = 1\n\t\t\tRETURN top\n")
        self.assertTrue(any("top" in p and p.startswith("R6 ") for p in validar_repo(self.raiz)))


if __name__ == "__main__":
    unittest.main()
