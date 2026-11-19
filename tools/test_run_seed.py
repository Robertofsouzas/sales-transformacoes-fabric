"""Teste do seed repetível do db_control.

Roda o seed duas vezes e confirma que a tabela continua com o mesmo total de linhas.

Uso (PowerShell, na raiz do repositório):
    $env:SEED_WORKSPACE = "ws_sales_feature_roberto"
    $env:SEED_SOURCE_DATABASE = "<nome do banco de origem>"
    python -m unittest tools.test_run_seed -v

O nome do banco de origem vem do ambiente de quem roda; nunca fica no repositório.
"""
import os
import unittest

ESPERADO = 34  # Staging 13 + Bronze 13 + Silver 8


class TestRunSeed(unittest.TestCase):
    def test_seed_duas_vezes_nao_duplica(self):
        from tools.run_seed import run_seed

        workspace = os.environ["SEED_WORKSPACE"]
        banco = os.environ["SEED_SOURCE_DATABASE"]

        self.assertEqual(run_seed(workspace, banco), ESPERADO)
        self.assertEqual(run_seed(workspace, banco), ESPERADO)


if __name__ == "__main__":
    unittest.main()
