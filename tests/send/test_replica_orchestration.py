from __future__ import annotations

import unittest
from types import SimpleNamespace

from prefect.states import Completed, Failed, Running

from scripts.send.flujo_maestro_replica_datos import (
    describir_timeout,
    validar_estado_deployment,
    validar_timeout_espera,
)


class ReplicaOrchestrationTests(unittest.TestCase):
    def test_none_timeout_waits_without_limit(self):
        validar_timeout_espera(None, "exportación")
        self.assertEqual(describir_timeout(None), "sin límite de espera")

    def test_non_positive_timeout_is_rejected(self):
        for value in (0, -1):
            with self.subTest(value=value):
                with self.assertRaisesRegex(ValueError, "mayor que cero"):
                    validar_timeout_espera(value, "exportación")

    def test_completed_deployment_is_accepted(self):
        resultado = SimpleNamespace(state=Completed())
        validar_estado_deployment(resultado, "exportación", "repl.tabla", None)

    def test_running_deployment_reports_timeout_without_claiming_it_finished(self):
        resultado = SimpleNamespace(state=Running())
        with self.assertRaisesRegex(RuntimeError, "sigue en Running.*600s"):
            validar_estado_deployment(resultado, "exportación", "repl.tabla", 600)

    def test_failed_deployment_reports_final_failure(self):
        resultado = SimpleNamespace(state=Failed())
        with self.assertRaisesRegex(RuntimeError, "terminó en Failed"):
            validar_estado_deployment(resultado, "importación", "src.tabla", None)


if __name__ == "__main__":
    unittest.main()
