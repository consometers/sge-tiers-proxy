import unittest
import suds

from sgeproxy.sge import SgeError, SgeErrorConverter


class TestDbMigrations(unittest.TestCase):
    def test_sge_error_catcher_converts_suds_webfault(self):

        # Looking like a SOAP fault element from Enedis
        class Object(object):
            pass

        fault = Object()
        fault.detail = Object()
        fault.detail.erreur = Object()
        fault.detail.erreur.resultat = Object()
        fault.detail.erreur.resultat.value = "TODO find a real example"
        fault.detail.erreur.resultat._code = "code"

        with self.assertRaises(SgeError) as context:
            with SgeErrorConverter():
                raise suds.WebFault(fault, None)

        self.assertEqual(context.exception.message, fault.detail.erreur.resultat.value)
        self.assertEqual(context.exception.code, fault.detail.erreur.resultat._code)

    def test_sge_error_catcher_converts_http_exception(self):

        http_exception = Exception((503, "Service Unavailable"))

        with self.assertRaises(SgeError) as context:
            with SgeErrorConverter():
                raise http_exception

        self.assertEqual(context.exception.message, "Service Unavailable")
        self.assertEqual(context.exception.code, "503")

    def test_sge_error_catcher_does_not_convert_others(self):

        other_exception = RuntimeError("Something else")

        with self.assertRaises(RuntimeError) as context:
            with SgeErrorConverter():
                raise other_exception

        self.assertEqual(context.exception, other_exception)


if __name__ == "__main__":

    unittest.main()
