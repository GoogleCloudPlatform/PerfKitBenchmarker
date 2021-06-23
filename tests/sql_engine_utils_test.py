"""Tests for sql_engine_util."""

import unittest

from perfkitbenchmarker import sql_engine_utils
from tests import pkb_common_test_case


class SqlEngineUtilTest(pkb_common_test_case.PkbCommonTestCase):

  def testGetDbEngineType(self):
    self.assertEqual(
        sql_engine_utils.GetDbEngineType('aurora-postgresql'), 'postgres')
    self.assertEqual(
        sql_engine_utils.GetDbEngineType('aurora-mysql'), 'mysql')
    self.assertEqual(
        sql_engine_utils.GetDbEngineType('sqlserver-ex'), 'sqlserver')
    self.assertEqual(
        sql_engine_utils.GetDbEngineType('mysql'), 'mysql')
    with self.assertRaises(TypeError):
      sql_engine_utils.GetDbEngineType('abc')


if __name__ == '__main__':
  unittest.main()
