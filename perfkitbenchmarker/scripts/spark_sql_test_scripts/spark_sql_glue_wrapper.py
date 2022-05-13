"""Wrapper for running PySpark scripts in an AWS Glue Job."""

import importlib
import json
import sys
from awsglue.utils import getResolvedOptions

args = getResolvedOptions(sys.argv, ['pkb_main', 'pkb_args'])
pkb_args = json.loads(args['pkb_args'])
pkb_main = importlib.import_module(args['pkb_main'])
pkb_main.main(pkb_main.parse_args(pkb_args))
