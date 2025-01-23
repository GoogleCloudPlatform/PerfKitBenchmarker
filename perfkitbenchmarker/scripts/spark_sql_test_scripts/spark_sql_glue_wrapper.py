"""Wrapper for running PySpark scripts in an AWS Glue Job."""

import importlib
import json
import sys

from awsglue import context
from awsglue import utils


def get_results_logger(spark_context):
  glue_context = context.GlueContext(spark_context)
  logger = glue_context.get_logger()
  return logger


args = utils.getResolvedOptions(sys.argv, ['pkb_main', 'pkb_args'])
pkb_args = json.loads(args['pkb_args'])
pkb_main = importlib.import_module(args['pkb_main'])
pkb_main.main(
    pkb_main.parse_args(pkb_args),
    results_logger_getter=get_results_logger,
)
