"""Tests for data_science_validator.

This module contains tests that verify the evaluation logic for data science
agent outputs, ensuring correct handling of missing files, empty files, and
various answer formats.
"""

from absl.testing import absltest
from perfkitbenchmarker.data.agents.data_science.validator import data_science_validator


class DataScienceValidatorTest(absltest.TestCase):

  def test_evaluate_file_not_found(self):
    self.assertFalse(data_science_validator.evaluate("non_existent_file.txt"))

  def test_evaluate_pass_full(self):
    temp_file = self.create_tempfile(
        "answer.txt", content="Found SuperWidget with quantity 8"
    )
    self.assertTrue(data_science_validator.evaluate(temp_file.full_path))

  def test_evaluate_pass_partial(self):
    temp_file = self.create_tempfile(
        "answer.txt", content="Found SuperWidget with quantity 5"
    )
    self.assertFalse(data_science_validator.evaluate(temp_file.full_path))

  def test_evaluate_fail_missing_product(self):
    temp_file = self.create_tempfile(
        "answer.txt", content="Found MegaWidget with quantity 8"
    )
    self.assertFalse(data_science_validator.evaluate(temp_file.full_path))

  def test_evaluate_fail_empty_file(self):
    temp_file = self.create_tempfile("answer.txt", content="")
    self.assertFalse(data_science_validator.evaluate(temp_file.full_path))


if __name__ == "__main__":
  absltest.main()
