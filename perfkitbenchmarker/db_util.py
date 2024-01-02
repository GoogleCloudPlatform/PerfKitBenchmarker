"""Module containing database utility functions."""

import random
import string
import uuid


def GenerateRandomDbPassword():
  """Generate a strong random password.

   # pylint: disable=line-too-long
  Reference:
  https://docs.microsoft.com/en-us/sql/relational-databases/security/password-policy?view=sql-server-ver15
  # pylint: enable=line-too-long

  Returns:
    A random database password.
  """
  prefix = [
      random.choice(string.ascii_lowercase),
      random.choice(string.ascii_uppercase),
      random.choice(string.digits),
  ]
  return ''.join(prefix) + str(uuid.uuid4())[:10]
