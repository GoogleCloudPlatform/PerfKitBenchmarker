r"""Command line utility for manipulating AWS Sagemaker/Jumpstart Models.

Example call with required values for all operations on first line:
```
python aws_jump_start_runner.py --model_id=meta-textgeneration-llama-2-7b-f --model_version=2.* --region=us-west-2 \
--operation=create --role=my-role:sagemaker-full-access
```
"""

from absl import app
from absl import flags
from sagemaker import predictor as predictor_lib
from sagemaker.jumpstart.model import JumpStartModel

_ENDPOINT_NAME = flags.DEFINE_string(
    'endpoint_name',
    '',
    help='Name of an existing endpoint. Not needed if an endpoint is created.',
)
_ROLE = flags.DEFINE_string(
    'role', '', help='AWS role the model/endpoint will run under.'
)
_MODEL_ID = flags.DEFINE_string(
    'model_id', '', help='Id of the model. Required.'
)
_MODEL_VERSION = flags.DEFINE_string(
    'model_version', '', help='Version of the model. Required.'
)
_REGION = flags.DEFINE_string(
    'region', '', help='Name of the region model/endpoint are in. Required.'
)
_OPERATION = flags.DEFINE_enum(
    'operation',
    default='prompt',
    enum_values=['create', 'delete', 'prompt'],
    help='Required. Operation that will be done against the endpoint.',
)
_PROMPT = flags.DEFINE_string(
    'prompt', '', help='Prompt sent to model, used in prompt mode'
)
_MAX_TOKENS = flags.DEFINE_integer(
    'max_tokens',
    512,
    help='Max tokens returned in response, used in prompt mode',
)
_TEMPERATURE = flags.DEFINE_float(
    'temperature',
    1.0,
    help='Temperature / randomness of responses, used in prompt mode',
)


def Create():
  assert _ROLE.value
  model = JumpStartModel(
      model_id=_MODEL_ID.value,
      model_version=_MODEL_VERSION.value,
      region=_REGION.value,
      role=_ROLE.value,
  )
  print('Model name: <' + model.name + '>')
  predictor = model.deploy(accept_eula=True)
  print('Endpoint name: <' + predictor.endpoint_name + '>')
  return predictor


def GetModel():
  assert _ENDPOINT_NAME.value
  return predictor_lib.retrieve_default(
      _ENDPOINT_NAME.value,
      region=_REGION.value,
      model_id=_MODEL_ID.value,
      model_version=_MODEL_VERSION.value,
  )


def SendPrompt(predictor: predictor_lib.Predictor):
  """Sends prompt given flags to the predictor."""
  assert _PROMPT.value

  def PrintDialog(response):
    assumed_role = response[0]['generation']['role'].capitalize()
    content = response[0]['generation']['content']
    print(f'Response>>>> {assumed_role}: {content}')
    print('\n====\n')

  payload = {
      'inputs': [[
          {'role': 'user', 'content': _PROMPT.value},
      ]],
      'parameters': {
          'max_new_tokens': _MAX_TOKENS.value,
          'temperature': _TEMPERATURE.value,
      },
  }
  response = predictor.predict(payload, custom_attributes='accept_eula=true')
  PrintDialog(response)


def Delete(predictor: predictor_lib.Predictor):
  predictor.delete_model()
  predictor.delete_endpoint()


def main(argv):
  del argv
  assert _MODEL_VERSION.value
  assert _MODEL_ID.value
  assert _REGION.value
  if _OPERATION.value == 'create':
    Create()
  elif _OPERATION.value == 'delete':
    predictor = GetModel()
    Delete(predictor)
  elif _OPERATION.value == 'prompt':
    predictor = GetModel()
    SendPrompt(predictor)


if __name__ == '__main__':
  app.run(main)
