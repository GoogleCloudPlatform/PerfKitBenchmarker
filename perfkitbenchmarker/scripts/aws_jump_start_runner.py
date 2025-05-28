r"""Command line utility for manipulating AWS Sagemaker/Jumpstart Models.

Example call with required values for all operations on first line:
```
python aws_jump_start_runner.py --model_id=meta-textgeneration-llama-2-7b-f --model_version=2.* --region=us-west-2 \
--operation=create --role=my-role:sagemaker-full-access
```
"""

import json
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
    'model_version', '', help='Version of the model. Optional.'
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
_PAYLOAD_JSON = flags.DEFINE_string(
    'payload',
    '',
    help='The payload prompt sent to the model, as a json string.',
)
_PROMPT = flags.DEFINE_string(
    'prompt', '', help='Prompt sent to model, used in prompt mode'
)


def Create():
  """Creates a new model and endpoint."""
  assert _ROLE.value
  kwargs = {
      'model_id': _MODEL_ID.value,
      'region': _REGION.value,
      'role': _ROLE.value,
  }
  if _MODEL_VERSION.value:
    kwargs['model_version'] = _MODEL_VERSION.value
  model = JumpStartModel(**kwargs)
  print('Model name: <' + model.name + '>')
  predictor = model.deploy(accept_eula=True)
  print('Endpoint name: <' + predictor.endpoint_name + '>')
  return predictor


def GetModel():
  """Gets an existing model as a python object."""
  assert _ENDPOINT_NAME.value
  kwargs = {
      'region': _REGION.value,
      'model_id': _MODEL_ID.value,
  }
  if _MODEL_VERSION.value:
    kwargs['model_version'] = _MODEL_VERSION.value
  return predictor_lib.retrieve_default(
      _ENDPOINT_NAME.value,
      **kwargs,
  )


def SendPrompt(predictor: predictor_lib.Predictor):
  """Sends prompt given flags to the predictor."""
  assert _PAYLOAD_JSON.value

  def PrintDialog(response):
    if 'choices' in response:
      message = response['choices'][0]['message']
    else:
      message = response[0]['generation']
    assumed_role = message['role'].capitalize()
    content = message['content']
    print(f'Response>>>> {assumed_role}: {content}')
    print('\n====\n')

  payload = json.loads(_PAYLOAD_JSON.value)
  response = predictor.predict(payload, custom_attributes='accept_eula=true')
  PrintDialog(response)


def Delete(predictor: predictor_lib.Predictor):
  """Deletes the model and endpoint."""
  predictor.delete_model()
  predictor.delete_endpoint()


def main(argv):
  del argv
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
