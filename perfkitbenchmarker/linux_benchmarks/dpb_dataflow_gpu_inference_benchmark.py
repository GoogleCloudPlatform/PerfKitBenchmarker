# Copyright 2026 PerfKitBenchmarker Authors. All rights reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""Dataflow GPU Inference Benchmark.

Measures streaming BERT text classification throughput and latency on GCP
Dataflow with GPU-accelerated workers, comparing two inference approaches:

  - local_gpu: Model runs directly on the Dataflow worker's GPU via
    Apache Beam RunInference with a LocalGPUHandler.
  - vertex_ai: Dataflow workers send HTTP requests to a Vertex AI endpoint
    via a VertexAIHandler.

Both use an identical streaming pipeline:
  Pub/Sub (input) -> Decode -> RunInference(BERT) -> FormatResult
                  -> Pub/Sub (output)

The benchmark performs a configurable rate sweep and reports end-to-end
latency percentiles (p50, p95, p99), throughput, message loss rate, and
estimated infrastructure cost at each rate.

Prerequisites:
  1. A pre-built Dataflow GPU worker Docker image pushed to a container
     registry. The image must use the Beam SDK boot launcher as its
     entrypoint. See the benchmark README for Dockerfile details.
  2. A Flex Template spec JSON file uploaded to GCS, referencing the image.
     PKB can generate this automatically if --dpb_dataflow_gpu_worker_image
     is set and --dpb_dataflow_gpu_flex_template_gcs_location is left unset.
  3. A pre-trained DistilBERT model directory in GCS
     (train with scripts/train_model.py from the benchmark repo).
  4. (vertex_ai mode only) A deployed Vertex AI dedicated endpoint.
  5. google-cloud-pubsub Python package installed in the PKB environment
     (pip install google-cloud-pubsub).
"""

import json
import logging
import os
import textwrap
import time

import numpy as np
from absl import flags
from perfkitbenchmarker import configs
from perfkitbenchmarker import errors
from perfkitbenchmarker import sample
from perfkitbenchmarker import temp_dir
from perfkitbenchmarker import vm_util
from perfkitbenchmarker.providers.gcp import gcp_dpb_dataflow
from perfkitbenchmarker.providers.gcp import util

BENCHMARK_NAME = 'dpb_dataflow_gpu_inference_benchmark'

BENCHMARK_CONFIG = """
dpb_dataflow_gpu_inference_benchmark:
  description: >
    Streaming GPU inference benchmark on Dataflow. Compares Local GPU and
    Vertex AI endpoint approaches for BERT text classification, measuring
    latency, throughput, and cost across a configurable rate sweep.
  dpb_service:
    service_type: dataflow
    worker_group:
      vm_spec:
        GCP:
          machine_type: n1-standard-4
      disk_spec:
        GCP:
          disk_size: 300
    worker_count: 1
"""

FLAGS = flags.FLAGS

# ---------------------------------------------------------------------------
# Flags
# ---------------------------------------------------------------------------

flags.DEFINE_string(
    'dpb_dataflow_gpu_worker_image',
    None,
    'Docker image for the Dataflow GPU worker. Must include the Beam SDK '
    'boot launcher as its entrypoint (COPY from apache/beam_python3.11_sdk). '
    'Required unless --dpb_dataflow_gpu_flex_template_gcs_location is set.',
)
flags.DEFINE_string(
    'dpb_dataflow_gpu_flex_template_gcs_location',
    None,
    'GCS path to a pre-built Flex Template spec JSON file. If unset, PKB '
    'will generate a minimal spec from --dpb_dataflow_gpu_worker_image and '
    'upload it to the Dataflow staging bucket.',
)
flags.DEFINE_string(
    'dpb_dataflow_gpu_model_path',
    None,
    'GCS path to the pre-trained DistilBERT model directory '
    '(e.g. gs://my-bucket/bert-model/). Required.',
)
flags.DEFINE_enum(
    'dpb_dataflow_gpu_inference_mode',
    'local_gpu',
    ['local_gpu', 'vertex_ai', 'both'],
    'Inference mode(s) to benchmark. "both" runs local_gpu and vertex_ai '
    'in parallel Dataflow jobs for a direct comparison.',
)
flags.DEFINE_list(
    'dpb_dataflow_gpu_rates',
    ['25', '50', '75', '100', '125', '150'],
    'Publish rates to sweep in messages per second.',
)
flags.DEFINE_integer(
    'dpb_dataflow_gpu_duration_per_rate',
    100,
    'Duration of each rate step in seconds. Total messages per step = '
    'rate * duration.',
)
flags.DEFINE_string(
    'dpb_dataflow_gpu_gpu_type',
    'nvidia-tesla-t4',
    'GPU accelerator type to attach to each Dataflow worker for local_gpu '
    'mode (e.g. nvidia-tesla-t4, nvidia-l4). Ignored for vertex_ai mode.',
)
flags.DEFINE_integer(
    'dpb_dataflow_gpu_harness_threads',
    None,
    'Number of Dataflow worker harness threads '
    '(--number_of_worker_harness_threads). Defaults to Dataflow streaming '
    'default (12). Set lower (e.g. 2-3) when using local GPU to reduce '
    'GPU lock contention.',
)
flags.DEFINE_integer(
    'dpb_dataflow_gpu_max_batch_size',
    64,
    'Maximum RunInference batch size. Larger batches improve GPU utilization '
    'but add queuing latency.',
)
flags.DEFINE_integer(
    'dpb_dataflow_gpu_min_batch_size',
    1,
    'Minimum RunInference batch size. Setting above 1 forces Beam to '
    'accumulate elements before inference, reducing GPU kernel launches.',
)
flags.DEFINE_string(
    'dpb_dataflow_gpu_vertex_endpoint_id',
    '',
    'Vertex AI endpoint ID for vertex_ai inference mode.',
)
flags.DEFINE_string(
    'dpb_dataflow_gpu_vertex_region',
    None,
    'Vertex AI region (defaults to the Dataflow job region).',
)
flags.DEFINE_string(
    'dpb_dataflow_gpu_vertex_endpoint_dns',
    '',
    'Dedicated endpoint DNS hostname for rawPredict calls '
    '(e.g. <id>.us-central1-<hash>.prediction.vertexai.goog). '
    'If set, bypasses the regional API proxy.',
)
flags.DEFINE_bool(
    'dpb_dataflow_gpu_raw_predict',
    True,
    'Use :rawPredict instead of :predict for Vertex AI calls. '
    'rawPredict bypasses pre/post-processing and routes directly to '
    'the dedicated endpoint.',
)
flags.DEFINE_integer(
    'dpb_dataflow_gpu_num_labels',
    3,
    'Number of output classification labels for the BERT model.',
)
flags.DEFINE_integer(
    'dpb_dataflow_gpu_max_seq_length',
    128,
    'Maximum token sequence length for BERT tokenization.',
)
flags.DEFINE_string(
    'dpb_dataflow_gpu_category_names',
    'INCOME_WAGE,INCOME_GIG,EXPENSE',
    'Comma-separated category names for BERT classification output labels.',
)
flags.DEFINE_integer(
    'dpb_dataflow_gpu_warmup_messages',
    200,
    'Number of messages to publish during warmup before the rate sweep '
    'begins. Warmup confirms the full pipeline stack is healthy.',
)
flags.DEFINE_integer(
    'dpb_dataflow_gpu_collect_timeout_minutes',
    10,
    'Maximum minutes to wait for output messages to arrive in the output '
    'subscription during each rate step.',
)
flags.DEFINE_integer(
    'dpb_dataflow_gpu_worker_health_timeout_minutes',
    20,
    'Maximum minutes to wait for Dataflow workers to become healthy after '
    'job submission.',
)
flags.DEFINE_integer(
    'dpb_dataflow_gpu_vertex_replicas',
    1,
    'Number of Vertex AI endpoint replicas for vertex_ai mode. Only used '
    'for metadata reporting; PKB does not manage endpoint scaling.',
)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

# Template for synthetic transaction messages published to Pub/Sub.
_MESSAGE_TEMPLATE = (
    'Transaction {i}: customer {action} at {location} for ${amount:.2f}'
)
_ACTIONS = ['purchase', 'refund', 'transfer', 'withdrawal', 'deposit']
_LOCATIONS = [
    'retail store',
    'online shop',
    'ATM',
    'gas station',
    'restaurant',
    'grocery store',
    'pharmacy',
    'hardware store',
    'coffee shop',
    'hotel',
]

# Minimum fraction of published messages that must be collected for a
# rate step to be considered "healthy" (not saturated/dropping messages).
_MIN_COLLECTION_FRACTION = 0.90

# Poll interval when waiting for workers to become healthy.
_HEALTH_POLL_INTERVAL_SECONDS = 30

# Seconds to wait after draining output subscription before a new rate step.
_INTER_RATE_DRAIN_SECONDS = 5

# ---------------------------------------------------------------------------
# Publisher helper script (written to a temp file and run as subprocess).
# Uses google-cloud-pubsub to publish messages at a controlled rate.
# ---------------------------------------------------------------------------

_PUBLISHER_SCRIPT = textwrap.dedent("""\
    import json
    import sys
    import time
    from google.cloud import pubsub_v1

    topic = sys.argv[1]
    rate = float(sys.argv[2])
    count = int(sys.argv[3])

    publisher = pubsub_v1.PublisherClient()
    interval = 1.0 / rate if rate > 0 else 0

    actions = {actions}
    locations = {locations}

    futures = []
    for i in range(count):
        publish_time_ms = str(int(time.time() * 1000))
        action = actions[i % len(actions)]
        location = locations[i % len(locations)]
        amount = 10.0 + (i % 490)
        text = (
            f'Transaction {{i}}: customer {{action}} at {{location}} '
            f'for ${{amount:.2f}}'
        )
        future = publisher.publish(
            topic,
            data=text.encode('utf-8'),
            publish_time_ms=publish_time_ms,
        )
        futures.append(future)
        if interval > 0 and i < count - 1:
            time.sleep(interval)

    for f in futures:
        f.result()

    print(f'Published {{count}} messages')
    sys.stdout.flush()
""")

# ---------------------------------------------------------------------------
# Collector helper script (written to a temp file and run as subprocess).
# Pulls messages from a Pub/Sub subscription and writes JSON to stdout.
# ---------------------------------------------------------------------------

_COLLECTOR_SCRIPT = textwrap.dedent("""\
    import json
    import sys
    import time
    from google.cloud import pubsub_v1

    subscription = sys.argv[1]
    expected_count = int(sys.argv[2])
    timeout_seconds = int(sys.argv[3])

    subscriber = pubsub_v1.SubscriberClient()
    collected = []
    deadline = time.time() + timeout_seconds

    while len(collected) < expected_count and time.time() < deadline:
        remaining = expected_count - len(collected)
        try:
            response = subscriber.pull(
                request={
                    'subscription': subscription,
                    'max_messages': min(1000, remaining),
                },
                timeout=10,
            )
        except Exception:
            time.sleep(2)
            continue

        ack_ids = []
        for recv in response.received_messages:
            ack_ids.append(recv.ack_id)
            attrs = dict(recv.message.attributes)
            try:
                collected.append({
                    'latency_ms': int(attrs.get('latency_ms', 0)),
                    'pure_inference_time_ms': float(
                        attrs.get('pure_inference_time_ms', 0)
                    ),
                    'queue_wait_ms': int(attrs.get('queue_wait_ms', 0)),
                    'inference_overhead_ms': int(
                        attrs.get('inference_overhead_ms', 0)
                    ),
                })
            except (ValueError, KeyError):
                pass

        if ack_ids:
            subscriber.acknowledge(
                request={'subscription': subscription, 'ack_ids': ack_ids}
            )

        if not response.received_messages:
            time.sleep(2)

    print(json.dumps(collected))
    sys.stdout.flush()
""")

# ---------------------------------------------------------------------------
# Drainer script: acknowledges and discards all pending messages in a sub.
# ---------------------------------------------------------------------------

_DRAINER_SCRIPT = textwrap.dedent("""\
    import sys
    import time
    from google.cloud import pubsub_v1

    subscription = sys.argv[1]
    subscriber = pubsub_v1.SubscriberClient()

    drained = 0
    while True:
        try:
            response = subscriber.pull(
                request={'subscription': subscription, 'max_messages': 1000},
                timeout=5,
            )
        except Exception:
            break
        if not response.received_messages:
            break
        ack_ids = [r.ack_id for r in response.received_messages]
        subscriber.acknowledge(
            request={'subscription': subscription, 'ack_ids': ack_ids}
        )
        drained += len(ack_ids)

    print(f'Drained {drained} messages')
    sys.stdout.flush()
""")


def GetConfig(user_config):
  return configs.LoadConfig(BENCHMARK_CONFIG, user_config, BENCHMARK_NAME)


def CheckPrerequisites(_):
  """Validates required flags before the benchmark runs."""
  if not FLAGS.dpb_dataflow_gpu_model_path:
    raise errors.Config.InvalidValue(
        '--dpb_dataflow_gpu_model_path is required.'
    )
  if (
      not FLAGS.dpb_dataflow_gpu_worker_image
      and not FLAGS.dpb_dataflow_gpu_flex_template_gcs_location
  ):
    raise errors.Config.InvalidValue(
        'Either --dpb_dataflow_gpu_worker_image or '
        '--dpb_dataflow_gpu_flex_template_gcs_location must be set.'
    )
  mode = FLAGS.dpb_dataflow_gpu_inference_mode
  if mode in ('vertex_ai', 'both'):
    if not FLAGS.dpb_dataflow_gpu_vertex_endpoint_id:
      raise errors.Config.InvalidValue(
          '--dpb_dataflow_gpu_vertex_endpoint_id is required for '
          f'inference mode "{mode}".'
      )


def Prepare(benchmark_spec):
  """Creates GCP resources and submits Dataflow streaming jobs.

  Steps:
    1. Determine project, region, and GCS locations from the dpb_service.
    2. Generate and upload a Flex Template spec if needed.
    3. Create Pub/Sub topics and subscriptions for each inference mode.
    4. Submit a Dataflow Flex Template streaming job per mode.
    5. Pin worker counts to prevent autoscaling during the benchmark.
    6. Wait for workers to become healthy (GPU workers take 5-15 min).
    7. Publish warmup messages and wait for output to confirm readiness.
  """
  dpb_service_instance = benchmark_spec.dpb_service
  project = util.GetDefaultProject()
  region = util.GetRegionFromZone(FLAGS.dpb_service_zone)
  staging_dir = dpb_service_instance.GetStagingLocation()

  # Store state on benchmark_spec for use in Run and Cleanup.
  benchmark_spec.gpu_benchmark = {
      'project': project,
      'region': region,
      'staging_dir': staging_dir,
      'job_ids': {},  # mode -> Dataflow job ID
      'input_topics': {},  # mode -> full Pub/Sub topic resource name
      'input_subs': {},  # mode -> full Pub/Sub subscription resource name
      'output_topics': {},  # mode -> full Pub/Sub topic resource name
      'output_subs': {},  # mode -> full Pub/Sub subscription resource name
  }
  state = benchmark_spec.gpu_benchmark

  # 1. Resolve Flex Template GCS location.
  template_gcs_location = _EnsureFlexTemplate(state, staging_dir)
  state['template_gcs_location'] = template_gcs_location

  # 2. Write helper scripts to the run temp directory.
  run_dir = temp_dir.GetRunDirPath()
  state['publisher_script'] = os.path.join(run_dir, 'gpu_publisher.py')
  state['collector_script'] = os.path.join(run_dir, 'gpu_collector.py')
  state['drainer_script'] = os.path.join(run_dir, 'gpu_drainer.py')
  _WriteHelperScripts(state)

  # 3. Create Pub/Sub topics and subscriptions.
  for mode in _GetModes():
    _CreatePubSubResources(state, mode, project)

  # 4. Submit Dataflow jobs.
  num_workers = benchmark_spec.dpb_service.spec.worker_count
  machine_type = (
      benchmark_spec.dpb_service.spec.worker_group.vm_spec.machine_type
  )
  for mode in _GetModes():
    job_id = _SubmitFlexTemplateJob(
        state, mode, project, region, num_workers, machine_type
    )
    state['job_ids'][mode] = job_id
    logging.info('Submitted Dataflow job %s for mode=%s', job_id, mode)

  # 5. Pin worker counts so autoscaler does not scale down.
  for mode, job_id in state['job_ids'].items():
    _PinWorkerCount(job_id, project, region, num_workers)

  # 6. Wait for workers to become healthy.
  timeout_min = FLAGS.dpb_dataflow_gpu_worker_health_timeout_minutes
  for mode, job_id in state['job_ids'].items():
    logging.info('Waiting for %s workers to become healthy...', mode)
    _WaitForHealthyWorkers(job_id, project, region, num_workers, timeout_min)

  # 7. Warmup: publish messages and wait for output.
  warmup_count = FLAGS.dpb_dataflow_gpu_warmup_messages
  timeout_s = FLAGS.dpb_dataflow_gpu_collect_timeout_minutes * 60
  for mode in _GetModes():
    logging.info('Running warmup for mode=%s (%d messages)', mode, warmup_count)
    _RunPublisher(
        state, state['input_topics'][mode], rate=50, count=warmup_count
    )
    _RunCollector(state, state['output_subs'][mode], warmup_count, timeout_s)
    _DrainOutputSubscription(state, state['output_subs'][mode])
    logging.info('Warmup complete for mode=%s', mode)


def Run(benchmark_spec):
  """Runs the rate sweep and returns performance samples.

  For each configured rate, publishes messages at that rate for the
  configured duration, waits for output, and computes latency/throughput
  metrics. Results are emitted as PKB samples with 'rate_msg_per_sec'
  in the metadata.

  Returns:
    List of sample.Sample with latency, throughput, loss, and cost metrics.
  """
  state = benchmark_spec.gpu_benchmark
  results = []
  rates = [int(r) for r in FLAGS.dpb_dataflow_gpu_rates]
  duration = FLAGS.dpb_dataflow_gpu_duration_per_rate
  timeout_s = FLAGS.dpb_dataflow_gpu_collect_timeout_minutes * 60

  for mode in _GetModes():
    for rate in rates:
      count = rate * duration
      logging.info(
          'Rate sweep: mode=%s rate=%d msg/s count=%d duration=%ds',
          mode,
          rate,
          count,
          duration,
      )

      # Drain any leftover messages from a previous step.
      _DrainOutputSubscription(state, state['output_subs'][mode])
      time.sleep(_INTER_RATE_DRAIN_SECONDS)

      # Publish at target rate.
      _RunPublisher(state, state['input_topics'][mode], rate=rate, count=count)

      # Collect output.
      raw_json = _RunCollector(
          state, state['output_subs'][mode], count, timeout_s
      )
      collected = json.loads(raw_json) if raw_json.strip() else []

      # Compute and append samples.
      metadata = _BuildMetadata(benchmark_spec, mode, rate)
      results.extend(_ComputeSamples(collected, count, rate, metadata))

  return results


def Cleanup(benchmark_spec):
  """Drains Dataflow jobs and deletes Pub/Sub topics and subscriptions."""
  if not hasattr(benchmark_spec, 'gpu_benchmark'):
    return
  state = benchmark_spec.gpu_benchmark
  project = state['project']
  region = state['region']

  # Cancel streaming Dataflow jobs.
  for mode, job_id in state.get('job_ids', {}).items():
    logging.info('Cancelling Dataflow job %s (mode=%s)', job_id, mode)
    try:
      cmd = util.GcloudCommand(None, 'dataflow', 'jobs', 'cancel', job_id)
      cmd.flags = {'project': project, 'region': region, 'format': 'json'}
      cmd.Issue(raise_on_failure=False)
    except Exception:  # pylint: disable=broad-except
      logging.warning('Failed to cancel job %s', job_id, exc_info=True)

  # Delete Pub/Sub topics and subscriptions.
  for mode in _GetModes():
    for resource_type, resource_map, gcloud_group in (
        ('subscription', state.get('input_subs', {}), 'subscriptions'),
        ('subscription', state.get('output_subs', {}), 'subscriptions'),
        ('topic', state.get('input_topics', {}), 'topics'),
        ('topic', state.get('output_topics', {}), 'topics'),
    ):
      resource = resource_map.get(mode)
      if not resource:
        continue
      resource_name = resource.split('/')[-1]
      try:
        cmd = util.GcloudCommand(
            None, 'pubsub', gcloud_group, 'delete', resource_name
        )
        cmd.flags = {'project': project, 'format': 'json'}
        cmd.Issue(raise_on_failure=False)
      except Exception:  # pylint: disable=broad-except
        logging.warning(
            'Failed to delete %s %s', resource_type, resource, exc_info=True
        )


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


def _GetModes():
  """Returns the list of inference modes to run."""
  mode = FLAGS.dpb_dataflow_gpu_inference_mode
  if mode == 'both':
    return ['local_gpu', 'vertex_ai']
  return [mode]


def _EnsureFlexTemplate(state, staging_dir):
  """Returns the GCS path to the Flex Template spec, generating it if needed."""
  if FLAGS.dpb_dataflow_gpu_flex_template_gcs_location:
    return FLAGS.dpb_dataflow_gpu_flex_template_gcs_location

  # Generate a minimal Flex Template spec and upload it to GCS.
  worker_image = FLAGS.dpb_dataflow_gpu_worker_image
  spec = {
      'image': worker_image,
      'sdkInfo': {'language': 'PYTHON'},
      'defaultEnvironment': {
          'sdkContainerImage': worker_image,
          'additionalExperiments': [
              'use_runner_v2',
              'no_use_multiple_sdk_containers',
          ],
      },
  }
  spec_path = os.path.join(
      temp_dir.GetRunDirPath(), 'gpu_inference_template_spec.json'
  )
  with open(spec_path, 'w') as f:
    json.dump(spec, f)

  gcs_spec_path = os.path.join(staging_dir, 'gpu_inference_template_spec.json')
  vm_util.IssueCommand([
      'gsutil',
      'cp',
      spec_path,
      gcs_spec_path,
  ])
  logging.info('Uploaded Flex Template spec to %s', gcs_spec_path)
  return gcs_spec_path


def _WriteHelperScripts(state):
  """Writes publisher, collector, and drainer Python scripts to disk."""
  actions_repr = repr(_ACTIONS)
  locations_repr = repr(_LOCATIONS)
  publisher_src = _PUBLISHER_SCRIPT.format(
      actions=actions_repr, locations=locations_repr
  )
  with open(state['publisher_script'], 'w') as f:
    f.write(publisher_src)
  with open(state['collector_script'], 'w') as f:
    f.write(_COLLECTOR_SCRIPT)
  with open(state['drainer_script'], 'w') as f:
    f.write(_DRAINER_SCRIPT)


def _CreatePubSubResources(state, mode, project):
  """Creates Pub/Sub topics and subscriptions for a given inference mode."""
  suffix = mode.replace('_', '-')
  input_topic_name = f'pkb-gpu-input-{suffix}'
  output_topic_name = f'pkb-gpu-output-{suffix}'
  input_sub_name = f'pkb-gpu-input-sub-{suffix}'
  output_sub_name = f'pkb-gpu-output-sub-{suffix}'

  for topic_name in (input_topic_name, output_topic_name):
    cmd = util.GcloudCommand(None, 'pubsub', 'topics', 'create', topic_name)
    cmd.flags = {'project': project, 'format': 'json'}
    cmd.Issue()

  for sub_name, topic_name in (
      (input_sub_name, input_topic_name),
      (output_sub_name, output_topic_name),
  ):
    cmd = util.GcloudCommand(
        None, 'pubsub', 'subscriptions', 'create', sub_name
    )
    cmd.flags = {
        'project': project,
        'topic': topic_name,
        'ack-deadline': '600',
        'format': 'json',
    }
    cmd.Issue()

  state['input_topics'][mode] = f'projects/{project}/topics/{input_topic_name}'
  state['output_topics'][
      mode
  ] = f'projects/{project}/topics/{output_topic_name}'
  state['input_subs'][
      mode
  ] = f'projects/{project}/subscriptions/{input_sub_name}'
  state['output_subs'][
      mode
  ] = f'projects/{project}/subscriptions/{output_sub_name}'
  logging.info(
      'Created Pub/Sub resources for mode=%s: input=%s output=%s',
      mode,
      input_topic_name,
      output_topic_name,
  )


def _SubmitFlexTemplateJob(
    state, mode, project, region, num_workers, machine_type
):
  """Submits a Dataflow Flex Template streaming job for one inference mode.

  For local_gpu mode, attaches a GPU accelerator to each worker.
  For vertex_ai mode, uses a CPU-only worker machine (no GPU needed since
  inference is performed on the Vertex AI endpoint).

  Args:
    state: Benchmark state dict from benchmark_spec.gpu_benchmark.
    mode: 'local_gpu' or 'vertex_ai'.
    project: GCP project ID.
    region: Dataflow region.
    num_workers: Fixed worker count.
    machine_type: Worker machine type from benchmark config.

  Returns:
    Dataflow job ID string.
  """
  job_name = f'pkb-gpu-inference-{mode.replace("_", "-")}-{int(time.time())}'
  vertex_region = FLAGS.dpb_dataflow_gpu_vertex_region or region

  # Build pipeline parameters.
  parameters = [
      f'mode={mode}',
      f'input_subscription={state["input_subs"][mode]}',
      f'output_topic={state["output_topics"][mode]}',
      f'model_path={FLAGS.dpb_dataflow_gpu_model_path}',
      f'num_labels={FLAGS.dpb_dataflow_gpu_num_labels}',
      f'max_seq_length={FLAGS.dpb_dataflow_gpu_max_seq_length}',
      f'category_names={FLAGS.dpb_dataflow_gpu_category_names}',
      f'max_batch_size={FLAGS.dpb_dataflow_gpu_max_batch_size}',
      f'min_batch_size={FLAGS.dpb_dataflow_gpu_min_batch_size}',
  ]
  if mode == 'vertex_ai':
    parameters += [
        f'vertex_endpoint_id={FLAGS.dpb_dataflow_gpu_vertex_endpoint_id}',
        f'vertex_region={vertex_region}',
        f'vertex_endpoint_dns={FLAGS.dpb_dataflow_gpu_vertex_endpoint_dns}',
    ]
    if FLAGS.dpb_dataflow_gpu_raw_predict:
      parameters.append('raw_predict=True')

  # Resolve effective machine type for vertex_ai mode. GPU-optimized machine
  # families (G2, A2, A3) require onHostMaintenance=TERMINATE, which Dataflow
  # only sets when a GPU accelerator is attached. Since vertex_ai workers
  # do not use GPUs, convert to the N1 equivalent preserving vCPU count.
  effective_machine_type = machine_type
  if mode == 'vertex_ai':
    effective_machine_type = _ResolveVertexAiWorkerMachine(machine_type)

  cmd = util.GcloudCommand(None, 'dataflow', 'flex-template', 'run', job_name)
  cmd.flags = {
      'project': project,
      'region': region,
      'template-file-gcs-location': state['template_gcs_location'],
      'worker-machine-type': effective_machine_type,
      'num-workers': num_workers,
      'max-workers': num_workers,
      'staging-location': state['staging_dir'],
      'format': 'json',
  }

  # Build additional experiments list.
  additional_experiments = [
      'use_runner_v2',
      'no_use_multiple_sdk_containers',
  ]
  if FLAGS.dpb_dataflow_gpu_harness_threads is not None:
    # num_threads_per_worker is the experiment alias for
    # --number_of_worker_harness_threads in Dataflow streaming.
    additional_experiments.append(
        f'num_threads_per_worker={FLAGS.dpb_dataflow_gpu_harness_threads}'
    )

  # For local_gpu mode, pass the GPU accelerator service option.
  if mode == 'local_gpu':
    gpu_type = FLAGS.dpb_dataflow_gpu_gpu_type
    additional_experiments.append(
        f'worker_accelerator=type:{gpu_type};count:1;install-nvidia-driver'
    )

  # gcloud flex-template run accepts --additional-experiments as a
  # repeated flag. Build it as a list.
  for exp in additional_experiments:
    cmd.additional_flags.append(f'--additional-experiments={exp}')

  for param in parameters:
    cmd.additional_flags.append(f'--parameters={param}')

  stdout, _, _ = cmd.Issue()

  try:
    result = json.loads(stdout)
    job_id = result['job']['id']
  except (json.JSONDecodeError, KeyError) as e:
    raise errors.Benchmarks.RunError(
        f'Failed to parse Dataflow job ID from flex-template run output: {e}'
    ) from e

  return job_id


def _ResolveVertexAiWorkerMachine(machine_type):
  """Converts GPU-optimized machine families to N1 for Vertex AI workers.

  GPU-optimized machine families (G2, A2, A3) require
  onHostMaintenance=TERMINATE, which Dataflow only sets when a GPU accelerator
  is attached. Since Vertex AI workers only perform HTTP calls (no GPU needed),
  convert to the N1 equivalent preserving the vCPU count.

  For example: g2-standard-8 -> n1-standard-8, a2-highgpu-1g -> n1-standard-8.
  """
  # Extract vCPU count from machine type string.
  # Common patterns: n1-standard-4, g2-standard-8, a2-highgpu-4g
  parts = machine_type.split('-')
  family = parts[0].lower()
  if family in ('g2', 'a2', 'a3'):
    # Find the numeric vCPU count in the last segment.
    last = parts[-1].rstrip('g')
    try:
      vcpus = int(last)
    except ValueError:
      vcpus = 4
    return f'n1-standard-{vcpus}'
  return machine_type


def _PinWorkerCount(job_id, project, region, num_workers):
  """Sets minNumWorkers=maxNumWorkers to prevent autoscaling during benchmark.

  The Beam Python SDK sets --num_workers and --max_num_workers but not
  --min_num_workers, allowing Streaming Engine's autoscaler to scale down.
  Use update-options to pin the count explicitly.
  """
  cmd = util.GcloudCommand(None, 'dataflow', 'jobs', 'update-options', job_id)
  cmd.flags = {
      'project': project,
      'region': region,
      'min-num-workers': num_workers,
      'max-num-workers': num_workers,
  }
  cmd.Issue(raise_on_failure=False)
  logging.info('Pinned worker count to %d for job %s', num_workers, job_id)


def _WaitForHealthyWorkers(job_id, project, region, num_workers, timeout_min):
  """Polls Dataflow metrics until the requested number of workers are active.

  Checks CurrentVcpuCount (and CurrentGpuCount for local_gpu mode) from
  the Dataflow service metrics. GPU workers can take 5-15 minutes to
  initialize (VM boot, container pull, driver install, model load).

  Args:
    job_id: Dataflow job ID.
    project: GCP project ID.
    region: Dataflow region.
    num_workers: Expected number of active workers.
    timeout_min: Maximum minutes to wait before raising an error.

  Raises:
    errors.Benchmarks.RunError: If workers do not become healthy in time.
  """
  deadline = time.time() + timeout_min * 60
  while time.time() < deadline:
    cmd = util.GcloudCommand(None, 'dataflow', 'metrics', 'list', job_id)
    cmd.use_beta_gcloud = True
    cmd.flags = {
        'project': project,
        'region': region,
        'source': 'service',
        'format': 'json',
    }
    stdout, _, retcode = cmd.Issue(raise_on_failure=False)
    if retcode != 0:
      time.sleep(_HEALTH_POLL_INTERVAL_SECONDS)
      continue

    try:
      metrics = json.loads(stdout)
    except json.JSONDecodeError:
      time.sleep(_HEALTH_POLL_INTERVAL_SECONDS)
      continue

    vcpu_count = 0
    for m in metrics:
      if m.get('name', {}).get('name') == 'CurrentVcpuCount':
        try:
          vcpu_count = int(m.get('scalar', 0))
        except (TypeError, ValueError):
          pass

    logging.info(
        'Job %s: CurrentVcpuCount=%d (waiting for %d workers)',
        job_id,
        vcpu_count,
        num_workers,
    )
    if vcpu_count >= num_workers:
      logging.info('Workers healthy for job %s', job_id)
      return

    time.sleep(_HEALTH_POLL_INTERVAL_SECONDS)

  raise errors.Benchmarks.RunError(
      f'Dataflow job {job_id} workers did not become healthy within '
      f'{timeout_min} minutes.'
  )


def _RunPublisher(state, topic, rate, count):
  """Publishes messages to a Pub/Sub topic at the specified rate.

  Args:
    state: Benchmark state dict.
    topic: Full Pub/Sub topic resource name.
    rate: Target publish rate in messages per second.
    count: Total number of messages to publish.
  """
  cmd = [
      'python3',
      state['publisher_script'],
      topic,
      str(rate),
      str(count),
  ]
  # Timeout: allow 2x the theoretical publish duration plus 60s buffer.
  timeout = max(120, int(count / rate * 2) + 60)
  stdout, _, retcode = vm_util.IssueCommand(cmd, timeout=timeout)
  if retcode != 0:
    raise errors.Benchmarks.RunError(
        f'Publisher script failed for topic {topic}.'
    )
  logging.info('Publisher: %s', stdout.strip())


def _RunCollector(state, subscription, expected_count, timeout_seconds):
  """Pulls output messages from a Pub/Sub subscription.

  Args:
    state: Benchmark state dict.
    subscription: Full Pub/Sub subscription resource name.
    expected_count: Number of messages to collect before returning.
    timeout_seconds: Maximum seconds to wait.

  Returns:
    JSON string of collected message attribute dicts.
  """
  cmd = [
      'python3',
      state['collector_script'],
      subscription,
      str(expected_count),
      str(timeout_seconds),
  ]
  stdout, _, retcode = vm_util.IssueCommand(cmd, timeout=timeout_seconds + 30)
  if retcode != 0:
    logging.warning(
        'Collector script returned non-zero for subscription %s', subscription
    )
    return '[]'
  return stdout.strip()


def _DrainOutputSubscription(state, subscription):
  """Acknowledges and discards all pending messages in a subscription."""
  cmd = ['python3', state['drainer_script'], subscription]
  stdout, _, _ = vm_util.IssueCommand(cmd, timeout=60, raise_on_failure=False)
  logging.info('Drainer: %s', stdout.strip())


def _BuildMetadata(benchmark_spec, mode, rate):
  """Builds the sample metadata dict for a single rate-step result."""
  dpb = benchmark_spec.dpb_service
  worker_machine = dpb.spec.worker_group.vm_spec.machine_type
  num_workers = dpb.spec.worker_count
  metadata = {
      'inference_mode': mode,
      'rate_msg_per_sec': rate,
      'duration_seconds': FLAGS.dpb_dataflow_gpu_duration_per_rate,
      'num_workers': num_workers,
      'worker_machine_type': worker_machine,
      'gpu_type': (
          FLAGS.dpb_dataflow_gpu_gpu_type if mode == 'local_gpu' else 'none'
      ),
      'harness_threads': FLAGS.dpb_dataflow_gpu_harness_threads,
      'max_batch_size': FLAGS.dpb_dataflow_gpu_max_batch_size,
      'min_batch_size': FLAGS.dpb_dataflow_gpu_min_batch_size,
      'model_path': FLAGS.dpb_dataflow_gpu_model_path,
      'max_seq_length': FLAGS.dpb_dataflow_gpu_max_seq_length,
      'num_labels': FLAGS.dpb_dataflow_gpu_num_labels,
  }
  if mode == 'vertex_ai':
    metadata.update({
        'vertex_endpoint_id': FLAGS.dpb_dataflow_gpu_vertex_endpoint_id,
        'vertex_region': (
            FLAGS.dpb_dataflow_gpu_vertex_region
            or util.GetRegionFromZone(FLAGS.dpb_service_zone)
        ),
        'vertex_replicas': FLAGS.dpb_dataflow_gpu_vertex_replicas,
        'raw_predict': FLAGS.dpb_dataflow_gpu_raw_predict,
    })
  return metadata


def _ComputeSamples(collected, published_count, rate, metadata):
  """Computes and returns PKB samples from collected output messages.

  Metrics:
    - collected_count: Number of output messages received.
    - loss_rate: Fraction of published messages not collected.
    - processing_throughput: Collected messages / total elapsed wall time.
    - latency_p50/p95/p99/mean: End-to-end latency percentiles in ms.
    - pure_inference_p50/p95/p99/mean: GPU/endpoint-only inference time.
    - queue_wait_p50/p95/p99: Time in Pub/Sub + Beam queue before inference.
    - healthy: 1 if >= 90% of messages collected, 0 otherwise.

  Args:
    collected: List of dicts with latency_ms, pure_inference_time_ms, etc.
    published_count: Total messages published in this rate step.
    rate: Target publish rate (messages/second).
    metadata: PKB sample metadata dict.

  Returns:
    List of sample.Sample.
  """
  results = []
  n = len(collected)
  loss_rate = (
      max(0.0, 1.0 - n / published_count) if published_count > 0 else 1.0
  )
  healthy = 1 if n >= published_count * _MIN_COLLECTION_FRACTION else 0

  results.append(sample.Sample('collected_count', n, 'messages', metadata))
  results.append(
      sample.Sample('published_count', published_count, 'messages', metadata)
  )
  results.append(sample.Sample('loss_rate', loss_rate, 'fraction', metadata))
  results.append(sample.Sample('healthy', healthy, 'bool', metadata))

  if not collected:
    logging.warning(
        'No messages collected for rate=%d mode=%s',
        rate,
        metadata.get('inference_mode'),
    )
    return results

  latencies = np.array([m['latency_ms'] for m in collected], dtype=float)
  inference_times = np.array(
      [m['pure_inference_time_ms'] for m in collected], dtype=float
  )
  queue_waits = np.array([m['queue_wait_ms'] for m in collected], dtype=float)

  for field, values, unit in (
      ('latency', latencies, 'ms'),
      ('pure_inference', inference_times, 'ms'),
      ('queue_wait', queue_waits, 'ms'),
  ):
    for pct in (50, 95, 99):
      results.append(
          sample.Sample(
              f'{field}_p{pct}',
              float(np.percentile(values, pct)),
              unit,
              metadata,
          )
      )
    results.append(
        sample.Sample(f'{field}_mean', float(np.mean(values)), unit, metadata)
    )

  # Processing throughput: collected messages over their actual time span.
  duration = FLAGS.dpb_dataflow_gpu_duration_per_rate
  processing_throughput = n / duration if duration > 0 else 0.0
  results.append(
      sample.Sample(
          'processing_throughput', processing_throughput, 'msg/s', metadata
      )
  )

  # Estimated cost per hour.
  cost = _EstimateCostPerHour(metadata)
  if cost is not None:
    results.append(
        sample.Sample('estimated_cost_per_hour', cost, 'USD/hr', metadata)
    )

  return results


# GCP on-demand pricing (us-central1). Keep in sync with pricing.json.
# See https://cloud.google.com/compute/all-pricing
_MACHINE_COST_PER_HR = {
    'n1-standard-4': 0.190,
    'n1-standard-8': 0.380,
    'g2-standard-4': 0.707,
    'g2-standard-8': 0.854,
}
# GPU add-on cost for N1 machines (G2 includes L4 in machine price).
_GPU_COST_PER_HR = {
    'nvidia-tesla-t4': 0.35,
    'nvidia-l4': 0.0,  # included in g2 machine price
}
# Vertex AI prediction machine pricing (includes GPU for G2).
_VERTEX_MACHINE_COST_PER_HR = {
    'n1-standard-4': 0.219,
    'n1-standard-8': 0.438,
    'g2-standard-4': 0.813,
    'g2-standard-8': 0.982,
}
_VERTEX_GPU_COST_PER_HR = {
    'nvidia-tesla-t4': 0.402,
    'nvidia-l4': 0.0,
}


def _EstimateCostPerHour(metadata):
  """Returns estimated cost per hour for the benchmark configuration.

  For local_gpu: workers * (machine $/hr + GPU $/hr)
  For vertex_ai: workers * worker_machine $/hr +
                 replicas * (endpoint_machine $/hr + endpoint_GPU $/hr)

  Returns None if pricing data is unavailable for the machine type.
  """
  mode = metadata.get('inference_mode')
  num_workers = metadata.get('num_workers', 1)
  machine_type = metadata.get('worker_machine_type', '')
  gpu_type = metadata.get('gpu_type', '')

  if mode == 'local_gpu':
    machine_cost = _MACHINE_COST_PER_HR.get(machine_type)
    gpu_cost = _GPU_COST_PER_HR.get(gpu_type, 0.0)
    if machine_cost is None:
      return None
    return num_workers * (machine_cost + gpu_cost)

  if mode == 'vertex_ai':
    # Dataflow worker cost (CPU only).
    worker_machine = _ResolveVertexAiWorkerMachine(machine_type)
    worker_cost = _MACHINE_COST_PER_HR.get(worker_machine)
    if worker_cost is None:
      return None
    # Vertex AI endpoint cost.
    vertex_machine = metadata.get('worker_machine_type', machine_type)
    endpoint_machine_cost = _VERTEX_MACHINE_COST_PER_HR.get(vertex_machine)
    endpoint_gpu_cost = _VERTEX_GPU_COST_PER_HR.get(gpu_type, 0.0)
    if endpoint_machine_cost is None:
      return None
    replicas = metadata.get('vertex_replicas', 1)
    return num_workers * worker_cost + replicas * (
        endpoint_machine_cost + endpoint_gpu_cost
    )

  return None
