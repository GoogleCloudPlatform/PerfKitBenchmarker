"""Shared workload deployment utilities for GKE Agent Sandbox benchmarks.

Provides idempotent functions to deploy the Agent Sandbox ecosystem
(CRDs, templates, warm pools, router, ADK agent, PSI reader) onto a
pre-provisioned GKE cluster. Called by each benchmark's Prepare() stage.

All functions are idempotent -- safe to call repeatedly without side effects.
"""

from __future__ import annotations


import logging
import os

from absl import flags
from jinja2 import Template
from perfkitbenchmarker import data
from perfkitbenchmarker import vm_util
from perfkitbenchmarker.resources.container_service import kubectl
from perfkitbenchmarker.scripts.agentic import gke_image_build_utils

FLAGS = flags.FLAGS

# ---------------------------------------------------------------------------
# Flags (registered once; shared across all benchmarks)
# ---------------------------------------------------------------------------

flags.DEFINE_string(
    "agent_sandbox_version",
    "v0.4.6",
    "Agent Sandbox controller version (GitHub release tag).",
)

flags.DEFINE_string(
    "agent_sandbox_router_image",
    "",
    "Sandbox router container image. If empty, router deployment is skipped.",
)

flags.DEFINE_string(
    "k8s_agentic_agent_image",
    "",
    "ADK agent container image. If empty, agent deployment is skipped.",
)

flags.DEFINE_string(
    "k8s_agentic_chromium_image",
    "",
    "Chromium sandbox container image. If empty, uses placeholder.",
)

flags.DEFINE_integer(
    "agent_sandbox_warmpool_replicas",
    2,
    "Default warm pool replica count for SandboxWarmPool resources.",
)

flags.DEFINE_integer(
    "agent_sandbox_chromium_replicas",
    1,
    "Default Chromium warm pool replica count.",
)

flags.DEFINE_string(
    "k8s_agentic_python_image",
    "registry.k8s.io/agent-sandbox/python-runtime-sandbox:v0.1.0",
    "Python runtime sandbox container image.",
)

flags.DEFINE_integer(
    "k8s_agentic_deploy_timeout",
    120,
    "Timeout in seconds for workload deployment rollout.",
)




# Module-level derived images (set during DeployWorkloads)
_derived_images = {}

# ---------------------------------------------------------------------------
# Template loading
# ---------------------------------------------------------------------------

_MANIFESTS_DIR = "k8s_agents/manifests"


def _LoadTemplate(template_name: str) -> Template:
    """Load a Jinja2 template from the data directory."""
    template_path = os.path.join(
        data.ResourcePath(_MANIFESTS_DIR), template_name
    )
    with open(template_path, "r") as f:
        return Template(f.read())


def _RenderAndApply(template_name: str, **kwargs: object) -> bool:
    """Load a Jinja2 template, render it, write to file, and kubectl apply."""
    template = _LoadTemplate(template_name)
    rendered = template.render(**kwargs)

    # Write rendered YAML to tmp dir (RunKubectlCommand does not support stdin)
    tmp_dir = os.path.join(
        data.ResourcePath(_MANIFESTS_DIR), "tmp"
    )
    os.makedirs(tmp_dir, exist_ok=True)

    # Strip .j2 extension for the rendered file
    rendered_name = template_name.replace(".j2", "")
    rendered_path = os.path.join(tmp_dir, rendered_name)
    with open(rendered_path, "w") as f:
        f.write(rendered)

    stdout, stderr, retcode = kubectl.RunKubectlCommand(
        ["apply", "-f", rendered_path],
        raise_on_failure=False,
    )
    if retcode != 0:
        logging.warning(
            "kubectl apply failed for %s: %s", template_name, stderr[:500]
        )
    return retcode == 0


flags.DEFINE_bool(
    "skip_deploy_snapshots",
    False,
    "Skip deployment of Pod Snapshot infrastructure. "
    "Set to True on non-GKE clusters where pod snapshots are not supported.",
)

flags.DEFINE_string(
    "k8s_snapshot_ksa_name",
    "pod-snapshot-sa",
    "Kubernetes service account for pod snapshots.",
)


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def _DeriveImagePaths(project: str, region: str, arch: str) -> dict[str, str]:
    """Derive container image paths from cluster config.

    Args:
        project: GCP project ID.
        region: GCP region (e.g. us-central1).
        arch: Docker platform architecture (amd64 or arm64).

    Returns:
        Dict with keys: adk_agent, sandbox_router, chromium.
    """
    return {
        "adk_agent": f"{region}-docker.pkg.dev/{project}/adk-repo/adk-agent:{arch}",
        "sandbox_router": f"{region}-docker.pkg.dev/{project}/agent-sandbox/sandbox-router:{arch}",
        "chromium": f"{region}-docker.pkg.dev/{project}/agent-sandbox/chrome-sandbox:{arch}",
    }

def DeployWorkloads(benchmark_spec: object | None = None) -> None:
    """Deploy the full Agent Sandbox ecosystem onto the GKE cluster.

    Idempotent: safe to call repeatedly. Sequence:
      1. Build images (if --skip_image_build=False)
      2. Create namespace
      3. Install Agent Sandbox CRDs
      4. Deploy SandboxTemplates + WarmPools
      5. Deploy Sandbox Router
      6. Deploy ADK Agent (Deployment + Service + RBAC)
      7. Deploy PSI Reader DaemonSet
      8. Wait for ADK Agent rollout
    """
    ns = FLAGS.k8s_agentic_namespace
    logging.info("=== DeployWorkloads: namespace=%s ===", ns)

    # Derive project, region, machine_type, cluster_name from benchmark_spec
    project = ""
    region = ""
    machine_type = ""
    cluster_name = ""
    cluster = None
    if benchmark_spec:
        cluster = getattr(benchmark_spec, 'container_cluster', None)
        if cluster:
            project = getattr(cluster, 'project', '') or ''
            zone = getattr(cluster, 'zone', '') or ''
            region = zone[:-2] if zone else ''
            cluster_name = getattr(cluster, 'name', '') or ''
            # Prefer sandbox nodepool machine_type
            nodepools = getattr(cluster, 'nodepools', None)
            if nodepools and isinstance(nodepools, dict):
                sandbox_pool = nodepools.get('sandbox')
                if sandbox_pool and hasattr(sandbox_pool, 'vm_spec'):
                    machine_type = getattr(sandbox_pool.vm_spec, 'machine_type', '') or ''
            if not machine_type and hasattr(cluster, 'vm_spec'):
                machine_type = getattr(cluster.vm_spec, 'machine_type', '') or ''
    # Fallback to global FLAGS if benchmark_spec not available
    if not project:
        project = getattr(FLAGS, 'project', '') or ''
    if not region:
        zone = getattr(FLAGS, 'zone', '') or ''
        region = zone[:-2] if zone else ''

    # Derive image paths for template rendering.
    # Chrome and Router images are built during prerequisites
    # (perfkitbenchmarker.scripts.agentic.gke_prerequisites), not during Prepare.
    # ADK agent image is built by PKB container_specs during Provision.
    arch = FLAGS.target_arch or "amd64"
    global _derived_images
    _derived_images = _DeriveImagePaths(project, region, arch)
    logging.info(
        "DeployWorkloads: project=%s region=%s arch=%s",
        project, region, arch,
    )
    logging.info("_derived_images: %s", _derived_images)

    _CreateNamespace(ns)
    _InstallCRDs()
    _DeploySandboxTemplates(ns)
    _DeploySandboxRouter(ns)
    # Prefer ADK image from PKB-native container_specs (built during Provision).
    # Falls back to FLAGS.k8s_agentic_agent_image or derived image path.
    adk_image_from_specs = ""
    if benchmark_spec:
        specs = getattr(benchmark_spec, "container_specs", {})
        adk_spec = specs.get("adk_agent")
        if adk_spec and getattr(adk_spec, "image", None):
            adk_image_from_specs = adk_spec.image
            logging.info("Using ADK image from container_specs: %s", adk_image_from_specs)
    _DeployADKAgent(ns, project=project, region=region, cluster_name=cluster_name, adk_image_override=adk_image_from_specs)
    _DeployPSIReader(ns)
    _WaitForAgentReady(ns)

    logging.info("DeployWorkloads complete.")


def DeploySnapshots() -> None:
    """Deploy Pod Snapshot infrastructure.

    Idempotent: safe to call repeatedly. Sequence:
      1. Create GCS bucket (hierarchical namespace)
      2. Create managed folder
      3. Create KSA for snapshots
      4. Bind IAM roles
      5. Deploy PodSnapshotStorageConfig + PodSnapshotPolicy
    """
    if FLAGS.skip_deploy_snapshots:
        logging.info("Skipping snapshot infrastructure (--skip_deploy_snapshots=True).")
        return

    ns = FLAGS.k8s_agentic_namespace
    project = getattr(FLAGS, 'project', '') or ''
    zone = getattr(FLAGS, 'zone', '') or ''
    region = zone[:-2] if zone else ''

    if not project:
        logging.warning("DeploySnapshots: FLAGS.project not set, skipping.")
        return

    bucket_name = "agent-sandbox-snapshots-{}".format(project)
    snapshot_folder = "benchmark-snapshots"
    ksa_name = FLAGS.k8s_snapshot_ksa_name

    logging.info("=== DeploySnapshots: bucket=%s ===", bucket_name)

    # 1. Create GCS bucket
    vm_util.IssueCommand(
        [
            "gcloud", "storage", "buckets", "create",
            "gs://{}".format(bucket_name),
            "--uniform-bucket-level-access",
            "--enable-hierarchical-namespace",
            "--soft-delete-duration=0d",
            "--location={}".format(region),
            "--project={}".format(project),
        ],
        raise_on_failure=False,
    )

    # 2. Create managed folder
    vm_util.IssueCommand(
        [
            "gcloud", "storage", "managed-folders", "create",
            "gs://{}/{}/".format(bucket_name, snapshot_folder),
            "--project={}".format(project),
        ],
        raise_on_failure=False,
    )

    # 3. Create KSA
    kubectl.RunKubectlCommand(
        ["create", "serviceaccount", ksa_name, "--namespace", ns],
        raise_on_failure=False,
    )

    # 4. IAM bindings
    project_number = _GetProjectNumber(project)
    if project_number:
        _BindSnapshotIAM(bucket_name, project, project_number, ns, ksa_name)

    # 5. Deploy PSSC + PSP
    _RenderAndApply(
        "snapshot-crds.yaml.j2",
        ns=ns,
        bucket_name=bucket_name,
        snapshot_folder=snapshot_folder,
    )

    logging.info("DeploySnapshots complete.")


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


def _CreateNamespace(ns: str) -> None:
    """Create namespace if it doesn't exist."""
    kubectl.RunKubectlCommand(
        ["create", "namespace", ns],
        raise_on_failure=False,
    )


def _InstallCRDs() -> None:
    """Install Agent Sandbox CRDs from GitHub release."""
    version = FLAGS.agent_sandbox_version
    base_url = (
        "https://github.com/kubernetes-sigs/agent-sandbox"
        "/releases/download/{}".format(version)
    )
    logging.info("Installing Agent Sandbox CRDs (%s)", version)
    kubectl.RunKubectlCommand(
        [
            "apply",
            "-f", "{}/manifest.yaml".format(base_url),
            "-f", "{}/extensions.yaml".format(base_url),
        ],
        raise_on_failure=False,
    )


def _DeploySandboxTemplates(ns: str) -> None:
    """Deploy SandboxTemplate + WarmPool for Python and Chromium."""
    python_image = FLAGS.k8s_agentic_python_image
    chromium_image = FLAGS.k8s_agentic_chromium_image or _derived_images.get("chromium", "chromium-placeholder:latest")
    warmpool_replicas = FLAGS.agent_sandbox_warmpool_replicas
    chromium_replicas = FLAGS.agent_sandbox_chromium_replicas

    _RenderAndApply(
        "sandbox-templates.yaml.j2",
        ns=ns,
        python_image=python_image,
        chromium_image=chromium_image,
        warmpool_replicas=warmpool_replicas,
        chromium_replicas=chromium_replicas,
    )


def _DeploySandboxRouter(ns: str) -> None:
    """Deploy the Sandbox Router Deployment + Service."""
    router_image = FLAGS.agent_sandbox_router_image or _derived_images.get("sandbox_router", "")
    if not router_image:
        logging.info("Sandbox router image not set, skipping router deployment.")
        return

    _RenderAndApply(
        "sandbox-router.yaml.j2",
        ns=ns,
        router_image=router_image,
    )


def _DeployADKAgent(ns: str, project: str = "", region: str = "", cluster_name: str = "", adk_image_override: str = "") -> None:
    """Deploy ADK Agent: SA, ClusterRole, RoleBinding, Deployment, Service."""

    # Prefer explicit --k8s_agentic_agent_image when set (e.g., pre-built ARM images)
    if FLAGS.k8s_agentic_agent_image:
        adk_image = FLAGS.k8s_agentic_agent_image
        logging.info("Using explicit k8s_agentic_agent_image: %s", adk_image)
    elif adk_image_override:
        adk_image = adk_image_override
        logging.info("Using ADK image from container_specs: %s", adk_image)
    else:
        adk_image = _derived_images.get("adk_agent", "")

    # Validate the image looks like a registry path, not a Dockerfile path.
    if adk_image and "docker.pkg.dev" not in adk_image:
        derived = _derived_images.get("adk_agent", "")
        if derived:
            logging.warning(
                "ADK image %s is not a registry path. Using derived: %s",
                adk_image, derived,
            )
            adk_image = derived

    if not adk_image:
        logging.info("ADK agent image not set, skipping agent deployment.")
        return

    logging.info("Using ADK image: %s", adk_image)

    project = project or ""
    region = region or ""
    cluster = cluster_name or ""

    _RenderAndApply(
        "adk-agent.yaml.j2",
        ns=ns,
        adk_image=adk_image,
        project=project,
        region=region,
        cluster=cluster,
    )


def _DeployPSIReader(ns: str) -> None:
    """Deploy PSI Reader DaemonSet for cgroup pressure metrics."""
    _RenderAndApply("psi-reader.yaml.j2", ns=ns)


def _WaitForAgentReady(ns: str) -> None:
    """Wait for ADK agent deployment to be ready.

    Always attempts the rollout wait regardless of how the image was
    specified (FLAGS.k8s_agentic_agent_image, container_specs, or _derived_images).
    kubectl rollout status returns non-zero harmlessly if the deployment
    does not exist, and raise_on_failure=False prevents that from
    propagating.
    """
    timeout = FLAGS.k8s_agentic_deploy_timeout
    logging.info("Waiting for adk-agent rollout (timeout=%ds)...", timeout)
    _, stderr, retcode = kubectl.RunKubectlCommand(
        [
            "rollout", "status", "deployment/adk-agent",
            "-n", ns,
            "--timeout={}s".format(timeout),
        ],
        raise_on_failure=False,
    )
    if retcode != 0:
        logging.warning(
            "adk-agent rollout status returned %d: %s",
            retcode, stderr.strip()[:200],
        )


def _GetProjectNumber(project: str) -> str | None:
    """Get GCP project number from project ID."""
    stdout, _, retcode = vm_util.IssueCommand(
        [
            "gcloud", "projects", "describe", project,
            "--format=value(projectNumber)",
        ],
        raise_on_failure=False,
    )
    return stdout.strip() if retcode == 0 else None


def _BindSnapshotIAM(bucket_name: str, project: str, project_number: str, ns: str, ksa_name: str) -> None:
    """Bind IAM roles for pod snapshot access."""
    # bucketViewer to namespace
    vm_util.IssueCommand(
        [
            "gcloud", "storage", "buckets", "add-iam-policy-binding",
            "gs://{}".format(bucket_name),
            "--member=principalSet://iam.googleapis.com/projects/{}"
            "/locations/global/workloadIdentityPools/{}.svc.id.goog"
            "/namespace/{}".format(project_number, project, ns),
            "--role=roles/storage.bucketViewer",
            "--quiet",
        ],
        raise_on_failure=False,
    )

    # objectAdmin to KSA
    vm_util.IssueCommand(
        [
            "gcloud", "storage", "buckets", "add-iam-policy-binding",
            "gs://{}".format(bucket_name),
            "--member=principal://iam.googleapis.com/projects/{}"
            "/locations/global/workloadIdentityPools/{}.svc.id.goog"
            "/subject/ns/{}/sa/{}".format(project_number, project, ns, ksa_name),
            "--role=roles/storage.objectAdmin",
            "--quiet",
        ],
        raise_on_failure=False,
    )

    # objectUser to GKE snapshot controller
    vm_util.IssueCommand(
        [
            "gcloud", "storage", "buckets", "add-iam-policy-binding",
            "gs://{}".format(bucket_name),
            "--member=serviceAccount:service-{}"
            "@container-engine-robot.iam.gserviceaccount.com".format(project_number),
            "--role=roles/storage.objectUser",
            "--quiet",
        ],
        raise_on_failure=False,
    )
