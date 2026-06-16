"""Shared image build utilities for GKE Agent Sandbox benchmarks.

Builds and pushes container images (ADK agent, Chrome sandbox, Sandbox Router)
via Google Cloud Build. Called from:
  - Provision() when --gke_skip_image_build is False (via BuildImages())
  - prerequisite_setup.py (via build_images_with_config())

Images built:
  - ADK Agent: perfkitbenchmarker/data/k8s_agents/workloads/adk_agent/ -> {region}-docker.pkg.dev/{project}/adk-repo/adk-agent:{arch}
  - Chrome Sandbox: cloned from agent-sandbox repo -> {region}-docker.pkg.dev/{project}/agent-sandbox/chrome-sandbox:{arch}
  - Sandbox Router: cloned from agent-sandbox repo -> {region}-docker.pkg.dev/{project}/agent-sandbox/sandbox-router:{arch}
"""

import logging
import os
import shutil
import subprocess
import tempfile

from absl import flags

FLAGS = flags.FLAGS

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def build_images_with_config(project, region, machine_type, cloud_build_sa=None):
    """Core image build logic — no FLAGS dependency.

    Callable from both PKB (via BuildImages()) and prerequisite_setup.py.

    Args:
        project: GCP project ID.
        region: GCP region (e.g. "us-central1").
        machine_type: Machine type string (e.g. "c4-standard-8").
            Used to derive target architecture (arm64 for c4a, amd64 otherwise).
        cloud_build_sa: Cloud Build service account email.
            If None, defaults to "adk-cloud-build-sa@{project}.iam.gserviceaccount.com".
    """
    # Derive architecture from machine family
    machine_family = machine_type.split("-")[0] if machine_type else "c4"
    target_arch = "arm64" if machine_family == "c4a" else "amd64"

    # Derive image paths
    adk_image = f"{region}-docker.pkg.dev/{project}/adk-repo/adk-agent:{target_arch}"
    chrome_image = (
        f"{region}-docker.pkg.dev/{project}/agent-sandbox/chrome-sandbox:{target_arch}"
    )
    router_image = (
        f"{region}-docker.pkg.dev/{project}/agent-sandbox/sandbox-router:{target_arch}"
    )

    # Cloud Build SA
    if cloud_build_sa is None:
        cloud_build_sa = f"adk-cloud-build-sa@{project}.iam.gserviceaccount.com"

    logger.info("=== Building Container Images ===")
    logger.info("  Project: %s", project)
    logger.info("  Region: %s", region)
    logger.info("  Architecture: %s", target_arch)
    logger.info("  Cloud Build SA: %s", cloud_build_sa)

    # 1. Build ADK Agent
    _BuildADKAgentImage(
        project=project,
        region=region,
        target_arch=target_arch,
        image_path=adk_image,
        cloud_build_sa=cloud_build_sa,
        machine_type=machine_type,
    )

    # 2. Build Chrome Sandbox
    _BuildChromeSandboxImage(
        project=project,
        region=region,
        target_arch=target_arch,
        image_path=chrome_image,
        cloud_build_sa=cloud_build_sa,
    )

    # 3. Build Sandbox Router
    _BuildSandboxRouterImage(
        project=project,
        region=region,
        target_arch=target_arch,
        image_path=router_image,
        cloud_build_sa=cloud_build_sa,
    )

    logger.info("=== All images built successfully ===")
    logger.info("  ADK Agent:      %s", adk_image)
    logger.info("  Chrome Sandbox: %s", chrome_image)
    logger.info("  Sandbox Router: %s", router_image)


def BuildImages():
    """FLAGS-based entry point (called from PKB Provision).

    Reads configuration from FLAGS (set in gke_provision_utils.py).
    Delegates to build_images_with_config() for the actual work.
    """
    build_images_with_config(
        project=FLAGS.gke_project_id,
        region=FLAGS.gke_region,
        machine_type=FLAGS.gke_sandbox_machine_type,
    )


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


def _BuildADKAgentImage(
    project, region, target_arch, image_path, cloud_build_sa, machine_type=None
):
    """Build and push the ADK Agent image.

    Uses the existing perfkitbenchmarker/data/k8s_agents/workloads/adk_agent/cloudbuild.yaml with --substitutions
    rather than generating a new one (avoids overwriting the committed file).
    """
    logger.info("Building ADK Agent image: %s", image_path)

    # Locate the agent source directory
    # Expected layout: repo_root/perfkitbenchmarker/data/k8s_agents/workloads/adk_agent/
    repo_root = _FindRepoRoot()
    agent_dir = os.path.join(repo_root, "perfkitbenchmarker", "data", "k8s_agents", "workloads", "adk_agent")

    if not os.path.isdir(agent_dir):
        raise RuntimeError(
            f"ADK agent source not found at {agent_dir}. "
            "Ensure you are running from the repository root."
        )

    # Generate generated.env from template
    _GenerateEnvFile(agent_dir, project, region, machine_type=machine_type)

    # Use the existing cloudbuild.yaml with substitutions (don't overwrite)
    cloudbuild_path = os.path.join(agent_dir, "cloudbuild.yaml")
    if not os.path.isfile(cloudbuild_path):
        raise RuntimeError(
            f"cloudbuild.yaml not found at {cloudbuild_path}. "
            "Expected perfkitbenchmarker/data/k8s_agents/workloads/adk_agent/cloudbuild.yaml to exist."
        )

    _RunCmd(
        [
            "gcloud",
            "builds",
            "submit",
            agent_dir,
            f"--config={cloudbuild_path}",
            f"--substitutions=_IMAGE_PATH={image_path},_PLATFORM=linux/{target_arch}",
            f"--project={project}",
            f"--service-account=projects/{project}/serviceAccounts/{cloud_build_sa}",
        ]
    )

    logger.info("ADK Agent image built successfully.")


def _BuildChromeSandboxImage(project, region, target_arch, image_path, cloud_build_sa):
    """Build and push the Chrome Sandbox image."""
    logger.info("Building Chrome Sandbox image: %s", image_path)

    tmp_dir = tempfile.mkdtemp(prefix="chrome-sandbox-")
    try:
        # Clone agent-sandbox repo (sparse checkout)
        logger.info("Cloning agent-sandbox chrome-sandbox source...")
        _RunCmd(
            [
                "git",
                "clone",
                "--depth",
                "1",
                "--filter=blob:none",
                "--sparse",
                "https://github.com/kubernetes-sigs/agent-sandbox.git",
                tmp_dir,
            ]
        )
        _RunCmd(
            ["git", "sparse-checkout", "set", "examples/chrome-sandbox"],
            cwd=tmp_dir,
        )

        build_dir = os.path.join(tmp_dir, "examples", "chrome-sandbox")
        if not os.path.isfile(os.path.join(build_dir, "Dockerfile")):
            raise RuntimeError(f"chrome-sandbox Dockerfile not found at {build_dir}")

        # Patch Dockerfile: add socat for CDP proxy
        dockerfile_path = os.path.join(build_dir, "Dockerfile")
        with open(dockerfile_path, "r") as f:
            content = f.read()
        content = content.replace(
            "RUN apt-get update && apt-get install --yes --no-install-recommends chromium",
            "RUN apt-get update && apt-get install --yes --no-install-recommends chromium socat",
        )
        with open(dockerfile_path, "w") as f:
            f.write(content)

        # Submit Cloud Build (generates cloudbuild.yaml in temp dir)
        _SubmitCloudBuild(
            source_dir=build_dir,
            image_path=image_path,
            target_arch=target_arch,
            project=project,
            cloud_build_sa=cloud_build_sa,
        )

        logger.info("Chrome Sandbox image built successfully.")
    finally:
        shutil.rmtree(tmp_dir, ignore_errors=True)


def _BuildSandboxRouterImage(project, region, target_arch, image_path, cloud_build_sa):
    """Build and push the Sandbox Router image."""
    logger.info("Building Sandbox Router image: %s", image_path)

    tmp_dir = tempfile.mkdtemp(prefix="sandbox-router-")
    try:
        # Clone agent-sandbox repo (sparse checkout)
        logger.info("Cloning agent-sandbox router source...")
        _RunCmd(
            [
                "git",
                "clone",
                "--depth",
                "1",
                "--filter=blob:none",
                "--sparse",
                "https://github.com/kubernetes-sigs/agent-sandbox.git",
                tmp_dir,
            ]
        )
        _RunCmd(
            [
                "git",
                "sparse-checkout",
                "set",
                "clients/python/agentic-sandbox-client/sandbox-router",
            ],
            cwd=tmp_dir,
        )

        build_dir = os.path.join(
            tmp_dir, "clients", "python", "agentic-sandbox-client", "sandbox-router"
        )
        if not os.path.isfile(os.path.join(build_dir, "Dockerfile")):
            raise RuntimeError(f"sandbox-router Dockerfile not found at {build_dir}")

        # Submit Cloud Build (generates cloudbuild.yaml in temp dir)
        _SubmitCloudBuild(
            source_dir=build_dir,
            image_path=image_path,
            target_arch=target_arch,
            project=project,
            cloud_build_sa=cloud_build_sa,
        )

        logger.info("Sandbox Router image built successfully.")
    finally:
        shutil.rmtree(tmp_dir, ignore_errors=True)


def _GenerateEnvFile(
    agent_dir, project, region, machine_type=None, namespace="agentic"
):
    """Render generated.env from template with current config values."""
    template_path = os.path.join(agent_dir, "generated.env.template")
    output_path = os.path.join(agent_dir, "generated.env")

    if not os.path.isfile(template_path):
        logger.warning(
            "generated.env.template not found at %s, skipping.", template_path
        )
        return

    with open(template_path, "r") as f:
        content = f.read()

    # Derive cluster name
    machine_family = machine_type.split("-")[0] if machine_type else "c4"
    suffix_map = {"c3": "c3metal", "c4": "c4", "c4d": "c4d", "c4a": "c4a"}
    cluster_suffix = suffix_map.get(machine_family, "c4")

    # Get username prefix for cluster name
    user = os.environ.get("USER", "benchmark")
    user_prefix = user.split(".")[0] if "." in user else user
    cluster_name = f"{user_prefix}-agentic-{cluster_suffix}"

    # Substitute variables
    replacements = {
        "${CLUSTER_NAME}": cluster_name,
        "${GOOGLE_CLOUD_PROJECT}": project,
        "${GOOGLE_CLOUD_LOCATION}": region,
        "${AGENTIC_NAMESPACE}": namespace,
        "${GOOGLE_GENAI_USE_VERTEXAI}": "true",
        "${SANDBOX_ROUTER_URL}": f"http://sandbox-router-svc.{namespace}.svc.cluster.local:8080",
        "${SAMPLE_COUNT}": "20",
        "${SAMPLE_WARMUP}": "0",
        "${PAYLOAD_SIZE_MB}": "1",
        "${PAYLOAD_ITERATIONS}": "20",
    }

    for key, value in replacements.items():
        content = content.replace(key, value)

    with open(output_path, "w") as f:
        f.write(content)

    logger.info("Generated %s", output_path)


def _SubmitCloudBuild(source_dir, image_path, target_arch, project, cloud_build_sa):
    """Generate a cloudbuild.yaml with substitutions and submit via Cloud Build.

    Used for Chrome and Router images (built in temp directories).
    The ADK agent uses its own committed cloudbuild.yaml instead.
    """
    cloudbuild_content = """steps:
  - name: 'gcr.io/cloud-builders/docker'
    args: ['build', '--platform', '${_PLATFORM}', '-t', '${_IMAGE_PATH}', '.']
    env:
      - 'DOCKER_BUILDKIT=1'
images:
  - '${_IMAGE_PATH}'
options:
  logging: CLOUD_LOGGING_ONLY
substitutions:
  _IMAGE_PATH: ''
  _PLATFORM: 'linux/amd64'
"""
    cloudbuild_path = os.path.join(source_dir, "cloudbuild.yaml")
    with open(cloudbuild_path, "w") as f:
        f.write(cloudbuild_content)

    _RunCmd(
        [
            "gcloud",
            "builds",
            "submit",
            source_dir,
            f"--config={cloudbuild_path}",
            f"--substitutions=_IMAGE_PATH={image_path},_PLATFORM=linux/{target_arch}",
            f"--project={project}",
            f"--service-account=projects/{project}/serviceAccounts/{cloud_build_sa}",
        ]
    )


def _FindRepoRoot():
    """Find the repository root by looking for known markers."""
    # Try relative to this file
    this_dir = os.path.dirname(os.path.abspath(__file__))
    # Expected: perfkitbenchmarker/linux_benchmarks/ -> go up 2 levels
    candidate = os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(this_dir))))
    if os.path.isdir(os.path.join(candidate, "perfkitbenchmarker", "data", "k8s_agents", "workloads", "adk_agent")):
        return candidate

    # Try CWD
    cwd = os.getcwd()
    if os.path.isdir(os.path.join(cwd, "perfkitbenchmarker", "data", "k8s_agents", "workloads", "adk_agent")):
        return cwd

    # Try parent of CWD
    parent = os.path.dirname(cwd)
    if os.path.isdir(os.path.join(parent, "perfkitbenchmarker", "data", "k8s_agents", "workloads", "adk_agent")):
        return parent

    raise RuntimeError(
        "Cannot locate repository root (looking for perfkitbenchmarker/data/k8s_agents/workloads/adk_agent/). "
        "Run from the repository root directory."
    )


def _RunCmd(cmd, cwd=None):
    """Run a shell command, raising on failure."""
    logger.info("  CMD: %s", " ".join(cmd))
    env = os.environ.copy()
    env["CLOUDSDK_AUTH_DISABLE_SSL_VALIDATION"] = "true"
    proc = subprocess.run(
        cmd,
        capture_output=True,
        text=True,
        cwd=cwd,
        timeout=600,
        env=env,
    )

    if proc.returncode != 0:
        raise RuntimeError(
            f"Command failed (rc={proc.returncode}): {' '.join(cmd)}\n"
            f"stderr: {proc.stderr[-500:]}"
        )
    return proc.stdout
