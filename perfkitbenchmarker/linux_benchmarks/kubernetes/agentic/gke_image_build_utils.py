"""Shared image build utilities for GKE Agent Sandbox benchmarks.

Builds and pushes container images (Chrome sandbox, Sandbox Router) via
Google Cloud Build. Called from gke_deploy_utils.DeployWorkloads() during
the Prepare stage.

NOTE: The ADK Agent image is built by the PKB native container_specs
mechanism during the Provision stage, not by this module.

Images built:
  - Chrome Sandbox: cloned from agent-sandbox repo
  - Sandbox Router: cloned from agent-sandbox repo
"""

import logging
import os
import shutil
import subprocess
import tempfile

from absl import flags
from perfkitbenchmarker import vm_util

FLAGS = flags.FLAGS

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Architecture detection
# ---------------------------------------------------------------------------

flags.DEFINE_string(
    "target_arch",
    "",
    "Target CPU architecture for container images (amd64 or arm64). "
    "If set, skips gcloud machine-type detection. "
    "Use this for non-GCP environments or when gcloud is unavailable.",
)

_ARCH_MAP = {
    "X86_64": "amd64",
    "ARM64": "arm64",
}


def _DetectArchitecture(machine_type, zone, project):
    """Detect CPU architecture for a GCP machine type.

    Uses gcloud to query the machine type's architecture, then maps
    GCP naming (X86_64/ARM64) to Docker platform naming (amd64/arm64).

    Falls back to amd64 if gcloud fails.
    """
    # Quick exit if user provided arch explicitly
    if FLAGS.target_arch:
        arch = FLAGS.target_arch.lower()
        if arch in ("amd64", "arm64"):
            logging.info("Using user-provided target_arch: %s", arch)
            return arch
        logging.warning(
            "Invalid --target_arch='%s'. Must be amd64 or arm64. "
            "Proceeding with gcloud detection.",
            FLAGS.target_arch,
        )

    try:
        stdout, _, retcode = vm_util.IssueCommand(
            [
                "gcloud",
                "compute",
                "machine-types",
                "describe",
                machine_type,
                f"--zone={zone}",
                f"--project={project}",
                "--format=value(architecture)",
            ],
            raise_on_failure=False,
            timeout=30,
        )
        if retcode == 0 and stdout.strip():
            gcp_arch = stdout.strip().upper()
            docker_arch = _ARCH_MAP.get(gcp_arch)
            if docker_arch:
                logging.info(
                    "Detected architecture for %s: %s -> %s",
                    machine_type,
                    gcp_arch,
                    docker_arch,
                )
                return docker_arch
            logging.warning(
                "Unknown GCP architecture '%s' for %s. Falling back to amd64.",
                gcp_arch,
                machine_type,
            )
    except Exception as e:
        logging.warning(
            "gcloud machine-type describe failed: %s. Falling back to amd64.", e
        )

    return "amd64"


def build_images_with_config(project, region, machine_type, zone, arch):
    """Core image build logic — no FLAGS dependency.

    Callable from both PKB (via BuildImages()) and prerequisite_setup.py.
    Uses the project's default Cloud Build SA (no custom SA needed).

    Args:
        project: GCP project ID.
        region: GCP region (e.g. "us-central1").
        machine_type: Machine type string (e.g. "c4-standard-8").
            Used to derive target architecture (arm64 for c4a, amd64 otherwise).
    """
    # Architecture passed in from caller (detected via gcloud)
    target_arch = arch

    # Derive image paths
    adk_image = f"{region}-docker.pkg.dev/{project}/adk-repo/adk-agent:{target_arch}"
    chrome_image = (
        f"{region}-docker.pkg.dev/{project}/agent-sandbox/chrome-sandbox:{target_arch}"
    )
    router_image = (
        f"{region}-docker.pkg.dev/{project}/agent-sandbox/sandbox-router:{target_arch}"
    )

    logger.info("=== Building Container Images (Chrome + Router only) ===")
    logger.info("  Project: %s", project)
    logger.info("  Region: %s", region)
    logger.info("  Architecture: %s", target_arch)
    logger.info("  Cloud Build SA: default (project Cloud Build SA)")
    logger.info("  NOTE: ADK Agent image is built by PKB via container_specs")

    # 1. Build Chrome Sandbox
    _BuildChromeSandboxImage(
        project=project,
        region=region,
        target_arch=target_arch,
        image_path=chrome_image,
    )

    # 3. Build Sandbox Router
    _BuildSandboxRouterImage(
        project=project,
        region=region,
        target_arch=target_arch,
        image_path=router_image,
    )

    logger.info("=== Chrome + Router images built successfully ===")
    logger.info("  Chrome Sandbox: %s", chrome_image)
    logger.info("  Sandbox Router: %s", router_image)
    logger.info("  (ADK Agent built by PKB via container_specs)")


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


def _BuildChromeSandboxImage(project, region, target_arch, image_path):
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
        )

        logger.info("Chrome Sandbox image built successfully.")
    finally:
        shutil.rmtree(tmp_dir, ignore_errors=True)


def _BuildSandboxRouterImage(project, region, target_arch, image_path):
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
        )

        logger.info("Sandbox Router image built successfully.")
    finally:
        shutil.rmtree(tmp_dir, ignore_errors=True)


def _SubmitCloudBuild(source_dir, image_path, target_arch, project):
    """Generate a cloudbuild.yaml with substitutions and submit via Cloud Build.

    Used for Chrome and Router images (built in temp directories).
    Uses the project's default Cloud Build SA.

    For cross-architecture builds (e.g. arm64 on amd64 workers), uses
    QEMU emulation + Docker Buildx to produce the target-arch image.
    A high-CPU machine type (E2_HIGHCPU_32) is used to offset the
    overhead of QEMU instruction translation.
    """
    if target_arch == "amd64":
        # Native build — no emulation needed
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
    else:
        # Cross-arch build — QEMU + Buildx required.
        # Cloud Build workers are amd64; QEMU registers binfmt handlers
        # so the kernel can execute arm64 binaries transparently.
        # E2_HIGHCPU_32 provides 32 vCPUs to offset emulation overhead.
        # Buildx --push handles the registry push directly, so no
        # top-level 'images:' key is needed.
        cloudbuild_content = """steps:
  - name: 'gcr.io/cloud-builders/docker'
    args: ['run', '--privileged', 'multiarch/qemu-user-static', '--reset', '-p', 'yes']
    id: 'qemu-setup'
  - name: 'gcr.io/cloud-builders/docker'
    args: ['buildx', 'create', '--use', '--name', 'multiarch-builder']
    id: 'create-builder'
    waitFor: ['qemu-setup']
  - name: 'gcr.io/cloud-builders/docker'
    args: ['buildx', 'build', '--platform', '${_PLATFORM}', '-t', '${_IMAGE_PATH}', '--push', '.']
    id: 'build-and-push'
    waitFor: ['create-builder']
options:
  logging: CLOUD_LOGGING_ONLY
  machineType: E2_HIGHCPU_32
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
        ]
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
        timeout=2400,  # 40 min: allows for QEMU cross-arch builds
        env=env,
    )

    if proc.returncode != 0:
        raise RuntimeError(
            f"Command failed (rc={proc.returncode}): {' '.join(cmd)}\n"
            f"stderr: {proc.stderr[-500:]}"
        )
    return proc.stdout
