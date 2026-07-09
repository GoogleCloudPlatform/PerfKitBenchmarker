#!/usr/bin/env python3
"""Prerequisite Setup for GKE Agentic Benchmarking.

Creates infrastructure that PKB cannot manage natively:
  - Enable required GCP APIs
  - Create Artifact Registry repositories
  - Create Cloud Build service account + IAM bindings

Run ONCE before PKB provisioning:
  python -m perfkitbenchmarker.scripts.agentic.gke_prerequisites \
      --project_id=<project> --region=<region>
"""

from __future__ import annotations


import argparse
import logging
import os
import subprocess
import time

from perfkitbenchmarker.scripts.agentic import gke_image_build_utils

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)


def _run(cmd: list[str], check: bool = True, timeout: int = 300) -> object:
    logger.info("CMD: %s", " ".join(cmd))
    result = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout)
    if check and result.returncode != 0:
        logger.error("Command failed (rc=%d): %s", result.returncode, result.stderr[-500:])
        raise RuntimeError(f"Command failed: {cmd}")
    return result


def _exists(cmd: list[str]) -> bool:
    result = subprocess.run(cmd, capture_output=True, text=True, timeout=60)
    return result.returncode == 0


def enable_apis(project_id: str) -> None:
    logger.info("=== Enabling GCP APIs ===")
    apis = [
        "container.googleapis.com",
        "artifactregistry.googleapis.com",
        "cloudbuild.googleapis.com",
        "aiplatform.googleapis.com",
        "storage.googleapis.com",
        "iam.googleapis.com",
        "connectgateway.googleapis.com",
        "gkehub.googleapis.com",
        "gkeconnect.googleapis.com",
        "iap.googleapis.com",
    ]
    _run(["gcloud", "services", "enable"] + apis + [f"--project={project_id}"])
    logger.info("APIs enabled.")


def create_artifact_registry(project_id: str, region: str) -> None:
    logger.info("=== Creating Artifact Registry Repos ===")
    # "adk-repo" is no longer needed here -- PKB creates its own AR repo
    # via container_registry during the Provision stage.
    # Only "agent-sandbox" is needed for Chrome/Router images.
    for repo in ["agent-sandbox"]:
        if _exists(["gcloud", "artifacts", "repositories", "describe", repo,
                    f"--location={region}", f"--project={project_id}"]):
            logger.info("AR repo %s already exists.", repo)
            continue
        _run(["gcloud", "artifacts", "repositories", "create", repo,
              "--repository-format=docker",
              f"--location={region}", f"--project={project_id}"])
        logger.info("AR repo %s created.", repo)


def grant_cloudbuild_sa_permissions(project_id: str) -> None:
    """Grant required IAM roles to the Cloud Build service account(s).

    Auto-detects which SA Cloud Build uses in this project:
      - Legacy projects: {number}@cloudbuild.gserviceaccount.com
      - Newer projects:  {number}-compute@developer.gserviceaccount.com

    Grants permissions to both SAs to ensure compatibility regardless
    of project configuration. This is idempotent and safe.
    """
    logger.info("=== Granting permissions to Cloud Build SA(s) ===")
    result = _run(["gcloud", "projects", "describe", project_id,
                   "--format=value(projectNumber)"])
    project_number = result.stdout.strip()
    if not project_number:
        logger.error("Could not determine project number for %s", project_id)
        return

    # Both possible Cloud Build SAs
    cloudbuild_sa = f"{project_number}@cloudbuild.gserviceaccount.com"
    compute_sa = f"{project_number}-compute@developer.gserviceaccount.com"

    # Detect which SA(s) exist
    sa_emails = []
    for sa in [cloudbuild_sa, compute_sa]:
        if _exists(["gcloud", "iam", "service-accounts", "describe",
                    sa, f"--project={project_id}"]):
            sa_emails.append(sa)
            logger.info("Found Cloud Build SA: %s", sa)
        else:
            logger.info("SA not found (skipping): %s", sa)

    if not sa_emails:
        logger.error("No Cloud Build SA found in project %s", project_id)
        return

    roles = [
        "roles/logging.logWriter",
        "roles/storage.objectViewer",
        "roles/artifactregistry.writer",
        "roles/serviceusage.serviceUsageConsumer",
    ]
    for sa_email in sa_emails:
        logger.info("Granting roles to %s", sa_email)
        for role in roles:
            _run(["gcloud", "projects", "add-iam-policy-binding", project_id,
                  f"--member=serviceAccount:{sa_email}",
                  f"--role={role}", "--condition=None", "--quiet"], check=False)
    logger.info("Cloud Build SA permissions granted.")




def build_sandbox_images(project_id: str, region: str, target_arch: str, arm_builder_subnet_cidr: str | None = None) -> None:
    """Build Chrome Sandbox and Sandbox Router images via Cloud Build."""
    logger.info("=== Building Sandbox Images (arch=%s) ===", target_arch)
    chrome_image = (
        f"{region}-docker.pkg.dev/{project_id}/agent-sandbox/chrome-sandbox:{target_arch}"
    )
    router_image = (
        f"{region}-docker.pkg.dev/{project_id}/agent-sandbox/sandbox-router:{target_arch}"
    )

    gke_image_build_utils._BuildChromeSandboxImage(
        project=project_id,
        region=region,
        target_arch=target_arch,
        image_path=chrome_image,
    )

    gke_image_build_utils._BuildSandboxRouterImage(
        project=project_id,
        region=region,
        target_arch=target_arch,
        image_path=router_image,
    )

    # Build ARM64 base image (heavy pip deps, one-time)
    if target_arch == "arm64":
        arm_base_image = (
            f"{region}-docker.pkg.dev/{project_id}/adk-repo/adk-agent:{target_arch}"
        )
        _build_arm64_on_vm(project_id, region, arm_base_image, arm_builder_subnet_cidr)
        logger.info("  ARM64 ADK Agent: %s", arm_base_image)

    logger.info("Sandbox images built successfully.")
    logger.info("  Chrome: %s", chrome_image)
    logger.info("  Router: %s", router_image)


def _build_arm64_on_vm(project_id: str, region: str, image_path: str, subnet_cidr: str | None = None) -> None:
    """Build ARM64 ADK agent image natively on an ARM VM.

    Creates an ephemeral VPC, subnet, Cloud NAT, and c4a-standard-4 VM.
    Installs Docker, transfers the Dockerfile and source, builds natively
    (~3-5 min), pushes to AR, and cleans up all resources.

    All resource creation is idempotent — skips if already exists.
    """
    vm_name = "pkb-arm64-builder"
    vpc_name = "pkb-arm64-builder-vpc"
    subnet_name = "pkb-arm64-builder-subnet"
    router_name = "pkb-arm64-builder-router"
    nat_name = "pkb-arm64-builder-nat"
    fw_name = vpc_name + "-allow-ssh"
    zone = region + "-c"
    machine_type = "c4a-standard-4"
    cidr = subnet_cidr or "10.200.0.0/24"

    # Absolute path to adk-agent Dockerfile directory.
    # Walk up from script location to find repo root (contains pkb.py).
    _repo = os.path.dirname(os.path.abspath(__file__))
    for _ in range(10):
        if os.path.isfile(os.path.join(_repo, "pkb.py")):
            break
        _repo = os.path.dirname(_repo)
    adk_dir = os.path.join(
        _repo, "perfkitbenchmarker", "data",
        "docker", "agentic", "adk-agent"
    )

    logger.info("Building ARM64 image natively on ARM VM")
    logger.info("  VM: %s (%s in %s)", vm_name, machine_type, zone)
    logger.info("  VPC: %s / %s (%s)", vpc_name, subnet_name, cidr)
    logger.info("  Image: %s", image_path)
    
    # 0. Ensure AR repo exists
    ar_repo = image_path.split("/")[-2]
    if _resource_exists("artifacts", "repositories", ar_repo, project_id, region=region):
        logger.info("AR repo %s already exists.", ar_repo)
    else:
        logger.info("Creating AR repo %s...", ar_repo)
        _run([
            "gcloud", "artifacts", "repositories", "create", ar_repo,
            "--repository-format=docker",
            "--location=" + region,
            "--project=" + project_id,
        ])
    

    try:
        # 1. Create ephemeral VPC (idempotent)
        if _resource_exists("compute", "networks", vpc_name, project_id):
            logger.info("VPC %s already exists, skipping creation.", vpc_name)
        else:
            logger.info("Creating ephemeral VPC...")
            _run([
                "gcloud", "compute", "networks", "create", vpc_name,
                "--subnet-mode=custom",
                "--project=" + project_id,
            ])

        # 2. Create subnet (idempotent)
        if _resource_exists("compute", "networks/subnets", subnet_name, project_id, region=region):
            logger.info("Subnet %s already exists, skipping creation.", subnet_name)
        else:
            logger.info("Creating ephemeral subnet...")
            _run([
                "gcloud", "compute", "networks", "subnets", "create", subnet_name,
                "--network=" + vpc_name,
                "--region=" + region,
                "--range=" + cidr,
                "--project=" + project_id,
            ])

        # 3. Create Cloud Router (required for Cloud NAT) (idempotent)
        if _resource_exists("compute", "routers", router_name, project_id, region=region):
            logger.info("Cloud Router %s already exists, skipping creation.", router_name)
        else:
            logger.info("Creating Cloud Router...")
            _run([
                "gcloud", "compute", "routers", "create", router_name,
                "--network=" + vpc_name,
                "--region=" + region,
                "--project=" + project_id,
            ])

        # 4. Create Cloud NAT (gives VM internet without external IP) (idempotent)
        if _resource_exists("compute", "routers/nats", nat_name, project_id, region=region, router=router_name):
            logger.info("Cloud NAT %s already exists, skipping creation.", nat_name)
        else:
            logger.info("Creating Cloud NAT...")
            _run([
                "gcloud", "compute", "routers", "nats", "create", nat_name,
                "--router=" + router_name,
                "--region=" + region,
                "--auto-allocate-nat-external-ips",
                "--nat-all-subnet-ip-ranges",
                "--project=" + project_id,
            ])

        # 5. Create firewall rule for IAP SSH (idempotent)
        if _resource_exists("compute", "firewall-rules", fw_name, project_id):
            logger.info("Firewall rule %s already exists, skipping creation.", fw_name)
        else:
            logger.info("Creating firewall rule for IAP SSH...")
            _run([
                "gcloud", "compute", "firewall-rules", "create", fw_name,
                "--network=" + vpc_name,
                "--allow=tcp:22",
                "--source-ranges=35.235.240.0/20",
                "--project=" + project_id,
            ])

        # 6. Create ARM VM with no external IP (idempotent)
        if _resource_exists("compute", "instances", vm_name, project_id, zone=zone):
            logger.info("VM %s already exists, skipping creation.", vm_name)
        else:
            logger.info("Creating ARM VM (no external IP)...")
            _run([
                "gcloud", "compute", "instances", "create", vm_name,
                "--machine-type=" + machine_type,
                "--zone=" + zone,
                "--image-family=debian-12-arm64",
                "--image-project=debian-cloud",
                "--boot-disk-size=50GB",
                "--scopes=cloud-platform",
                "--network=" + vpc_name,
                "--subnet=" + subnet_name,
                "--no-address",
                "--project=" + project_id,
            ])

        # 7. Wait for SSH to be ready (via IAP tunnel)
        logger.info("Waiting for SSH to be ready (via IAP)...")
        _wait_for_ssh(vm_name, zone, project_id)

        # 8. Install Docker on the VM
        logger.info("Installing Docker on ARM VM...")
        _run_on_vm(vm_name, zone, project_id,
            "sudo apt-get update -qq && "
            "sudo apt-get install -y -qq docker.io && "
            "sudo usermod -aG docker $USER"
        )

        # 9. Configure Docker for Artifact Registry (using VM service account)
        logger.info("Configuring AR auth on VM...")
        ar_host = region + "-docker.pkg.dev"
        _run_on_vm(vm_name, zone, project_id,
            "sudo gcloud auth configure-docker " + ar_host + " --quiet"
        )

        # 10. Transfer source files to VM
        logger.info("Transferring source files to VM...")
        _run_on_vm(vm_name, zone, project_id, "mkdir -p /tmp/adk-agent")
        _run([
            "gcloud", "compute", "scp", "--recurse",
            "--tunnel-through-iap",
            adk_dir + "/.",
            vm_name + ":/tmp/adk-agent/",
            "--zone=" + zone,
            "--project=" + project_id,
        ])

        # 11. Build the image natively on ARM (uses original Dockerfile)
        logger.info("Building image natively on ARM VM (~3-5 min)...")
        _run_on_vm(vm_name, zone, project_id,
            "cd /tmp/adk-agent && sudo docker build -t " + image_path + " ."
        )

        # 12. Push to Artifact Registry
        logger.info("Pushing image to AR...")
        _run_on_vm(vm_name, zone, project_id,
            "sudo docker push " + image_path
        )

        logger.info("ARM64 image built and pushed: %s", image_path)

    finally:
        # Cleanup: reverse creation order
        logger.info("Cleaning up ARM builder resources...")
        _run([
            "gcloud", "compute", "instances", "delete", vm_name,
            "--zone=" + zone,
            "--project=" + project_id,
            "--quiet",
        ], check=False)
        _run([
            "gcloud", "compute", "firewall-rules", "delete", fw_name,
            "--project=" + project_id,
            "--quiet",
        ], check=False)
        _run([
            "gcloud", "compute", "routers", "nats", "delete", nat_name,
            "--router=" + router_name,
            "--region=" + region,
            "--project=" + project_id,
            "--quiet",
        ], check=False)
        _run([
            "gcloud", "compute", "routers", "delete", router_name,
            "--region=" + region,
            "--project=" + project_id,
            "--quiet",
        ], check=False)
        _run([
            "gcloud", "compute", "networks", "subnets", "delete", subnet_name,
            "--region=" + region,
            "--project=" + project_id,
            "--quiet",
        ], check=False)
        _run([
            "gcloud", "compute", "networks", "delete", vpc_name,
            "--project=" + project_id,
            "--quiet",
        ], check=False)
        logger.info("ARM builder resources cleaned up.")


def _resource_exists(service: str, resource_type: str, name: str, project_id: str, zone: str | None = None, region: str | None = None, router: str | None = None) -> bool:
    """Check if a GCP resource exists. Returns True/False."""
    cmd = ["gcloud", service]

    if resource_type == "networks":
        cmd += ["networks", "describe", name]
    elif resource_type == "networks/subnets":
        cmd += ["networks", "subnets", "describe", name, "--region=" + region]
    elif resource_type == "firewall-rules":
        cmd += ["firewall-rules", "describe", name]
    elif resource_type == "instances":
        cmd += ["instances", "describe", name, "--zone=" + zone]
    elif resource_type == "routers":
        cmd += ["routers", "describe", name, "--region=" + region]
    elif resource_type == "routers/nats":
        cmd += ["routers", "nats", "describe", name, "--router=" + router, "--region=" + region]
    elif resource_type == "repositories":
        cmd += ["repositories", "describe", name, "--location=" + region]        
    else:
        cmd += [resource_type, "describe", name]

    cmd += ["--project=" + project_id]

    result = subprocess.run(cmd, capture_output=True, text=True, timeout=30)
    return result.returncode == 0


def _wait_for_ssh(vm_name: str, zone: str, project_id: str, max_attempts: int = 20, delay: int = 10) -> None:
    """Poll until SSH is ready on the VM via IAP tunnel."""
    for attempt in range(max_attempts):
        try:
            result = subprocess.run(
                [
                    "gcloud", "compute", "ssh", vm_name,
                    "--zone=" + zone,
                    "--project=" + project_id,
                    "--tunnel-through-iap",
                    "--command", "echo ready",
                    "--ssh-flag=-o ConnectTimeout=10",
                    "--ssh-flag=-o StrictHostKeyChecking=no",
                ],
                capture_output=True, text=True, timeout=60,
            )
            if result.returncode == 0:
                logger.info("SSH ready after %d seconds", (attempt + 1) * delay)
                return
        except subprocess.TimeoutExpired:
            pass
        logger.info("  SSH not ready (attempt %d/%d), waiting %ds...",
                     attempt + 1, max_attempts, delay)
        time.sleep(delay)
    raise RuntimeError(
        "SSH to " + vm_name + " not ready after " + str(max_attempts * delay) + "s"
    )


def _run_on_vm(vm_name: str, zone: str, project_id: str, command: str) -> None:
    """Run a command on the ARM VM via gcloud compute ssh through IAP.
    
    Streams stdout/stderr to the terminal in real-time.
    """
    cmd = [
        "gcloud", "compute", "ssh", vm_name,
        "--zone=" + zone,
        "--project=" + project_id,
        "--tunnel-through-iap",
        "--command", command,
        "--ssh-flag=-o StrictHostKeyChecking=no",
    ]
    logger.info("CMD: %s", " ".join(cmd))
    result = subprocess.run(cmd, timeout=600)
    if result.returncode != 0:
        raise RuntimeError("Command failed (rc=" + str(result.returncode) + "): " + " ".join(cmd))

def main() -> None:
    p = argparse.ArgumentParser(description="GKE Agentic Benchmark Prerequisites")
    p.add_argument("--project_id", required=True, help="GCP project ID")
    p.add_argument("--region", default="us-central1", help="GCP region")
    p.add_argument(
        "--target_arch",
        required=True,
        choices=["amd64", "arm64"],
        help="Target CPU architecture for container images (amd64 or arm64)",
    )
    p.add_argument(
        "--skip_image_build",
        action="store_true",
        help="Skip Chrome and Router image builds (images already in registry)",
    )
    p.add_argument(
        "--arm_builder_subnet_cidr",
        default="10.200.0.0/24",
        help="CIDR range for ephemeral ARM builder subnet (default: 10.200.0.0/24)",
    )
    args = p.parse_args()
    enable_apis(args.project_id)
    create_artifact_registry(args.project_id, args.region)
    grant_cloudbuild_sa_permissions(args.project_id)
    if not args.skip_image_build:
        build_sandbox_images(args.project_id, args.region, args.target_arch, args.arm_builder_subnet_cidr)
    else:
        logger.info("Skipping image builds (--skip_image_build)")
    print("\nPrerequisite setup complete!")


if __name__ == "__main__":
    main()

