#!/usr/bin/env python3
"""Prerequisite Setup for GKE Agentic Benchmarking.

Creates infrastructure that PKB cannot manage natively:
  - Enable required GCP APIs
  - Create Artifact Registry repositories
  - Create Cloud Build service account + IAM bindings

Run ONCE before PKB provisioning:
  python -m perfkitbenchmarker.linux_benchmarks.kubernetes.agentic.gke_prerequisites \
      --project_id=<project> --region=<region>
"""

import argparse
import logging
import os
import subprocess
import time

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)


def _run(cmd, check=True, timeout=300):
    logger.info("CMD: %s", " ".join(cmd))
    result = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout)
    if check and result.returncode != 0:
        logger.error("Command failed (rc=%d): %s", result.returncode, result.stderr[-500:])
        raise RuntimeError(f"Command failed: {cmd}")
    return result


def _exists(cmd):
    result = subprocess.run(cmd, capture_output=True, text=True, timeout=60)
    return result.returncode == 0


def enable_apis(project_id):
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


def create_artifact_registry(project_id, region):
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


def grant_cloudbuild_sa_permissions(project_id):
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




def build_sandbox_images(project_id, region, target_arch):
    """Build Chrome Sandbox and Sandbox Router images via Cloud Build."""
    logger.info("=== Building Sandbox Images (arch=%s) ===", target_arch)
    from perfkitbenchmarker.linux_benchmarks.kubernetes.agentic import gke_image_build_utils

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

    logger.info("Sandbox images built successfully.")
    logger.info("  Chrome: %s", chrome_image)
    logger.info("  Router: %s", router_image)

def main():
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
    args = p.parse_args()
    enable_apis(args.project_id)
    create_artifact_registry(args.project_id, args.region)
    grant_cloudbuild_sa_permissions(args.project_id)
    if not args.skip_image_build:
        build_sandbox_images(args.project_id, args.region, args.target_arch)
    else:
        logger.info("Skipping image builds (--skip_image_build)")
    print("\nPrerequisite setup complete!")


if __name__ == "__main__":
    main()

