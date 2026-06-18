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
    for repo in ["adk-repo", "agent-sandbox"]:
        if _exists(["gcloud", "artifacts", "repositories", "describe", repo,
                    f"--location={region}", f"--project={project_id}"]):
            logger.info("AR repo %s already exists.", repo)
            continue
        _run(["gcloud", "artifacts", "repositories", "create", repo,
              "--repository-format=docker",
              f"--location={region}", f"--project={project_id}"])
        logger.info("AR repo %s created.", repo)


def create_cloud_build_sa(project_id):
    logger.info("=== Creating Cloud Build SA ===")
    sa_name = "adk-cloud-build-sa"
    sa_email = f"{sa_name}@{project_id}.iam.gserviceaccount.com"
    if not _exists(["gcloud", "iam", "service-accounts", "describe",
                    sa_email, f"--project={project_id}"]):
        _run(["gcloud", "iam", "service-accounts", "create", sa_name,
              f"--display-name={sa_name}", f"--project={project_id}"])
        logger.info("SA %s created. Waiting for propagation...", sa_email)
        time.sleep(10)
    else:
        logger.info("SA %s already exists.", sa_email)
    roles = [
        "roles/logging.logWriter",
        "roles/storage.objectViewer",
        "roles/artifactregistry.writer",
        "roles/serviceusage.serviceUsageConsumer",
    ]
    for role in roles:
        _run(["gcloud", "projects", "add-iam-policy-binding", project_id,
              f"--member=serviceAccount:{sa_email}",
              f"--role={role}", "--condition=None", "--quiet"], check=False)
    logger.info("Cloud Build SA roles bound.")


def main():
    p = argparse.ArgumentParser(description="GKE Agentic Benchmark Prerequisites")
    p.add_argument("--project_id", required=True, help="GCP project ID")
    p.add_argument("--region", default="us-central1", help="GCP region")
    args = p.parse_args()
    enable_apis(args.project_id)
    create_artifact_registry(args.project_id, args.region)
    create_cloud_build_sa(args.project_id)
    print("\nPrerequisite setup complete!")


if __name__ == "__main__":
    main()

