#!/usr/bin/env python3
"""Post-Teardown Cleanup for GKE Agentic Benchmarking.

Cleans up infrastructure created by gke_prerequisites.py and DeploySnapshots():
  - Delete Cloud Build service account + IAM bindings
  - Delete GCS snapshot bucket
  - Delete Artifact Registry repositories

Run ONCE after all benchmarks are complete (after PKB Teardown has deleted the cluster):
  python -m perfkitbenchmarker.scripts.agentic.gke_post_teardown \
      --project_id=<project> --region=<region>
"""

from __future__ import annotations


import argparse
import logging
import subprocess

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)


def _run(cmd: list[str], check: bool = False, timeout: int = 300) -> object:
    logger.info("CMD: %s", " ".join(cmd))
    result = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout)
    if check and result.returncode != 0:
        logger.warning("Command failed (rc=%d): %s", result.returncode, result.stderr[-300:])
    return result


def revoke_cloudbuild_sa_permissions(project_id: str) -> None:
    """Revoke extra IAM roles from Cloud Build SA(s).

    Mirrors grant_cloudbuild_sa_permissions() from gke_prerequisites.py.
    Revokes roles from both possible SAs. Does NOT delete them
    (they are project-managed).
    """
    logger.info("=== Revoking extra permissions from Cloud Build SA(s) ===")
    result = _run(["gcloud", "projects", "describe", project_id,
                   "--format=value(projectNumber)"])
    project_number = result.stdout.strip()
    if not project_number:
        logger.warning("Could not determine project number, skipping SA cleanup")
        return
    sa_emails = [
        f"{project_number}@cloudbuild.gserviceaccount.com",
        f"{project_number}-compute@developer.gserviceaccount.com",
    ]
    roles = ["roles/logging.logWriter", "roles/storage.objectViewer",
             "roles/artifactregistry.writer", "roles/serviceusage.serviceUsageConsumer"]
    for sa_email in sa_emails:
        for role in roles:
            _run(["gcloud", "projects", "remove-iam-policy-binding", project_id,
                  f"--member=serviceAccount:{sa_email}", f"--role={role}", "--quiet"])
    logger.info("Cloud Build SA extra permissions revoked.")


def teardown_snapshot_bucket(project_id: str, region: str) -> None:
    logger.info("=== Deleting Snapshot Bucket ===")
    bucket_name = f"agent-sandbox-snapshots-{project_id}"
    _run(["gcloud", "storage", "rm", f"gs://{bucket_name}/**",
          f"--project={project_id}", "--quiet"])
    _run(["gcloud", "storage", "buckets", "delete", f"gs://{bucket_name}",
          f"--project={project_id}", "--quiet"])
    logger.info("Snapshot bucket deleted.")


def teardown_images(project_id: str, region: str) -> None:
    logger.info("=== Deleting AR repos ===")
    # "adk-repo" is created/deleted by PKB container_registry lifecycle
    # (Provision creates it, Teardown deletes it). If you skip PKB Teardown,
    # run: gcloud artifacts repositories delete adk-repo --location=<region>
    # Only "agent-sandbox" (Chrome + Router images) needs manual cleanup here.
    for repo in ["agent-sandbox"]:
        _run(["gcloud", "artifacts", "repositories", "delete", repo,
              f"--location={region}", f"--project={project_id}", "--quiet"])
    logger.info("AR repos deleted.")


def main() -> None:
    p = argparse.ArgumentParser(description="GKE Agentic Benchmark Post-Teardown")
    p.add_argument("--project_id", required=True, help="GCP project ID")
    p.add_argument("--region", default="us-central1", help="GCP region")
    p.add_argument("--keep_images", action="store_true", help="Skip AR repo deletion")
    p.add_argument("--keep_bucket", action="store_true", help="Skip snapshot bucket deletion")
    args = p.parse_args()
    revoke_cloudbuild_sa_permissions(args.project_id)
    if not args.keep_bucket:
        teardown_snapshot_bucket(args.project_id, args.region)
    if not args.keep_images:
        teardown_images(args.project_id, args.region)
    print("\nPost-teardown complete!")


if __name__ == "__main__":
    main()
