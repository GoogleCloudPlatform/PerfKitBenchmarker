#!/usr/bin/env python3
"""Post-Teardown Cleanup for GKE Agentic Benchmarking.

Cleans up infrastructure created by gke_prerequisites.py and DeploySnapshots():
  - Delete Cloud Build service account + IAM bindings
  - Delete GCS snapshot bucket
  - Delete Artifact Registry repositories

Run ONCE after all benchmarks are complete (after PKB Teardown has deleted the cluster):
  python -m perfkitbenchmarker.linux_benchmarks.kubernetes.agentic.gke_post_teardown \
      --project_id=<project> --region=<region>
"""

import argparse
import logging
import subprocess

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)


def _run(cmd, check=False, timeout=300):
    logger.info("CMD: %s", " ".join(cmd))
    result = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout)
    if check and result.returncode != 0:
        logger.warning("Command failed (rc=%d): %s", result.returncode, result.stderr[-300:])
    return result


def teardown_cloud_build_sa(project_id):
    logger.info("=== Deleting Cloud Build SA ===")
    sa_email = f"adk-cloud-build-sa@{project_id}.iam.gserviceaccount.com"
    roles = ["roles/logging.logWriter", "roles/storage.objectViewer",
             "roles/artifactregistry.writer", "roles/serviceusage.serviceUsageConsumer"]
    for role in roles:
        _run(["gcloud", "projects", "remove-iam-policy-binding", project_id,
              f"--member=serviceAccount:{sa_email}", f"--role={role}", "--quiet"])
    _run(["gcloud", "iam", "service-accounts", "delete", sa_email,
          f"--project={project_id}", "--quiet"])
    logger.info("Cloud Build SA deleted.")


def teardown_snapshot_bucket(project_id, region):
    logger.info("=== Deleting Snapshot Bucket ===")
    bucket_name = f"agent-sandbox-snapshots-{project_id}"
    _run(["gcloud", "storage", "rm", f"gs://{bucket_name}/**",
          f"--project={project_id}", "--quiet"])
    _run(["gcloud", "storage", "buckets", "delete", f"gs://{bucket_name}",
          f"--project={project_id}", "--quiet"])
    logger.info("Snapshot bucket deleted.")


def teardown_images(project_id, region):
    logger.info("=== Deleting AR repos ===")
    for repo in ["adk-repo", "agent-sandbox"]:
        _run(["gcloud", "artifacts", "repositories", "delete", repo,
              f"--location={region}", f"--project={project_id}", "--quiet"])
    logger.info("AR repos deleted.")


def main():
    p = argparse.ArgumentParser(description="GKE Agentic Benchmark Post-Teardown")
    p.add_argument("--project_id", required=True, help="GCP project ID")
    p.add_argument("--region", default="us-central1", help="GCP region")
    p.add_argument("--keep_images", action="store_true", help="Skip AR repo deletion")
    p.add_argument("--keep_bucket", action="store_true", help="Skip snapshot bucket deletion")
    args = p.parse_args()
    teardown_cloud_build_sa(args.project_id)
    if not args.keep_bucket:
        teardown_snapshot_bucket(args.project_id, args.region)
    if not args.keep_images:
        teardown_images(args.project_id, args.region)
    print("\nPost-teardown complete!")


if __name__ == "__main__":
    main()
