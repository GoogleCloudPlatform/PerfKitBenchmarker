#!/usr/bin/env python3
"""Prerequisite Setup for GKE Agentic Benchmarking.

Creates infrastructure that PKB's native container_cluster provisioner
cannot manage: VPC, Subnet, Cloud Router, NAT, Firewall Rules, Artifact
Registry, Cloud Build SA, IAM bindings, and container image builds.

This script is run ONCE before PKB provisioning. PKB then references the
pre-existing VPC/subnet via --gce_network_name and --gce_subnet_name flags.

Usage:
  # Full setup (including image builds):
  python -m perfkitbenchmarker.linux_benchmarks.gke_prerequisite_setup \
      --project_id=my-project \
      --region=us-central1 --zone=us-central1-a \
      --machine_type=c4-standard-8

  # Setup without image builds:
  python -m perfkitbenchmarker.linux_benchmarks.gke_prerequisite_setup \
      --project_id=my-project \
      --region=us-central1 --zone=us-central1-a \
      --skip_image_build

  # Teardown:
  python -m perfkitbenchmarker.linux_benchmarks.gke_prerequisite_setup \
      --project_id=my-project \
      --region=us-central1 --zone=us-central1-a \
      --teardown

  # Teardown (keep images):
  python -m perfkitbenchmarker.linux_benchmarks.gke_prerequisite_setup \
      --project_id=my-project \
      --region=us-central1 --zone=us-central1-a \
      --teardown --keep_images
"""

import argparse
import logging
import os
import subprocess
import sys
import time

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _run(cmd, check=True, timeout=300, capture=False):
    """Run a shell command, logging it first."""
    cmd_str = " ".join(cmd) if isinstance(cmd, list) else cmd
    logging.info("CMD: %s", cmd_str)
    result = subprocess.run(
        cmd if isinstance(cmd, list) else cmd.split(),
        capture_output=capture,
        text=True,
        timeout=timeout,
    )
    if check and result.returncode != 0:
        stderr = result.stderr if capture else ""
        logging.error("Command failed (rc=%d): %s", result.returncode, stderr)
        raise RuntimeError(f"Command failed: {cmd_str}")
    return result


def _exists(cmd):
    """Return True if a gcloud describe/get command succeeds."""
    result = subprocess.run(
        cmd if isinstance(cmd, list) else cmd.split(),
        capture_output=True,
        text=True,
        timeout=60,
    )
    return result.returncode == 0


def _derive_config(args):
    """Derive configuration values from arguments."""
    user_prefix = os.environ.get("USER", "pkb").split(".")[0]
    machine_family = args.machine_type.split("-")[0]

    # Disk type
    disk_type = "pd-balanced" if machine_family == "c3" else "hyperdisk-balanced"

    # Architecture
    target_arch = "arm64" if machine_family == "c4a" else "amd64"

    # Cluster suffix
    if "metal" in args.machine_type:
        cluster_suffix = "c3metal"
    else:
        cluster_suffix = machine_family

    # Master CIDR (unique per cluster suffix)
    master_cidrs = {
        "c4": "172.16.0.0/28",
        "c4d": "172.16.0.16/28",
        "c4a": "172.16.0.32/28",
        "c3metal": "172.16.0.48/28",
    }
    master_cidr = master_cidrs.get(cluster_suffix, "172.16.0.64/28")

    return {
        "user_prefix": user_prefix,
        "machine_family": machine_family,
        "disk_type": disk_type,
        "target_arch": target_arch,
        "cluster_suffix": cluster_suffix,
        "master_cidr": master_cidr,
        "vpc_name": f"{user_prefix}-agentic-vpc",
        "subnet_name": f"{user_prefix}-agentic-subnet",
        "subnet_cidr": args.subnet_cidr,
        "router_name": f"{user_prefix}-agentic-nat-router",
        "nat_name": f"{user_prefix}-agentic-nat-config",
        "adk_repo_name": "adk-repo",
        "sandbox_repo_name": "agent-sandbox",
        "cloud_build_sa": "adk-cloud-build-sa",
        "cloud_build_sa_email": f"adk-cloud-build-sa@{args.project_id}.iam.gserviceaccount.com",
        "adk_image": f"{args.region}-docker.pkg.dev/{args.project_id}/adk-repo/adk-agent:{target_arch}",
        "chromium_image": f"{args.region}-docker.pkg.dev/{args.project_id}/agent-sandbox/chrome-sandbox:{target_arch}",
        "router_image": f"{args.region}-docker.pkg.dev/{args.project_id}/agent-sandbox/sandbox-router:{target_arch}",
    }


# ---------------------------------------------------------------------------
# Setup Steps
# ---------------------------------------------------------------------------


def enable_apis(args):
    """Enable required GCP APIs."""
    logging.info("=== Enabling GCP APIs ===")
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
    _run([
        "gcloud", "services", "enable", *apis,
        f"--project={args.project_id}",
    ])
    logging.info("APIs enabled.")


def create_vpc(args, config):
    """Create custom VPC."""
    logging.info("=== Creating VPC ===")
    if _exists([
        "gcloud", "compute", "networks", "describe", config["vpc_name"],
        f"--project={args.project_id}",
    ]):
        logging.info("VPC %s already exists.", config["vpc_name"])
        return

    _run([
        "gcloud", "compute", "networks", "create", config["vpc_name"],
        "--subnet-mode=custom",
        f"--project={args.project_id}",
    ])
    logging.info("VPC %s created.", config["vpc_name"])


def create_subnet(args, config):
    """Create subnet in the VPC."""
    logging.info("=== Creating Subnet ===")
    if _exists([
        "gcloud", "compute", "networks", "subnets", "describe",
        config["subnet_name"],
        f"--region={args.region}",
        f"--project={args.project_id}",
    ]):
        logging.info("Subnet %s already exists.", config["subnet_name"])
        return

    _run([
        "gcloud", "compute", "networks", "subnets", "create",
        config["subnet_name"],
        f"--network={config['vpc_name']}",
        f"--region={args.region}",
        f"--range={config['subnet_cidr']}",
        f"--project={args.project_id}",
    ])
    logging.info("Subnet %s created.", config["subnet_name"])


def create_firewall_rules(args, config):
    """Create firewall rules."""
    logging.info("=== Creating Firewall Rules ===")

    rules = [
        {
            "name": f"{config['vpc_name']}-allow-iap-ssh",
            "rules": "tcp:22",
            "source_ranges": "35.235.240.0/20",
            "priority": "1000",
        },
        {
            "name": f"{config['vpc_name']}-allow-internal",
            "rules": "tcp,udp,icmp",
            "source_ranges": config["subnet_cidr"],
            "priority": "1000",
        },
    ]

    for rule in rules:
        if _exists([
            "gcloud", "compute", "firewall-rules", "describe", rule["name"],
            f"--project={args.project_id}",
        ]):
            logging.info("Firewall rule %s already exists.", rule["name"])
            continue

        _run([
            "gcloud", "compute", "firewall-rules", "create", rule["name"],
            f"--network={config['vpc_name']}",
            "--direction=INGRESS",
            "--action=ALLOW",
            f"--rules={rule['rules']}",
            f"--source-ranges={rule['source_ranges']}",
            f"--priority={rule['priority']}",
            f"--project={args.project_id}",
        ])
        logging.info("Firewall rule %s created.", rule["name"])


def create_router_and_nat(args, config):
    """Create Cloud Router and NAT for private node internet access."""
    logging.info("=== Creating Cloud Router + NAT ===")

    # Router
    if not _exists([
        "gcloud", "compute", "routers", "describe", config["router_name"],
        f"--region={args.region}",
        f"--project={args.project_id}",
    ]):
        _run([
            "gcloud", "compute", "routers", "create", config["router_name"],
            f"--network={config['vpc_name']}",
            f"--region={args.region}",
            f"--project={args.project_id}",
        ])
        logging.info("Router %s created.", config["router_name"])
    else:
        logging.info("Router %s already exists.", config["router_name"])

    # NAT
    if not _exists([
        "gcloud", "compute", "routers", "nats", "describe", config["nat_name"],
        f"--router={config['router_name']}",
        f"--region={args.region}",
        f"--project={args.project_id}",
    ]):
        _run([
            "gcloud", "compute", "routers", "nats", "create", config["nat_name"],
            f"--router={config['router_name']}",
            f"--region={args.region}",
            "--nat-all-subnet-ip-ranges",
            "--auto-allocate-nat-external-ips",
            f"--project={args.project_id}",
        ])
        logging.info("NAT %s created.", config["nat_name"])
    else:
        logging.info("NAT %s already exists.", config["nat_name"])


def create_artifact_registry(args, config):
    """Create Artifact Registry repositories."""
    logging.info("=== Creating Artifact Registry Repos ===")

    for repo in [config["adk_repo_name"], config["sandbox_repo_name"]]:
        result = subprocess.run(
            [
                "gcloud", "artifacts", "repositories", "describe", repo,
                f"--location={args.region}",
                f"--project={args.project_id}",
            ],
            capture_output=True, text=True, timeout=30,
        )
        if result.returncode == 0:
            logging.info("AR repo %s already exists.", repo)
            continue

        _run([
            "gcloud", "artifacts", "repositories", "create", repo,
            "--repository-format=docker",
            f"--location={args.region}",
            f"--project={args.project_id}",
        ])
        logging.info("AR repo %s created.", repo)


def create_cloud_build_sa(args, config):
    """Create Cloud Build service account and bind IAM roles."""
    logging.info("=== Creating Cloud Build SA ===")

    sa_email = config["cloud_build_sa_email"]

    # Create SA
    if not _exists([
        "gcloud", "iam", "service-accounts", "describe", sa_email,
        f"--project={args.project_id}",
    ]):
        _run([
            "gcloud", "iam", "service-accounts", "create",
            config["cloud_build_sa"],
            f"--display-name={config['cloud_build_sa']}",
            f"--project={args.project_id}",
        ])
        logging.info("SA %s created. Waiting for propagation...", sa_email)
        time.sleep(10)
    else:
        logging.info("SA %s already exists.", sa_email)

    # Bind roles
    roles = [
        "roles/logging.logWriter",
        "roles/storage.objectViewer",
        "roles/artifactregistry.writer",
        "roles/serviceusage.serviceUsageConsumer",
    ]
    for role in roles:
        _run([
            "gcloud", "projects", "add-iam-policy-binding", args.project_id,
            f"--member=serviceAccount:{sa_email}",
            f"--role={role}",
            "--condition=None", "--quiet",
        ], check=False)

    logging.info("Cloud Build SA roles bound.")


def build_images(args, config):
    """Build and push container images via Cloud Build.

    Delegates to gke_image_build_utils.build_images_with_config()
    to avoid duplicating Cloud Build logic.
    """
    if args.skip_image_build:
        logging.info("=== Skipping Image Builds (--skip_image_build) ===")
        return

    logging.info("=== Building Container Images ===")

    # Import the shared image build module (same package)
    from perfkitbenchmarker.linux_benchmarks.kubernetes.agentic import gke_image_build_utils

    gke_image_build_utils.build_images_with_config(
        project=args.project_id,
        region=args.region,
        machine_type=args.machine_type,
        cloud_build_sa=config["cloud_build_sa_email"],
    )

    logging.info("=== Image builds complete ===")


# ---------------------------------------------------------------------------
# Teardown Steps
# ---------------------------------------------------------------------------


def teardown(args, config):
    """Tear down all prerequisite resources."""
    logging.info("=== Prerequisite Teardown ===")

    # AR repos
    if not args.keep_images:
        logging.info("Deleting Artifact Registry repos...")
        for repo in [config["adk_repo_name"], config["sandbox_repo_name"]]:
            _run([
                "gcloud", "artifacts", "repositories", "delete", repo,
                f"--location={args.region}",
                f"--project={args.project_id}", "--quiet",
            ], check=False)
    else:
        logging.info("Keeping AR repos (--keep_images).")

    # Cloud Build SA
    logging.info("Deleting Cloud Build SA...")
    sa_email = config["cloud_build_sa_email"]
    roles = [
        "roles/logging.logWriter",
        "roles/storage.objectViewer",
        "roles/artifactregistry.writer",
        "roles/serviceusage.serviceUsageConsumer",
    ]
    for role in roles:
        _run([
            "gcloud", "projects", "remove-iam-policy-binding", args.project_id,
            f"--member=serviceAccount:{sa_email}",
            f"--role={role}", "--quiet",
        ], check=False)
    _run([
        "gcloud", "iam", "service-accounts", "delete", sa_email,
        f"--project={args.project_id}", "--quiet",
    ], check=False)

    # NAT + Router
    logging.info("Deleting NAT + Router...")
    _run([
        "gcloud", "compute", "routers", "nats", "delete", config["nat_name"],
        f"--router={config['router_name']}",
        f"--region={args.region}",
        f"--project={args.project_id}", "--quiet",
    ], check=False)
    _run([
        "gcloud", "compute", "routers", "delete", config["router_name"],
        f"--region={args.region}",
        f"--project={args.project_id}", "--quiet",
    ], check=False)

    # Firewall rules
    logging.info("Deleting firewall rules...")
    for suffix in ["allow-iap-ssh", "allow-internal"]:
        _run([
            "gcloud", "compute", "firewall-rules", "delete",
            f"{config['vpc_name']}-{suffix}",
            f"--project={args.project_id}", "--quiet",
        ], check=False)

    # Subnet + VPC
    logging.info("Deleting subnet + VPC...")
    _run([
        "gcloud", "compute", "networks", "subnets", "delete",
        config["subnet_name"],
        f"--region={args.region}",
        f"--project={args.project_id}", "--quiet",
    ], check=False)
    _run([
        "gcloud", "compute", "networks", "delete", config["vpc_name"],
        f"--project={args.project_id}", "--quiet",
    ], check=False)

    logging.info("=== Prerequisite Teardown Complete ===")


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def parse_args():
    p = argparse.ArgumentParser(
        description="Prerequisite Setup for GKE Agentic Benchmarking",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    p.add_argument("--project_id", required=True, help="GCP project ID")
    p.add_argument("--region", default="us-central1", help="GCP region (default: us-central1)")
    p.add_argument("--zone", default="us-central1-a", help="GCP zone (default: us-central1-a)")
    p.add_argument("--machine_type", default="c4-standard-8",
                   help="Machine type for sandbox nodes (default: c4-standard-8)")
    p.add_argument("--subnet_cidr", default="10.134.20.0/24",
                   help="Subnet CIDR range (default: 10.134.20.0/24)")
    p.add_argument("--skip_image_build", action="store_true", default=False,
                   help="Skip container image builds")
    p.add_argument("--teardown", action="store_true", default=False,
                   help="Tear down prerequisite resources instead of creating them")
    p.add_argument("--keep_images", action="store_true", default=False,
                   help="Keep AR repos during teardown")
    return p.parse_args()


def main():
    args = parse_args()
    config = _derive_config(args)

    print(f"\n{'='*60}")
    print(f"Project:      {args.project_id}")
    print(f"Region:       {args.region}")
    print(f"Zone:         {args.zone}")
    print(f"Machine Type: {args.machine_type}")
    print(f"VPC:          {config['vpc_name']}")
    print(f"Subnet:       {config['subnet_name']} ({config['subnet_cidr']})")
    print(f"Mode:         {'TEARDOWN' if args.teardown else 'SETUP'}")
    print(f"{'='*60}\n")

    if args.teardown:
        teardown(args, config)
    else:
        enable_apis(args)
        create_vpc(args, config)
        create_subnet(args, config)
        create_firewall_rules(args, config)
        create_router_and_nat(args, config)
        create_artifact_registry(args, config)
        create_cloud_build_sa(args, config)
        build_images(args, config)

        print(f"\n{'='*60}")
        print("Prerequisite setup complete!")
        print(f"{'='*60}")
        print(f"\nPKB flags to reference this infrastructure:")
        print(f"  --gce_network_name={config['vpc_name']}")
        print(f"\nNext: Run PKB with container_cluster provisioning:")
        print(f"  python pkb.py --benchmarks=gke_python_density \\")
        print(f"      --gce_network_name={config['vpc_name']} \\")
        print(f"      --zone={args.zone} \\")
        print(f"      --gke_use_beta=true \\")
        print(f"      --gke_additional_flags=\"--enable-pod-snapshots,...,--subnetwork={config['subnet_name']}\"")


if __name__ == "__main__":
    main()
