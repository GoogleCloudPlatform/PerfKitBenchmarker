"""Shared Provision/Teardown utilities for GKE Agent Sandbox benchmarks.

Provides the full GKE infrastructure lifecycle (create and destroy) used
by all seven UC benchmark scripts.  Each benchmark's Provision() and
Teardown() functions delegate to the public functions in this module.

Infrastructure created (in order):
  1. VPC + Subnet
  2. Firewall rules (IAP SSH, internal, laptop IP)
  3. Cloud Router + NAT
  4. GKE Cluster (DPv2, Workload Identity, optional Pod Snapshots)
  5. Fleet registration / credential retrieval
  6. gVisor sandbox node pool
  7. Artifact Registry repositories
  8. Cloud Build service account + IAM bindings
  9. Container images (optional, gated by --gke_skip_image_build)

Teardown respects two flags:
  --gke_teardown_keep_images: skip AR repo deletion
  --gke_teardown_keep_infra:  only delete K8s workloads, keep cluster/network
"""

import logging
import subprocess
import time

from absl import flags

FLAGS = flags.FLAGS

# Image build utilities (Phase 3)
# Imported after FLAGS to avoid circular dependency
# The actual import is deferred to Provision() to allow flag registration order

# ---------------------------------------------------------------------------
# Provision/Teardown flags
# ---------------------------------------------------------------------------

flags.DEFINE_string(
    "gke_project_id",
    "",
    "GCP project ID for the benchmark cluster. Required for Provision/Teardown.",
)

flags.DEFINE_string(
    "gke_region",
    "us-central1",
    "GCP region for networking and Artifact Registry.",
)

flags.DEFINE_string(
    "gke_zone",
    "us-central1-a",
    "GCP zone for the GKE cluster and node pools.",
)

flags.DEFINE_string(
    "gke_sandbox_machine_type",
    "c4-standard-8",
    "Machine type for the gVisor sandbox node pool.",
)

flags.DEFINE_string(
    "gke_cluster_suffix",
    "",
    "Cluster name suffix. If empty, derived from machine family (e.g. 'c4').",
)

flags.DEFINE_string(
    "gke_gke_version",
    "1.35.3-gke.1389000",
    "GKE cluster version.",
)

flags.DEFINE_bool(
    "gke_use_connect_gateway",
    True,
    "Use Connect Gateway for kubectl access instead of direct public endpoint.",
)

flags.DEFINE_bool(
    "gke_enable_pod_snapshots",
    True,
    "Enable GKE Pod Snapshots (Preview feature, uses gcloud beta).",
)

flags.DEFINE_bool(
    "gke_skip_image_build",
    True,
    "Skip container image builds during Provision. Set to False on first run.",
)

flags.DEFINE_integer(
    "gke_sandbox_node_count",
    1,
    "Number of nodes in the gVisor sandbox node pool.",
)

flags.DEFINE_integer(
    "gke_sandbox_disk_size",
    100,
    "Disk size in GB for sandbox node pool nodes.",
)

flags.DEFINE_integer(
    "gke_sandbox_max_pods_per_node",
    250,
    "Max pods per node on the sandbox node pool.",
)

flags.DEFINE_string(
    "gke_subnet_cidr",
    "10.134.20.0/24",
    "CIDR range for the benchmark subnet.",
)

flags.DEFINE_bool(
    "gke_teardown_keep_images",
    False,
    "If True, skip Artifact Registry repo deletion during Teardown.",
)

flags.DEFINE_bool(
    "gke_teardown_keep_infra",
    False,
    "If True, only delete K8s workloads during Teardown (keep cluster/network).",
)


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


def _run(cmd, timeout=300, check=True):
    """Run a shell command and return CompletedProcess.

    Args:
        cmd: List of command arguments.
        timeout: Max seconds to wait.
        check: If True, raise on non-zero exit.

    Returns:
        subprocess.CompletedProcess
    """
    logging.info("CMD: %s", " ".join(cmd))
    proc = subprocess.run(
        cmd, capture_output=True, text=True, timeout=timeout,
    )
    if proc.returncode != 0:
        logging.warning("CMD stderr: %s", proc.stderr[-500:] if proc.stderr else "")
        if check:
            raise RuntimeError(
                f"Command failed (rc={proc.returncode}): {' '.join(cmd[:6])}\n"
                f"{proc.stderr[-300:]}"
            )
    return proc


def _run_quiet(cmd, timeout=300):
    """Run a command, suppress errors (idempotent checks)."""
    return _run(cmd, timeout=timeout, check=False)


def _resource_exists(cmd):
    """Return True if a gcloud describe/get command succeeds."""
    proc = _run_quiet(cmd)
    return proc.returncode == 0


def _derive_config():
    """Derive computed configuration values from flags.

    Returns:
        dict with all computed names and settings.
    """
    project = FLAGS.gke_project_id
    if not project:
        raise RuntimeError("--gke_project_id is required for Provision/Teardown.")

    region = FLAGS.gke_region
    zone = FLAGS.gke_zone
    machine_type = FLAGS.gke_sandbox_machine_type

    # Derive machine family (e.g. "c4" from "c4-standard-8")
    machine_family = machine_type.split("-")[0]

    # Derive cluster suffix
    cluster_suffix = FLAGS.gke_cluster_suffix
    if not cluster_suffix:
        if machine_family == "c3" and "metal" in machine_type:
            cluster_suffix = "c3metal"
        else:
            cluster_suffix = machine_family

    # Derive disk type
    if machine_family == "c3":
        disk_type = "pd-balanced"
    else:
        disk_type = "hyperdisk-balanced"

    # Derive architecture
    if machine_family == "c4a":
        target_arch = "arm64"
    else:
        target_arch = "amd64"

    # Derive master CIDR
    master_cidr_map = {
        "c4": "172.16.0.0/28",
        "c4d": "172.16.0.16/28",
        "c4a": "172.16.0.32/28",
        "c3metal": "172.16.0.48/28",
    }
    master_cidr = master_cidr_map.get(cluster_suffix, "172.16.0.64/28")

    # Use a prefix derived from project for naming
    name_prefix = "pkb"

    cluster_name = f"{name_prefix}-agentic-{cluster_suffix}"
    vpc_name = f"{name_prefix}-agentic-vpc"
    subnet_name = f"{name_prefix}-agentic-subnet"
    router_name = f"{name_prefix}-agentic-nat-router"
    nat_name = f"{name_prefix}-agentic-nat-config"
    sandbox_pool_name = "agentic-sandbox-pool"
    adk_repo_name = "adk-repo"
    sandbox_repo_name = "agent-sandbox"
    cloud_build_sa = "adk-cloud-build-sa"
    cloud_build_sa_email = f"{cloud_build_sa}@{project}.iam.gserviceaccount.com"
    namespace = FLAGS.gke_namespace

    return {
        "project": project,
        "region": region,
        "zone": zone,
        "machine_type": machine_type,
        "machine_family": machine_family,
        "cluster_suffix": cluster_suffix,
        "disk_type": disk_type,
        "target_arch": target_arch,
        "master_cidr": master_cidr,
        "cluster_name": cluster_name,
        "vpc_name": vpc_name,
        "subnet_name": subnet_name,
        "subnet_cidr": FLAGS.gke_subnet_cidr,
        "router_name": router_name,
        "nat_name": nat_name,
        "sandbox_pool_name": sandbox_pool_name,
        "adk_repo_name": adk_repo_name,
        "sandbox_repo_name": sandbox_repo_name,
        "cloud_build_sa": cloud_build_sa,
        "cloud_build_sa_email": cloud_build_sa_email,
        "namespace": namespace,
        "gke_version": FLAGS.gke_gke_version,
        "sandbox_node_count": FLAGS.gke_sandbox_node_count,
        "sandbox_disk_size": FLAGS.gke_sandbox_disk_size,
        "sandbox_max_pods": FLAGS.gke_sandbox_max_pods_per_node,
        "use_connect_gateway": FLAGS.gke_use_connect_gateway,
        "enable_pod_snapshots": FLAGS.gke_enable_pod_snapshots,
        "sandbox_version": FLAGS.gke_sandbox_version,
    }


# ---------------------------------------------------------------------------
# Provision steps
# ---------------------------------------------------------------------------


def _enable_apis(cfg):
    """Enable required GCP services."""
    logging.info("Enabling required GCP APIs...")
    apis = [
        "iap.googleapis.com",
        "container.googleapis.com",
        "artifactregistry.googleapis.com",
        "cloudbuild.googleapis.com",
        "aiplatform.googleapis.com",
        "storage.googleapis.com",
        "iam.googleapis.com",
        "connectgateway.googleapis.com",
        "gkehub.googleapis.com",
        "gkeconnect.googleapis.com",
    ]
    _run(["gcloud", "services", "enable"] + apis + [f"--project={cfg['project']}"],
         timeout=120)


def _create_network(cfg):
    """Create VPC, subnet, firewall rules, Cloud Router, and NAT."""
    project = cfg["project"]
    region = cfg["region"]
    vpc = cfg["vpc_name"]
    subnet = cfg["subnet_name"]
    cidr = cfg["subnet_cidr"]
    router = cfg["router_name"]
    nat = cfg["nat_name"]

    # VPC
    if not _resource_exists(["gcloud", "compute", "networks", "describe", vpc,
                             f"--project={project}"]):
        logging.info("Creating VPC %s...", vpc)
        _run(["gcloud", "compute", "networks", "create", vpc,
              "--subnet-mode=custom", f"--project={project}"])

    # Subnet
    if not _resource_exists(["gcloud", "compute", "networks", "subnets", "describe",
                             subnet, f"--region={region}", f"--project={project}"]):
        logging.info("Creating subnet %s...", subnet)
        _run(["gcloud", "compute", "networks", "subnets", "create", subnet,
              f"--network={vpc}", f"--region={region}",
              f"--range={cidr}", f"--project={project}"])

    # Firewall: IAP SSH
    fw_iap = f"{vpc}-allow-iap-ssh"
    if not _resource_exists(["gcloud", "compute", "firewall-rules", "describe",
                             fw_iap, f"--project={project}"]):
        logging.info("Creating firewall rule %s...", fw_iap)
        _run(["gcloud", "compute", "firewall-rules", "create", fw_iap,
              f"--network={vpc}", "--direction=INGRESS", "--action=ALLOW",
              "--rules=tcp:22", "--source-ranges=35.235.240.0/20",
              "--priority=1000", f"--project={project}"])

    # Firewall: internal
    fw_int = f"{vpc}-allow-internal"
    if not _resource_exists(["gcloud", "compute", "firewall-rules", "describe",
                             fw_int, f"--project={project}"]):
        logging.info("Creating firewall rule %s...", fw_int)
        _run(["gcloud", "compute", "firewall-rules", "create", fw_int,
              f"--network={vpc}", "--direction=INGRESS", "--action=ALLOW",
              "--rules=tcp,udp,icmp", f"--source-ranges={cidr}",
              "--priority=1000", f"--project={project}"])

    # Cloud Router
    if not _resource_exists(["gcloud", "compute", "routers", "describe", router,
                             f"--region={region}", f"--project={project}"]):
        logging.info("Creating Cloud Router %s...", router)
        _run(["gcloud", "compute", "routers", "create", router,
              f"--network={vpc}", f"--region={region}", f"--project={project}"])

    # Cloud NAT
    if not _resource_exists(["gcloud", "compute", "routers", "nats", "describe", nat,
                             f"--router={router}", f"--region={region}",
                             f"--project={project}"]):
        logging.info("Creating Cloud NAT %s...", nat)
        _run(["gcloud", "compute", "routers", "nats", "create", nat,
              f"--router={router}", f"--region={region}",
              "--nat-all-subnet-ip-ranges", "--auto-allocate-nat-external-ips",
              f"--project={project}"])


def _create_cluster(cfg):
    """Create the GKE cluster with DPv2 and Workload Identity."""
    project = cfg["project"]
    zone = cfg["zone"]
    cluster = cfg["cluster_name"]

    if _resource_exists(["gcloud", "container", "clusters", "describe", cluster,
                         f"--zone={zone}", f"--project={project}"]):
        logging.info("GKE cluster %s already exists.", cluster)
        return

    logging.info("Creating GKE cluster %s...", cluster)

    if cfg["enable_pod_snapshots"]:
        snapshot_flag = ["--enable-pod-snapshots"]
        logging.info("Pod Snapshots ENABLED (using gcloud beta).")
        cmd = ["gcloud", "beta", "container", "clusters", "create", cluster]
    else:
        snapshot_flag = []
        cmd = ["gcloud", "container", "clusters", "create", cluster]

    cmd += [
        f"--zone={zone}",
        f"--network={cfg['vpc_name']}",
        f"--subnetwork={cfg['subnet_name']}",
        "--enable-private-nodes",
        "--enable-ip-alias",
        f"--master-ipv4-cidr={cfg['master_cidr']}",
        f"--cluster-version={cfg['gke_version']}",
        "--no-enable-shielded-nodes",
        "--num-nodes=1",
        f"--machine-type={cfg['machine_type']}",
        f"--disk-type={cfg['disk_type']}",
        "--disk-size=50",
        "--enable-dataplane-v2",
        f"--workload-pool={project}.svc.id.goog",
        "--release-channel=None",
        f"--project={project}",
    ] + snapshot_flag

    _run(cmd, timeout=600)
    logging.info("GKE cluster %s created.", cluster)


def _get_credentials(cfg):
    """Register to fleet and get kubectl credentials."""
    project = cfg["project"]
    zone = cfg["zone"]
    cluster = cfg["cluster_name"]

    if cfg["use_connect_gateway"]:
        # Register to fleet
        if not _resource_exists(["gcloud", "container", "fleet", "memberships",
                                 "describe", cluster, f"--project={project}"]):
            logging.info("Registering cluster %s to fleet...", cluster)
            _run(["gcloud", "container", "fleet", "memberships", "register", cluster,
                  f"--gke-cluster={zone}/{cluster}",
                  "--enable-workload-identity",
                  f"--project={project}"], timeout=120)

        logging.info("Getting credentials via Connect Gateway...")
        _run(["gcloud", "container", "fleet", "memberships", "get-credentials",
              cluster, f"--project={project}"], timeout=60)
    else:
        logging.info("Getting credentials (direct endpoint)...")
        _run(["gcloud", "container", "clusters", "get-credentials", cluster,
              f"--zone={zone}", f"--project={project}"], timeout=60)


def _create_sandbox_node_pool(cfg):
    """Create the gVisor-enabled sandbox node pool."""
    project = cfg["project"]
    zone = cfg["zone"]
    cluster = cfg["cluster_name"]
    pool_name = cfg["sandbox_pool_name"]

    if _resource_exists(["gcloud", "container", "node-pools", "describe", pool_name,
                         f"--cluster={cluster}", f"--zone={zone}",
                         f"--project={project}"]):
        logging.info("Sandbox node pool %s already exists.", pool_name)
        return

    logging.info("Creating sandbox node pool %s with gVisor...", pool_name)
    cmd = [
        "gcloud", "container", "node-pools", "create", pool_name,
        f"--cluster={cluster}",
        f"--zone={zone}",
        f"--project={project}",
        f"--machine-type={cfg['machine_type']}",
        f"--num-nodes={cfg['sandbox_node_count']}",
        f"--disk-type={cfg['disk_type']}",
        f"--disk-size={cfg['sandbox_disk_size']}",
        f"--max-pods-per-node={cfg['sandbox_max_pods']}",
        "--node-labels=dedicated=agentic-sandbox",
        "--node-taints=dedicated=agentic-sandbox:NoSchedule",
        "--workload-metadata=GKE_METADATA",
        "--sandbox", "type=gvisor",
    ]
    _run(cmd, timeout=600)
    logging.info("Sandbox node pool %s created.", pool_name)


def _create_artifact_registry(cfg):
    """Create Artifact Registry repositories."""
    project = cfg["project"]
    region = cfg["region"]

    for repo_name in (cfg["adk_repo_name"], cfg["sandbox_repo_name"]):
        logging.info("Ensuring AR repo %s exists...", repo_name)
        _run_quiet([
            "gcloud", "artifacts", "repositories", "create", repo_name,
            "--repository-format=docker",
            f"--location={region}",
            f"--project={project}",
        ])


def _create_cloud_build_sa(cfg):
    """Create Cloud Build service account and bind IAM roles."""
    project = cfg["project"]
    sa_email = cfg["cloud_build_sa_email"]
    sa_name = cfg["cloud_build_sa"]

    # Create SA if not exists
    if not _resource_exists(["gcloud", "iam", "service-accounts", "describe",
                             sa_email, f"--project={project}"]):
        logging.info("Creating Cloud Build SA %s...", sa_email)
        _run(["gcloud", "iam", "service-accounts", "create", sa_name,
              f"--display-name={sa_name}", f"--project={project}"])
        # Wait for propagation
        time.sleep(10)

    roles = [
        "roles/logging.logWriter",
        "roles/storage.objectViewer",
        "roles/artifactregistry.writer",
        "roles/serviceusage.serviceUsageConsumer",
    ]
    for role in roles:
        _run_quiet([
            "gcloud", "projects", "add-iam-policy-binding", project,
            f"--member=serviceAccount:{sa_email}",
            f"--role={role}",
            "--condition=None", "--quiet",
        ])
    logging.info("Cloud Build SA ready.")


# ---------------------------------------------------------------------------
# Teardown steps
# ---------------------------------------------------------------------------


def _teardown_workloads(cfg):
    """Delete K8s workloads, CRDs, and namespace."""
    ns = cfg["namespace"]
    version = cfg["sandbox_version"]

    logging.info("Deleting namespace %s...", ns)
    _run_quiet(["kubectl", "delete", "namespace", ns,
                "--ignore-not-found=true", "--timeout=120s"])

    logging.info("Removing Agent Sandbox CRDs...")
    _run_quiet(["kubectl", "delete", "-f",
                f"https://github.com/kubernetes-sigs/agent-sandbox/releases/download/{version}/extensions.yaml",
                "--ignore-not-found=true"])
    _run_quiet(["kubectl", "delete", "-f",
                f"https://github.com/kubernetes-sigs/agent-sandbox/releases/download/{version}/manifest.yaml",
                "--ignore-not-found=true"])

    logging.info("Removing cluster-scoped RBAC...")
    _run_quiet(["kubectl", "delete", "clusterrolebinding",
                "adk-agent-sandbox-binding", "--ignore-not-found=true"])
    _run_quiet(["kubectl", "delete", "clusterrole",
                "adk-agent-sandbox-role", "--ignore-not-found=true"])


def _teardown_images(cfg):
    """Delete Artifact Registry repositories."""
    project = cfg["project"]
    region = cfg["region"]

    for repo_name in (cfg["adk_repo_name"], cfg["sandbox_repo_name"]):
        logging.info("Deleting AR repo %s...", repo_name)
        _run_quiet(["gcloud", "artifacts", "repositories", "delete", repo_name,
                    f"--location={region}", f"--project={project}", "--quiet"])


def _teardown_cloud_build_sa(cfg):
    """Delete Cloud Build service account and IAM bindings."""
    project = cfg["project"]
    sa_email = cfg["cloud_build_sa_email"]

    roles = [
        "roles/logging.logWriter",
        "roles/storage.objectViewer",
        "roles/artifactregistry.writer",
        "roles/serviceusage.serviceUsageConsumer",
    ]
    for role in roles:
        _run_quiet([
            "gcloud", "projects", "remove-iam-policy-binding", project,
            f"--member=serviceAccount:{sa_email}",
            f"--role={role}", "--quiet",
        ])

    _run_quiet(["gcloud", "iam", "service-accounts", "delete", sa_email,
                f"--project={project}", "--quiet"])
    logging.info("Cloud Build SA deleted.")


def _teardown_cluster(cfg):
    """Delete GKE node pools and cluster."""
    project = cfg["project"]
    zone = cfg["zone"]
    cluster = cfg["cluster_name"]
    pool_name = cfg["sandbox_pool_name"]

    logging.info("Deleting sandbox node pool %s...", pool_name)
    _run_quiet(["gcloud", "container", "node-pools", "delete", pool_name,
                f"--cluster={cluster}", f"--zone={zone}",
                f"--project={project}", "--quiet"])

    logging.info("Deleting GKE cluster %s...", cluster)
    _run_quiet(["gcloud", "container", "clusters", "delete", cluster,
                f"--zone={zone}", f"--project={project}", "--quiet"])


def _teardown_network(cfg):
    """Delete network resources in reverse dependency order."""
    project = cfg["project"]
    region = cfg["region"]
    vpc = cfg["vpc_name"]
    router = cfg["router_name"]
    nat = cfg["nat_name"]
    subnet = cfg["subnet_name"]

    logging.info("Deleting Cloud NAT and Router...")
    _run_quiet(["gcloud", "compute", "routers", "nats", "delete", nat,
                f"--router={router}", f"--region={region}",
                f"--project={project}", "--quiet"])
    _run_quiet(["gcloud", "compute", "routers", "delete", router,
                f"--region={region}", f"--project={project}", "--quiet"])

    logging.info("Deleting firewall rules...")
    for suffix in ("allow-iap-ssh", "allow-internal"):
        _run_quiet(["gcloud", "compute", "firewall-rules", "delete",
                    f"{vpc}-{suffix}", f"--project={project}", "--quiet"])

    logging.info("Deleting subnet and VPC...")
    _run_quiet(["gcloud", "compute", "networks", "subnets", "delete", subnet,
                f"--region={region}", f"--project={project}", "--quiet"])
    _run_quiet(["gcloud", "compute", "networks", "delete", vpc,
                f"--project={project}", "--quiet"])


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


flags.DEFINE_enum(
    "gke_provision_mode",
    "custom",
    ["custom", "native"],
    "Provisioning mode: 'custom' uses direct gcloud calls (Phase 1 logic), "
    "'native' uses PKB's container_cluster with prerequisite_setup.py.",
)

def Provision():
    """Provision GKE infrastructure.

    Mode is controlled by --gke_provision_mode:
      - custom: Direct gcloud calls (full control, no PKB cluster management)
      - native: PKB manages cluster via container_cluster spec.
                Requires prerequisite_setup.py to have been run first.
    """
    mode = FLAGS.gke_provision_mode
    if mode == "native":
        logging.info(
            "Provision mode=native: PKB manages cluster via container_cluster. "
            "Ensure prerequisite_setup.py was run first (VPC, NAT, AR, images)."
        )
        return  # PKB handles cluster creation via container_cluster spec

    logging.info("Provision mode=custom: using direct gcloud calls.")
    cfg = _derive_config()

    logging.info("=== Provision: project=%s cluster=%s machine=%s ===",
                 cfg["project"], cfg["cluster_name"], cfg["machine_type"])

    _enable_apis(cfg)
    _create_network(cfg)
    _create_cluster(cfg)
    _get_credentials(cfg)
    _create_sandbox_node_pool(cfg)
    _create_artifact_registry(cfg)
    _create_cloud_build_sa(cfg)

    # --- Phase 3: Build container images ---
    if not FLAGS.gke_skip_image_build:
        from perfkitbenchmarker.linux_benchmarks.kubernetes.agentic import gke_image_build_utils
        gke_image_build_utils.BuildImages()
    else:
        logging.info("Skipping image builds (--gke_skip_image_build=true)")

    logging.info("=== Provision complete: %s ===", cfg["cluster_name"])


def Teardown():
    """Teardown GKE infrastructure.

    Mode is controlled by --gke_provision_mode:
      - custom: Direct gcloud calls to delete all resources.
      - native: PKB manages cluster deletion. Run prerequisite_setup.py --teardown
                separately to clean up VPC/NAT/AR.
    """
    mode = FLAGS.gke_provision_mode
    if mode == "native":
        logging.info(
            "Teardown mode=native: PKB manages cluster deletion. "
            "Run prerequisite_setup.py --teardown to clean up VPC/NAT/AR."
        )
        return  # PKB handles cluster deletion

    logging.info("Teardown mode=custom: using direct gcloud calls.")
    cfg = _derive_config()

    logging.info("=== Teardown: project=%s cluster=%s ===",
                 cfg["project"], cfg["cluster_name"])
    logging.info("  keep_images=%s  keep_infra=%s",
                 FLAGS.gke_teardown_keep_images,
                 FLAGS.gke_teardown_keep_infra)

    # Always delete workloads
    _teardown_workloads(cfg)

    # Conditionally delete images
    if not FLAGS.gke_teardown_keep_images:
        _teardown_images(cfg)

    # Conditionally delete infrastructure
    if not FLAGS.gke_teardown_keep_infra:
        _teardown_cloud_build_sa(cfg)
        _teardown_cluster(cfg)
        _teardown_network(cfg)

    logging.info("=== Teardown complete ===")
