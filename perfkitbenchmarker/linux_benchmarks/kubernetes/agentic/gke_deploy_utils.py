"""Shared workload deployment utilities for GKE Agent Sandbox benchmarks.

Provides idempotent functions to deploy the Agent Sandbox ecosystem
(CRDs, templates, warm pools, router, ADK agent, PSI reader) onto a
pre-provisioned GKE cluster. Called by each benchmark's Prepare() stage.

All functions are idempotent -- safe to call repeatedly without side effects.
"""

import json
import logging
import os
import subprocess
import time

from absl import flags

FLAGS = flags.FLAGS

# ---------------------------------------------------------------------------
# Flags (registered once; shared across all benchmarks)
# ---------------------------------------------------------------------------

flags.DEFINE_string(
    "gke_sandbox_version",
    "v0.4.6",
    "Agent Sandbox controller version (GitHub release tag).",
)

flags.DEFINE_string(
    "gke_sandbox_router_image",
    "",
    "Sandbox router container image. If empty, router deployment is skipped.",
)

flags.DEFINE_string(
    "gke_adk_image",
    "",
    "ADK agent container image. If empty, agent deployment is skipped.",
)

flags.DEFINE_string(
    "gke_chromium_image",
    "",
    "Chromium sandbox container image. If empty, uses placeholder.",
)

flags.DEFINE_integer(
    "gke_warmpool_replicas",
    2,
    "Default warm pool replica count for SandboxWarmPool resources.",
)

flags.DEFINE_integer(
    "gke_chromium_replicas",
    1,
    "Default Chromium warm pool replica count.",
)

flags.DEFINE_string(
    "gke_python_image",
    "registry.k8s.io/agent-sandbox/python-runtime-sandbox:v0.1.0",
    "Python runtime sandbox container image.",
)

flags.DEFINE_integer(
    "gke_deploy_timeout",
    120,
    "Timeout in seconds for workload deployment rollout.",
)

flags.DEFINE_string(
    "gke_cluster_name",
    "",
    "GKE cluster name. Used in ADK agent env vars for Workload Identity.",
)

# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

# ---------------------------------------------------------------------------
# Image path auto-derivation and mode-aware scheduling
# (Insert this block BEFORE the "def DeployWorkloads():" function)
# ---------------------------------------------------------------------------


def _DeriveImagePaths():
    """Auto-derive container image paths from project/region/machine_type.

    When --gke_adk_image or --gke_sandbox_router_image are empty,
    derives them from --gke_project_id, --gke_region, and
    --gke_sandbox_machine_type using the same convention as
    gke_image_build_utils.py and the bash build scripts.
    """
    project = getattr(FLAGS, "gke_project_id", "") or ""
    region = getattr(FLAGS, "gke_region", "") or ""
    machine_type = getattr(FLAGS, "gke_sandbox_machine_type", "") or ""

    if not project or not region:
        logging.info("Cannot auto-derive images: project=%s region=%s", project, region)
        return

    machine_family = machine_type.split("-")[0] if machine_type else "c4"
    target_arch = "arm64" if machine_family == "c4a" else "amd64"

    if not FLAGS.gke_adk_image:
        FLAGS.gke_adk_image = "{}-docker.pkg.dev/{}/adk-repo/adk-agent:{}".format(
            region, project, target_arch
        )
        logging.info("Auto-derived gke_adk_image: %s", FLAGS.gke_adk_image)

    if not FLAGS.gke_sandbox_router_image:
        FLAGS.gke_sandbox_router_image = (
            "{}-docker.pkg.dev/{}/agent-sandbox/sandbox-router:{}".format(
                region, project, target_arch
            )
        )
        logging.info(
            "Auto-derived gke_sandbox_router_image: %s",
            FLAGS.gke_sandbox_router_image,
        )

    if not FLAGS.gke_chromium_image:
        FLAGS.gke_chromium_image = (
            "{}-docker.pkg.dev/{}/agent-sandbox/chrome-sandbox:{}".format(
                region, project, target_arch
            )
        )
        logging.info(
            "Auto-derived gke_chromium_image: %s", FLAGS.gke_chromium_image
        )

    if not FLAGS.gke_cluster_name:
        import os as _os

        user_prefix = _os.environ.get("USER", "pkb").split(".")[0]
        suffix_map = {"c3": "c3metal", "c4": "c4", "c4d": "c4d", "c4a": "c4a"}
        cluster_suffix = suffix_map.get(machine_family, machine_family)
        FLAGS.gke_cluster_name = "{}-agentic-{}".format(
            user_prefix, cluster_suffix
        )
        logging.info(
            "Auto-derived gke_cluster_name: %s", FLAGS.gke_cluster_name
        )


def _GetSandboxNodeSelector():
    """Return the correct nodeSelector dict based on provisioning mode.

    - native mode: PKB auto-labels nodes with pkb_nodepool=<pool_name>
    - custom mode: bash scripts label nodes with dedicated=agentic-sandbox
    """
    try:
        mode = FLAGS.gke_provision_mode
    except (AttributeError, KeyError):
        mode = "custom"
    if mode == "native":
        return {"pkb_nodepool": "sandbox"}
    return {"dedicated": "agentic-sandbox"}


def _GetSandboxTolerations():
    """Return tolerations list based on provisioning mode.

    Both modes need the gVisor toleration (auto-applied by GKE to sandbox pools).
    Custom mode additionally needs the dedicated=agentic-sandbox toleration
    (manually applied by setup_infrastructure_gke.sh).
    """
    try:
        mode = FLAGS.gke_provision_mode
    except (AttributeError, KeyError):
        mode = "custom"
    tolerations = [
        {
            "key": "sandbox.gke.io/runtime",
            "operator": "Equal",
            "value": "gvisor",
            "effect": "NoSchedule",
        },
    ]
    if mode != "native":
        tolerations.insert(
            0,
            {
                "key": "dedicated",
                "operator": "Equal",
                "value": "agentic-sandbox",
                "effect": "NoSchedule",
            },
        )
    return tolerations


def _NodeSelectorYaml(indent=6):
    """Generate nodeSelector YAML block for embedding in manifests."""
    selector = _GetSandboxNodeSelector()
    spaces = " " * indent
    lines = ["{}nodeSelector:".format(spaces)]
    for k, v in selector.items():
        lines.append("{}  {}: {}".format(spaces, k, v))
    return "\n".join(lines)


def _TolerationsYaml(indent=6):
    """Generate tolerations YAML block for embedding in manifests."""
    tolerations = _GetSandboxTolerations()
    spaces = " " * indent
    lines = ["{}tolerations:".format(spaces)]
    for t in tolerations:
        lines.append('{}  - key: "{}"'.format(spaces, t["key"]))
        lines.append('{}    operator: "{}"'.format(spaces, t["operator"]))
        lines.append('{}    value: "{}"'.format(spaces, t["value"]))
        lines.append('{}    effect: "{}"'.format(spaces, t["effect"]))
    return "\n".join(lines)


def DeployWorkloads():
    """Deploy the full Agent Sandbox ecosystem onto the GKE cluster.

    Idempotent: safe to call repeatedly. Sequence:
      1. Create namespace
      2. Install Agent Sandbox CRDs
      3. Deploy SandboxTemplates + WarmPools
      4. Deploy Sandbox Router
      5. Deploy ADK Agent (Deployment + Service + RBAC)
      6. Deploy PSI Reader DaemonSet
      7. Wait for ADK Agent rollout
    """
    _DeriveImagePaths()
    ns = FLAGS.gke_namespace
    logging.info("=== DeployWorkloads: namespace=%s ===", ns)

    _CreateNamespace(ns)
    _InstallCRDs()
    _DeploySandboxTemplates(ns)
    _DeploySandboxRouter(ns)
    _DeployADKAgent(ns)
    _DeployPSIReader(ns)
    _WaitForAgentReady(ns)

    logging.info("DeployWorkloads complete.")


def DeploySnapshots():
    """Deploy Pod Snapshot infrastructure (UC-A only).

    Idempotent: safe to call repeatedly. Sequence:
      1. Create GCS bucket (hierarchical namespace)
      2. Create managed folder
      3. Create KSA for snapshots
      4. Bind IAM roles
      5. Deploy PodSnapshotStorageConfig + PodSnapshotPolicy
    """
    ns = FLAGS.gke_namespace
    project = FLAGS.gke_project_id
    region = FLAGS.gke_region

    if not project:
        logging.warning("DeploySnapshots: gke_project_id not set, skipping.")
        return

    bucket_name = "agent-sandbox-snapshots-{}".format(project)
    snapshot_folder = "benchmark-snapshots"
    ksa_name = "pod-snapshot-sa"

    logging.info("=== DeploySnapshots: bucket=%s ===", bucket_name)

    # 1. Create GCS bucket
    _RunCmd(
        [
            "gcloud",
            "storage",
            "buckets",
            "create",
            "gs://{}".format(bucket_name),
            "--uniform-bucket-level-access",
            "--enable-hierarchical-namespace",
            "--soft-delete-duration=0d",
            "--location={}".format(region),
            "--project={}".format(project),
        ],
        check=False,
    )

    # 2. Create managed folder
    _RunCmd(
        [
            "gcloud",
            "storage",
            "managed-folders",
            "create",
            "gs://{}/{}/".format(bucket_name, snapshot_folder),
            "--project={}".format(project),
        ],
        check=False,
    )

    # 3. Create KSA
    _RunKubectl(
        [
            "create",
            "serviceaccount",
            ksa_name,
            "--namespace",
            ns,
        ],
        check=False,
    )

    # 4. IAM bindings
    project_number = _GetProjectNumber(project)
    if project_number:
        _BindSnapshotIAM(bucket_name, project, project_number, ns, ksa_name)

    # 5. Deploy PSSC + PSP
    _DeploySnapshotCRDs(ns, bucket_name, snapshot_folder)

    logging.info("DeploySnapshots complete.")


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


def _RunCmd(cmd, check=True, timeout=120):
    """Run a shell command and return (stdout, returncode)."""
    logging.info("CMD: %s", " ".join(cmd))
    proc = subprocess.run(
        cmd,
        capture_output=True,
        text=True,
        timeout=timeout,
    )
    if check and proc.returncode != 0:
        logging.warning(
            "Command failed (rc=%d): %s", proc.returncode, proc.stderr[:500]
        )
    return proc.stdout.strip(), proc.returncode


def _RunKubectl(args, check=True, timeout=120):
    """Run kubectl with optional kubeconfig."""
    cmd = ["kubectl"]
    if FLAGS.gke_kubeconfig:
        cmd += ["--kubeconfig", FLAGS.gke_kubeconfig]
    cmd += list(args)
    return _RunCmd(cmd, check=check, timeout=timeout)


def _KubectlApply(manifest_str):
    """Apply a YAML manifest string via kubectl stdin."""
    cmd = ["kubectl", "apply", "-f", "-"]
    if FLAGS.gke_kubeconfig:
        cmd = [
            "kubectl",
            "--kubeconfig",
            FLAGS.gke_kubeconfig,
            "apply",
            "-f",
            "-",
        ]
    proc = subprocess.run(
        cmd,
        input=manifest_str,
        capture_output=True,
        text=True,
        timeout=60,
    )
    if proc.returncode != 0:
        logging.warning("kubectl apply failed: %s", proc.stderr[:500])
    return proc.returncode == 0


def _CreateNamespace(ns):
    """Create namespace if it doesn't exist."""
    _RunKubectl(["create", "namespace", ns], check=False)


def _InstallCRDs():
    """Install Agent Sandbox CRDs from GitHub release."""
    version = FLAGS.gke_sandbox_version
    base_url = (
        "https://github.com/kubernetes-sigs/agent-sandbox"
        "/releases/download/{}".format(version)
    )
    logging.info("Installing Agent Sandbox CRDs (%s)", version)
    _RunKubectl(
        [
            "apply",
            "-f",
            "{}/manifest.yaml".format(base_url),
            "-f",
            "{}/extensions.yaml".format(base_url),
        ],
        check=False,
    )


def _DeploySandboxTemplates(ns):
    """Deploy SandboxTemplate + WarmPool for Python and Chromium."""
    python_image = FLAGS.gke_python_image
    chromium_image = FLAGS.gke_chromium_image or "chromium-placeholder:latest"
    warmpool_replicas = FLAGS.gke_warmpool_replicas
    chromium_replicas = FLAGS.gke_chromium_replicas

    manifest = """---
apiVersion: extensions.agents.x-k8s.io/v1alpha1
kind: SandboxTemplate
metadata:
  name: python-sandbox-template
  namespace: {ns}
spec:
  podTemplate:
    metadata:
      labels:
        sandbox: python-sandbox-example
    spec:
      runtimeClassName: gvisor
      containers:
      - name: python-runtime
        image: {python_image}
{node_selector_yaml}
{tolerations_yaml}
      restartPolicy: "OnFailure"
---
apiVersion: extensions.agents.x-k8s.io/v1alpha1
kind: SandboxWarmPool
metadata:
  name: python-sandbox-warmpool
  namespace: {ns}
spec:
  replicas: {warmpool_replicas}
  sandboxTemplateRef:
    name: python-sandbox-template
---
apiVersion: extensions.agents.x-k8s.io/v1alpha1
kind: SandboxTemplate
metadata:
  name: chromium-sandbox-template
  namespace: {ns}
spec:
  podTemplate:
    metadata:
      labels:
        sandbox: chromium-sandbox-example
    spec:
      runtimeClassName: gvisor
      containers:
      - name: chromium-runtime
        image: {chromium_image}
        command: ["/bin/sh", "-c"]
        args:
          - |
            socat TCP-LISTEN:9223,fork,reuseaddr TCP:127.0.0.1:9222 &
            exec chromium --headless --no-sandbox --disable-gpu --disable-dev-shm-usage --remote-debugging-port=9222 --no-first-run --disable-field-trial-config --user-data-dir=/tmp/chrome-data about:blank
        ports:
          - containerPort: 9223
{node_selector_yaml}
{tolerations_yaml}
      restartPolicy: "OnFailure"
---
apiVersion: extensions.agents.x-k8s.io/v1alpha1
kind: SandboxWarmPool
metadata:
  name: chromium-sandbox-warmpool
  namespace: {ns}
spec:
  replicas: {chromium_replicas}
  sandboxTemplateRef:
    name: chromium-sandbox-template
---
apiVersion: networking.k8s.io/v1
kind: NetworkPolicy
metadata:
  name: allow-orchestrator-to-chromium
  namespace: {ns}
spec:
  podSelector:
    matchLabels:
      sandbox: chromium-sandbox-example
  policyTypes:
  - Ingress
  ingress:
  - from:
    - podSelector:
        matchLabels:
          app: adk-agent
    ports:
    - protocol: TCP
      port: 9223
""".format(
        ns=ns,
        python_image=python_image,
        chromium_image=chromium_image,
        warmpool_replicas=warmpool_replicas,
        chromium_replicas=chromium_replicas,
        node_selector_yaml=_NodeSelectorYaml(),
        tolerations_yaml=_TolerationsYaml(),
    )
    _KubectlApply(manifest)


def _DeploySandboxRouter(ns):
    """Deploy the Sandbox Router Deployment + Service."""
    router_image = FLAGS.gke_sandbox_router_image
    if not router_image:
        logging.info("Sandbox router image not set, skipping router deployment.")
        return

    manifest = """---
apiVersion: v1
kind: Service
metadata:
  name: sandbox-router-svc
  namespace: {ns}
spec:
  type: ClusterIP
  selector:
    app: sandbox-router
  ports:
  - name: http
    protocol: TCP
    port: 8080
    targetPort: 8080
---
apiVersion: apps/v1
kind: Deployment
metadata:
  name: sandbox-router-deployment
  namespace: {ns}
spec:
  replicas: 2
  selector:
    matchLabels:
      app: sandbox-router
  template:
    metadata:
      labels:
        app: sandbox-router
    spec:
      serviceAccountName: adk-agent-sa
      topologySpreadConstraints:
        - maxSkew: 1
          topologyKey: topology.kubernetes.io/zone
          whenUnsatisfiable: ScheduleAnyway
          labelSelector:
            matchLabels:
              app: sandbox-router
      containers:
      - name: router
        image: {router_image}
        ports:
        - containerPort: 8080
        env:
        - name: ALLOW_UNAUTHENTICATED_ROUTER
          value: "true"
        readinessProbe:
          httpGet:
            path: /healthz
            port: 8080
          initialDelaySeconds: 5
          periodSeconds: 5
        livenessProbe:
          httpGet:
            path: /healthz
            port: 8080
          initialDelaySeconds: 10
          periodSeconds: 10
        resources:
          requests:
            cpu: "250m"
            memory: "512Mi"
          limits:
            cpu: "1000m"
            memory: "1Gi"
      securityContext:
        runAsUser: 1000
        runAsGroup: 1000
""".format(ns=ns, router_image=router_image)
    _KubectlApply(manifest)


def _DeployADKAgent(ns):
    """Deploy ADK Agent: SA, ClusterRole, RoleBinding, Deployment, Service."""
    adk_image = FLAGS.gke_adk_image
    if not adk_image:
        logging.info("ADK agent image not set, skipping agent deployment.")
        return

    project = FLAGS.gke_project_id or ""
    region = FLAGS.gke_region or ""
    cluster = FLAGS.gke_cluster_name or ""

    manifest = """---
apiVersion: v1
kind: ServiceAccount
metadata:
  name: adk-agent-sa
  namespace: {ns}
---
apiVersion: rbac.authorization.k8s.io/v1
kind: ClusterRole
metadata:
  name: adk-agent-sandbox-role
rules:
  - apiGroups: ["agents.x-k8s.io"]
    resources: ["sandboxes"]
    verbs: ["get", "list", "watch", "create", "update", "patch", "delete"]
  - apiGroups: ["agents.x-k8s.io"]
    resources: ["sandboxwarmpool", "sandboxwarmpools"]
    verbs: ["get", "list", "watch"]
  - apiGroups: ["extensions.agents.x-k8s.io"]
    resources: ["sandboxclaims"]
    verbs: ["get", "list", "watch", "create", "update", "patch", "delete"]
  - apiGroups: [""]
    resources: ["pods", "pods/log", "pods/exec", "services", "configmaps"]
    verbs: ["get", "list", "watch"]
  - apiGroups: [""]
    resources: ["pods/portforward"]
    verbs: ["create"]
  - apiGroups: ["metrics.k8s.io"]
    resources: ["pods"]
    verbs: ["get", "list"]
---
apiVersion: rbac.authorization.k8s.io/v1
kind: RoleBinding
metadata:
  name: adk-agent-sandbox-binding
  namespace: {ns}
subjects:
  - kind: ServiceAccount
    name: adk-agent-sa
    namespace: {ns}
roleRef:
  kind: ClusterRole
  name: adk-agent-sandbox-role
  apiGroup: rbac.authorization.k8s.io
---
apiVersion: apps/v1
kind: Deployment
metadata:
  name: adk-agent
  namespace: {ns}
spec:
  replicas: 1
  selector:
    matchLabels:
      app: adk-agent
  template:
    metadata:
      labels:
        app: adk-agent
    spec:
      serviceAccountName: adk-agent-sa
      containers:
      - name: adk-agent
        imagePullPolicy: Always
        image: {adk_image}
        resources:
          limits:
            memory: "16384Mi"
            cpu: "6000m"
          requests:
            memory: "512Mi"
            cpu: "1000m"
        ports:
        - containerPort: 8080
        livenessProbe:
          httpGet:
            path: /healthz
            port: 8080
          initialDelaySeconds: 15
          periodSeconds: 30
          timeoutSeconds: 10
          failureThreshold: 6
        readinessProbe:
          httpGet:
            path: /healthz
            port: 8080
          initialDelaySeconds: 5
          periodSeconds: 10
          timeoutSeconds: 5
          failureThreshold: 3
        env:
          - name: PORT
            value: "8080"
          - name: GOOGLE_CLOUD_PROJECT
            value: "{project}"
          - name: GOOGLE_CLOUD_LOCATION
            value: "{region}"
          - name: GOOGLE_GENAI_USE_VERTEXAI
            value: "true"
          - name: CLUSTER_NAME
            value: "{cluster}"
          - name: AGENTIC_NAMESPACE
            value: "{ns}"
          - name: SANDBOX_ROUTER_URL
            value: "http://sandbox-router-svc.{ns}.svc.cluster.local:8080"
---
apiVersion: v1
kind: Service
metadata:
  name: adk-agent
  namespace: {ns}
spec:
  type: ClusterIP
  ports:
    - port: 80
      targetPort: 8080
  selector:
    app: adk-agent
""".format(ns=ns, adk_image=adk_image, project=project, region=region, cluster=cluster)
    _KubectlApply(manifest)


def _DeployPSIReader(ns):
    """Deploy PSI Reader DaemonSet for cgroup pressure metrics."""
    manifest = """---
apiVersion: apps/v1
kind: DaemonSet
metadata:
  name: psi-reader
  namespace: {ns}
  labels:
    app: psi-reader
spec:
  selector:
    matchLabels:
      app: psi-reader
  template:
    metadata:
      labels:
        app: psi-reader
    spec:
{node_selector_yaml}
{tolerations_yaml}
      hostPID: true
      containers:
      - name: reader
        image: busybox:1.36
        command: ["sleep", "infinity"]
        securityContext:
          privileged: true
        volumeMounts:
        - name: cgroup
          mountPath: /host/sys/fs/cgroup
          readOnly: true
        - name: proc
          mountPath: /host/proc
          readOnly: true
        resources:
          requests:
            cpu: "10m"
            memory: "16Mi"
          limits:
            cpu: "50m"
            memory: "32Mi"
      volumes:
      - name: cgroup
        hostPath:
          path: /sys/fs/cgroup
      - name: proc
        hostPath:
          path: /proc
""".format(
        ns=ns,
        node_selector_yaml=_NodeSelectorYaml(),
        tolerations_yaml=_TolerationsYaml(),
    )
    _KubectlApply(manifest)


def _WaitForAgentReady(ns):
    """Wait for ADK agent deployment to be ready."""
    adk_image = FLAGS.gke_adk_image
    if not adk_image:
        logging.info("ADK agent not deployed, skipping rollout wait.")
        return
    timeout = FLAGS.gke_deploy_timeout
    logging.info("Waiting for adk-agent rollout (timeout=%ds)...", timeout)
    _RunKubectl(
        [
            "rollout",
            "status",
            "deployment/adk-agent",
            "-n",
            ns,
            "--timeout={}s".format(timeout),
        ],
        check=False,
    )


def _GetProjectNumber(project):
    """Get GCP project number from project ID."""
    stdout, rc = _RunCmd(
        [
            "gcloud",
            "projects",
            "describe",
            project,
            "--format=value(projectNumber)",
        ],
        check=False,
    )
    return stdout if rc == 0 else None


def _BindSnapshotIAM(bucket_name, project, project_number, ns, ksa_name):
    """Bind IAM roles for pod snapshot access."""
    # bucketViewer to namespace
    _RunCmd(
        [
            "gcloud",
            "storage",
            "buckets",
            "add-iam-policy-binding",
            "gs://{}".format(bucket_name),
            "--member=principalSet://iam.googleapis.com/projects/{}"
            "/locations/global/workloadIdentityPools/{}.svc.id.goog"
            "/namespace/{}".format(project_number, project, ns),
            "--role=roles/storage.bucketViewer",
            "--quiet",
        ],
        check=False,
    )

    # objectAdmin to KSA
    _RunCmd(
        [
            "gcloud",
            "storage",
            "buckets",
            "add-iam-policy-binding",
            "gs://{}".format(bucket_name),
            "--member=principal://iam.googleapis.com/projects/{}"
            "/locations/global/workloadIdentityPools/{}.svc.id.goog"
            "/subject/ns/{}/sa/{}".format(project_number, project, ns, ksa_name),
            "--role=roles/storage.objectAdmin",
            "--quiet",
        ],
        check=False,
    )

    # objectUser to GKE snapshot controller
    _RunCmd(
        [
            "gcloud",
            "storage",
            "buckets",
            "add-iam-policy-binding",
            "gs://{}".format(bucket_name),
            "--member=serviceAccount:service-{}"
            "@container-engine-robot.iam.gserviceaccount.com".format(project_number),
            "--role=roles/storage.objectUser",
            "--quiet",
        ],
        check=False,
    )


def _DeploySnapshotCRDs(ns, bucket_name, snapshot_folder):
    """Deploy PodSnapshotStorageConfig + PodSnapshotPolicy."""
    manifest = """---
apiVersion: podsnapshot.gke.io/v1
kind: PodSnapshotStorageConfig
metadata:
  name: benchmark-pssc-gcs
spec:
  snapshotStorageConfig:
    gcs:
      bucket: "{bucket_name}"
      path: "{snapshot_folder}"
---
apiVersion: podsnapshot.gke.io/v1
kind: PodSnapshotPolicy
metadata:
  name: benchmark-psp
  namespace: {ns}
spec:
  storageConfigName: benchmark-pssc-gcs
  selector:
    matchLabels:
      app: snapshot-benchmark-workload
  triggerConfig:
    type: manual
    postCheckpoint: resume
""".format(ns=ns, bucket_name=bucket_name, snapshot_folder=snapshot_folder)
    _KubectlApply(manifest)
