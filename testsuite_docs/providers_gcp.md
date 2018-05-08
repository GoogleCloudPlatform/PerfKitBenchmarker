### [perfkitbenchmarker.providers.gcp.flags ](perfkitbenchmarker/providers/gcp/flags.py)

#### Description:

Module containing flags applicable across benchmark run on GCP.

#### Flags:

`--additional_gcloud_flags`: Additional flags to pass to gcloud.
    (default: '')
    (a comma separated list)

`--gce_accelerator_type_override`: When specified, override the accelerator_type
    string passed to the gcloud compute instance create command.

`--gce_boot_disk_size`: The boot disk size in GB for GCP VMs.
    (an integer)

`--gce_boot_disk_type`: <pd-standard|pd-ssd>: The boot disk type for GCP VMs.

`--[no]gce_migrate_on_maintenance`: If true, allow VM migration on GCE host
    maintenance.
    (default: 'true')

`--gce_network_name`: The name of an already created network to use instead of
    creating a new one.

`--gce_num_local_ssds`: The number of ssds that should be added to the VM. Note
    that this is currently only supported in certain zones (see
    https://cloud.google.com/compute/docs/local-ssd).
    (default: '0')
    (an integer)

`--[no]gce_preemptible_vms`: If true, use preemptible VMs on GCE.
    (default: 'false')

`--gce_remote_access_firewall_rule`: The name of an already created firewall
    rule which allows remote access instead of creating a new one.

`--gce_ssd_interface`: <SCSI|NVME>: The ssd interface for GCE local SSD.
    (default: 'SCSI')

`--gce_subnet_addr`: Address range to the subnet, given in CDR notation. Not
    used unless --gce_subnet_region is given.
    (default: '10.128.0.0/20')

`--gce_subnet_region`: Region to create subnet in instead of automatically
    creating one in every region.

`--gcloud_path`: The path for the gcloud utility.
    (default: 'gcloud')

`--gcloud_scopes`: If set, space-separated list of scopes to apply to every
    created machine

`--gcp_host_type`: The host type of all sole tenant hosts that get created.

`--gcp_instance_metadata`: A colon separated key-value pair that will be added
    to the "--metadata" flag of the gcloud cli (with the colon replaced by the
    equal sign). Multiple key-value pairs may be specified by separating each
    pair by commas. This option can be repeated multiple times. For information
    about GCP instance metadata, see: --metadata from `gcloud help compute
    instances create`.;
    repeat this option to specify a list of values
    (default: '[]')

`--gcp_instance_metadata_from_file`: A colon separated key-value pair that will
    be added to the "--metadata-from-file" flag of the gcloud cli (with the
    colon replaced by the equal sign). Multiple key-value pairs may be specified
    by separating each pair by commas. This option can be repeated multiple
    times. For information about GCP instance metadata, see: --metadata-from-
    file from `gcloud help compute instances create`.;
    repeat this option to specify a list of values
    (default: '[]')

`--gcp_min_cpu_platform`:
    <none|sandybridge|ivybridge|haswell|broadwell|skylake>: When specified, the
    VM will have either the specified architecture or a newer one. Architecture
    availability is zone dependent.

`--gcp_num_vms_per_host`: The number of VMs per dedicated host. If None, VMs
    will be packed on a single host until no more can be packed at which point a
    new host will be created.
    (an integer)

`--gcp_preprovisioned_data_bucket`: GCS bucket where pre-provisioned data has
    been copied.

`--gcp_redis_gb`: Size of redis cluster in gb
    (default: '5')
    (an integer)

`--image_family`: The family of the image that the boot disk will be initialized
    with. The --image flag will take priority over this flag. See:
    https://cloud.google.com/sdk/gcloud/reference/compute/instances/create

`--image_project`: The project against which all image references will be
    resolved. See:
    https://cloud.google.com/sdk/gcloud/reference/compute/disks/create

### [perfkitbenchmarker.providers.gcp.gcp_dpb_dataflow ](perfkitbenchmarker/providers/gcp/gcp_dpb_dataflow.py)

#### Description:

Module containing class for GCP's dataflow service.

No Clusters can be created or destroyed, since it is a managed solution
See details at: https://cloud.google.com/dataflow/


#### Flags:

`--dpb_dataflow_runner`: Flag to specify the pipeline runner at runtime.
    (default: 'DataflowRunner')

`--dpb_dataflow_sdk`: SDK used to build the Dataflow executable.

`--dpb_dataflow_staging_location`: Google Cloud Storage bucket for Dataflow to
    stage the binary and any temporary files. You must create this bucket ahead
    of time, before running your pipeline.

### [perfkitbenchmarker.providers.gcp.gcp_dpb_dataproc ](perfkitbenchmarker/providers/gcp/gcp_dpb_dataproc.py)

#### Description:

Module containing class for GCP's dataproc service.

Clusters can be created, have jobs submitted to them and deleted. See details
at https://cloud.google.com/dataproc/


#### Flags:

`--dpb_dataproc_distcp_num_maps`: Number of maps to copy data.
    (an integer)

`--dpb_dataproc_image_version`: The image version to use for the cluster.

### [perfkitbenchmarker.providers.gcp.gcp_spanner ](perfkitbenchmarker/providers/gcp/gcp_spanner.py)

#### Description:

Module containing class for GCP's spanner instances.

Instances can be created and deleted.


#### Flags:

`--cloud_spanner_config`: The config for the Cloud Spanner instance.
    (default: 'regional-us-central1')

`--cloud_spanner_nodes`: The number of nodes for the Cloud Spanner instance.
    (default: '1')
    (an integer)

`--cloud_spanner_project`: The project for the Cloud Spanner instance. Use
    default project if unset.

### [perfkitbenchmarker.providers.gcp.gcs ](perfkitbenchmarker/providers/gcp/gcs.py)

#### Description:

Interface to Google Cloud Storage.

#### Flags:

`--google_cloud_sdk_version`: Use a particular version of the Google Cloud SDK,
    e.g.: 103.0.0

