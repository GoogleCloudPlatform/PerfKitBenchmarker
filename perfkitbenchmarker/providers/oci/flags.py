"""Module containing flags applicable across benchmark run on OCI."""

from absl import flags

VALID_TIERS = ['VM.Standard', 'VM.Optimized']

VALID_SHAPES = ['.A1.Flex', '3.Flex', '.E4.Flex']

flags.DEFINE_string('oci_availability_domain', None, 'The availability domain')

flags.DEFINE_string('oci_fault_domain', None, 'The fault domain')

flags.DEFINE_string('oci_shape', 'VM.Standard.A1.Flex', 'Performance tier to use for the machine type. Defaults to '
                                                        'Standard.')

flags.DEFINE_integer('oci_compute_units', 1, 'Number of compute units to allocate for the machine type')

flags.DEFINE_integer('oci_compute_memory', None, 'Number of memory in gbs to allocate for the machine type')

flags.DEFINE_integer('oci_boot_disk_size', 50, 'Size of Boot disk in GBs')

flags.DEFINE_boolean('oci_use_vcn', True, 'Use in built networking')

flags.DEFINE_integer('oci_num_local_ssds', 0, 'No. of disks')

flags.DEFINE_string(
    'oci_network_name', None, 'The name of an already created '
    'network to use instead of creating a new one.')
flags.DEFINE_string('oci_profile', 'DEFAULT', 'default profile to use')
