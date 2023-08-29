"""Module containing classes related to Oracle Network."""

import json
import logging
import uuid

from absl import flags
from perfkitbenchmarker import network
from perfkitbenchmarker import providers
from perfkitbenchmarker import resource
from perfkitbenchmarker import vm_util
from perfkitbenchmarker.providers.oci import util

FLAGS = flags.FLAGS

MAX_NAME_LENGTH = 128
WAIT_INTERVAL_SECONDS = 600

VCN_CREATE_STATUSES = frozenset(
    ['AVAILABLE', 'PROVISIONING', 'TERMINATED', 'TERMINATING', 'UPDATING']
)

SUBNET_CREATE_STATUSES = frozenset(
    ['AVAILABLE', 'PROVISIONING', 'TERMINATED', 'TERMINATING', 'UPDATING']
)

IG_CREATE_STATUSES = frozenset(
    ['AVAILABLE', 'PROVISIONING', 'TERMINATED', 'TERMINATING']
)

ROUTE_TABLE_UPDATE_STATUSES = frozenset(
    ['AVAILABLE', 'PROVISIONING', 'TERMINATED', 'TERMINATING']
)

SECURITY_LIST_UPDATE_STATUSES = frozenset(
    ['AVAILABLE', 'PROVISIONING', 'TERMINATED', 'TERMINATING']
)


class OciVcn(resource.BaseResource):
    """An object representing an Oci VCN."""

    def __init__(self, name, region):
        super(OciVcn, self).__init__()
        self.status = None
        self.region = region
        self.id = None
        self.name = name
        self.cidr_blocks = ["172.16.0.0/16"]
        self.cidr_block = None
        self.vcn_id = None
        self.subnet_id = None
        self.ig_id = None
        self.rt_id = None
        self.security_list_id = None
        self.tags = util.MakeFormattedDefaultTags()

    @vm_util.Retry(poll_interval=60, log_errors=False)
    def WaitForVcnStatus(self, status_list):
        """Waits until the disk's status is in status_list."""
        logging.info('Waiting until the instance status is: %s' % status_list)
        status_cmd = util.OCI_PREFIX + [
            'network',
            'vcn',
            'get',
            f'--vcn-id {self.vcn_id}']
        status_cmd = util.GetEncodedCmd(status_cmd)
        out, _ = vm_util.IssueRetryableCommand(status_cmd)
        state = json.loads(out)
        check_state = state['data']['lifecycle-state']
        self.status = check_state
        assert check_state in status_list

    def GetVcnIDFromName(self):
        """Gets VCN OCIid from Name"""
        get_cmd = util.OCI_PREFIX + [
            'network',
            'vcn',
            'list',
            f'--display-name {self.name}']
        get_cmd = util.GetEncodedCmd(get_cmd)
        logging.info(get_cmd)
        stdout, _, _ = vm_util.IssueCommand(get_cmd, raise_on_failure=False)
        response = json.loads(stdout)
        self.vcn_id = response['data'][0]['id']
        logging.info(self.vcn_id)

    def _Create(self):
        """Creates the VPC."""
        logging.info("Creating custom CIDR Block")
        create_cmd = util.OCI_PREFIX + [
            'network',
            'vcn',
            'create',
            f'--display-name pkb-{FLAGS.run_uri}',
            f'--dns-label vcn{FLAGS.run_uri}',
            f'--freeform-tags {self.tags}',
            '--from-json \'{"cidr-blocks":["172.16.0.0/16"]}\'']
        create_cmd = util.GetEncodedCmd(create_cmd)
        logging.info(create_cmd)
        stdout, _, _ = vm_util.IssueCommand(create_cmd, raise_on_failure=False)
        response = json.loads(stdout)
        self.vcn_id = response['data']['id']
        self.cidr_block = response['data']['cidr-block']

    def _Delete(self):
        delete_cmd = util.OCI_PREFIX + [
            'network',
            'vcn',
            'delete',
            f'--vcn-id {self.vcn_id}',
            '--force']
        delete_cmd = util.GetEncodedCmd(delete_cmd)
        stdout, _, _ = vm_util.IssueCommand(delete_cmd, raise_on_failure=False)

    def GetSubnetIdFromVCNId(self):
        """Gets Subnet OCIid from Name"""
        get_cmd = util.OCI_PREFIX + [
            'network',
            'subnet',
            'list',
            f'--vcn-id {self.vcn_id}']
        get_cmd = util.GetEncodedCmd(get_cmd)
        logging.info(get_cmd)
        stdout, _, _ = vm_util.IssueCommand(get_cmd, raise_on_failure=False)
        response = json.loads(stdout)
        self.subnet_id = response['data'][0]['id']

    @vm_util.Retry(poll_interval=60, log_errors=False)
    def WaitForSubnetStatus(self, status_list):
        """Waits until the disk's status is in status_list."""
        logging.info('Waiting until the instance status is: %s' % status_list)
        status_cmd = util.OCI_PREFIX + [
            'network',
            'subnet',
            'get',
            f'--subnet-id {self.subnet_id}']
        status_cmd = util.GetEncodedCmd(status_cmd)
        out, _ = vm_util.IssueRetryableCommand(status_cmd)
        state = json.loads(out)
        check_state = state['data']['lifecycle-state']
        self.status = check_state
        assert check_state in status_list

    def CreateSubnet(self):
        """Creates the VPC."""
        logging.info("Creating custom subnet Block")
        create_cmd = util.OCI_PREFIX + [
            'network',
            'subnet',
            'create',
            f'--display-name pkb-{FLAGS.run_uri}',
            f'--dns-label sub{FLAGS.run_uri}',
            f'--cidr-block {self.cidr_block}',
            f'--vcn-id {self.vcn_id}']
        create_cmd = util.GetEncodedCmd(create_cmd)
        stdout, _, _ = vm_util.IssueCommand(create_cmd, raise_on_failure=False)
        response = json.loads(stdout)
        self.subnet_id = response['data']['id']

    def DeleteSubnet(self):
        """Creates the VPC."""
        logging.info("Creating custom subnet Block")
        create_cmd = util.OCI_PREFIX + [
            'network',
            'subnet',
            'delete',
            f'--subnet-id {self.subnet_id}',
            '--force']
        create_cmd = util.GetEncodedCmd(create_cmd)
        stdout, _, _ = vm_util.IssueCommand(create_cmd, raise_on_failure=False)

    def WaitForInternetGatewayStatus(self, status_list):
        """Waits until the disk's status is in status_list."""
        logging.info('Waiting until the instance status is: %s', status_list)
        status_cmd = util.OCI_PREFIX + [
            'network',
            'internet-gateway',
            'get',
            f'--ig-id {self.ig_id}']
        status_cmd = util.GetEncodedCmd(status_cmd)
        out, _ = vm_util.IssueRetryableCommand(status_cmd)
        state = json.loads(out)
        check_state = state['data']['lifecycle-state']
        self.status = check_state
        assert check_state in status_list

    def CreateInternetGateway(self):
        """Creates the Internet Gateway."""
        logging.info("Creating custom Internet Gateway")
        create_cmd = util.OCI_PREFIX + [
            'network',
            'internet-gateway',
            'create',
            f'--display-name pkb-{FLAGS.run_uri}',
            f'--vcn-id {self.vcn_id}',
            '--is-enabled  True']
        create_cmd = util.GetEncodedCmd(create_cmd)
        stdout, _, _ = vm_util.IssueCommand(create_cmd, raise_on_failure=False)
        response = json.loads(stdout)
        self.ig_id = response['data']['id']

    def DeleteInternetGateway(self):
        """Creates the VPC."""
        logging.info("Creating custom subnet Block")
        create_cmd = util.OCI_PREFIX + [
            'network',
            'internet-gateway',
            'delete',
            f'--ig-id {self.ig_id}',
            '--force']
        create_cmd = util.GetEncodedCmd(create_cmd)
        stdout, _, _ = vm_util.IssueCommand(create_cmd, raise_on_failure=False)

    def WaitForRouteTableStatus(self, status_list):
        """Waits until the disk's status is in status_list."""
        logging.info('Waiting until the instance status is: %s', status_list)
        status_cmd = util.OCI_PREFIX + [
            'network',
            'route-table',
            'get',
            f'--rt-id {self.rt_id}']
        status_cmd = util.GetEncodedCmd(status_cmd)
        out, _ = vm_util.IssueRetryableCommand(status_cmd)
        state = json.loads(out)
        check_state = state['data']['lifecycle-state']
        self.status = check_state
        assert check_state in status_list

    def WaitForSecurityListStatus(self, status_list):
        """Waits until the disk's status is in status_list."""
        logging.info('Waiting until the instance status is: %s', status_list)
        status_cmd = util.OCI_PREFIX + [
            'network',
            'security-list',
            'get',
            f'--security-list-id {self.security_list_id}']
        status_cmd = util.GetEncodedCmd(status_cmd)
        out, _ = vm_util.IssueRetryableCommand(status_cmd)
        state = json.loads(out)
        check_state = state['data']['lifecycle-state']
        self.status = check_state
        assert check_state in status_list

    def UpdateRouteTable(self):
        """Updates the Route Table."""
        logging.info("Update Routing Table with Internet Gateway")
        create_cmd = util.OCI_PREFIX + [
            'network',
            'route-table',
            'update',
            f'--rt-id {self.rt_id}',
            '--force',
            '--route-rules \'[{\"cidrBlock\":"0.0.0.0/0\",\"networkEntityId\":\"%s\"}]\'' % self.ig_id]
        create_cmd = util.GetEncodedCmd(create_cmd)
        stdout, _, _ = vm_util.IssueCommand(create_cmd, raise_on_failure=False)

    def ClearRouteTable(self):
        """Updates the Route Table."""
        logging.info("Update Routing Table with Internet Gateway")
        create_cmd = util.OCI_PREFIX + [
            'network',
            'route-table',
            'update',
            f'--rt-id {self.rt_id}',
            '--force',
            '--route-rules \'[]\'']
        create_cmd = util.GetEncodedCmd(create_cmd)
        stdout, _, _ = vm_util.IssueCommand(create_cmd, raise_on_failure=False)

    def AddSecurityListIngressRule(self, start_port=22, end_port=None, source_range=None):
        if not end_port:
            end_port = start_port
        end_port = end_port or start_port
        source_range = source_range or '0.0.0.0/0'

        current_security_rules = self.GetSecurityListFromId()
        print("Type")
        print(type(current_security_rules))
        print("Length: ")
        print(str(len(current_security_rules)))
        for i in range(0, (len(current_security_rules))):
            print(current_security_rules[i])
        #tcp =6 #udp=17
        """Updates security list to allow traffic on a specific port"""
        logging.info(f"Add ingress rule for ports {start_port} : {end_port}")
        rule_json_string = '{"source": "' + source_range + '","protocol":"6","isStateless": false, "tcpOptions":{\
"destinationPortRange": {"max": ' + str(end_port) + ',"min": ' + str(start_port) + '}}}'
        current_security_rules.append(json.loads(rule_json_string))

        current_security_rules_str = json.dumps(current_security_rules)
        current_security_rules_str = "'" + current_security_rules_str + "'"
        cmd = util.OCI_PREFIX + [
            'network',
            'security-list',
            'update',
            f'--security-list-id {self.security_list_id}',
            '--force',
            f'--ingress-security-rules {current_security_rules_str}']
        print(cmd)
        cmd = util.GetEncodedCmd(cmd)
        stdout, _, _ = vm_util.IssueCommand(cmd, raise_on_failure=False)

    def GetSecurityListFromId(self):
        cmd = util.OCI_PREFIX + [
            'network',
            'security-list',
            'get',
            f'--security-list-id {self.security_list_id}']
        get_cmd = util.GetEncodedCmd(cmd)
        logging.info(get_cmd)
        stdout, _, _ = vm_util.IssueCommand(get_cmd, raise_on_failure=False)
        response = json.loads(stdout)
        ingress_rules = response['data']['ingress-security-rules']
        logging.info(ingress_rules)
        return ingress_rules

    def GetDefaultRouteTableId(self):
        """Get Default Route Table OCI Id."""
        status_cmd = util.OCI_PREFIX + [
            'network',
            'vcn',
            'get',
            f'--vcn-id {self.vcn_id}']
        status_cmd = util.GetEncodedCmd(status_cmd)
        out, _, _ = vm_util.IssueCommand(status_cmd)
        state = json.loads(out)
        self.rt_id = state['data']['default-route-table-id']

    def GetDefaultSecurityListId(self):
        """Get Default Route Table OCI Id."""
        status_cmd = util.OCI_PREFIX + [
            'network',
            'vcn',
            'get',
            f'--vcn-id {self.vcn_id}']
        status_cmd = util.GetEncodedCmd(status_cmd)
        out, _, _ = vm_util.IssueCommand(status_cmd)
        state = json.loads(out)
        self.security_list_id = state['data']['default-security-list-id']


class OciNetwork(network.BaseNetwork):
    """Object representing a AliCloud Network."""

    CLOUD = providers.OCI

    def __init__(self, spec):
        super(OciNetwork, self).__init__(spec)
        self.name = FLAGS.oci_network_name or ('perfkit-%s-%s' % (FLAGS.run_uri, str(uuid.uuid4())[-12:]))
        self.region = spec.zone
        self.use_vcn = FLAGS.oci_use_vcn
        self.network_id = None
        self.vcn_id = None

        if self.use_vcn:
            self.vcn = OciVcn(self.name, self.region)
            self.security_group = None

    #        else:
    #            self.vcn = OciVcn(self.name, self.region)
    #            self.security_group = None

    @vm_util.Retry()
    def Create(self):
        """Creates the network."""
        if self.use_vcn:
            self.vcn.Create()
            self.vcn.WaitForVcnStatus(["AVAILABLE"])
            self.vcn.GetDefaultRouteTableId()
            self.vcn.GetDefaultSecurityListId()
            self.vcn.CreateSubnet()
            self.vcn.WaitForSubnetStatus(["AVAILABLE"])
            self.network_id = self.vcn.subnet_id
            self.vcn.CreateInternetGateway()
            self.vcn.WaitForInternetGatewayStatus(["AVAILABLE"])
            self.vcn.UpdateRouteTable()
            self.vcn.WaitForRouteTableStatus(["AVAILABLE"])
            # Add opening in VCN for SSH
            self.vcn.AddSecurityListIngressRule(start_port=22)
            self.vcn.WaitForSecurityListStatus(["AVAILABLE"])
        else:
            self.vcn.GetVcnIDFromName()
            self.vcn.GetSubnetIdFromVCNId()
            self.network_id = self.vcn.subnet_id

    def Delete(self):
        """Deletes the network."""
        if self.use_vcn:
            self.vcn.ClearRouteTable()
            self.vcn.DeleteInternetGateway()
            self.vcn.DeleteSubnet()
            self.vcn.Delete()


class OCIFirewall(network.BaseFirewall):

    CLOUD = provider_info.OCI

    def __init__(self):
        super(OCIFirewall, self).__init__()

    def AllowPort(self, vm, start_port, end_port=None, source_range=None):
        """
        Open a port range on a specific vm. This seems to normally be called by the vm object.

        :param vm:
        :param start_port:
        :param end_port:
        :return:
        """

        if not vm.network.vcn:
            # TODO: What happens when we do not have a vcn? Is that possible?
            logging.error('Opening ports with OCI cloud only supported when using a VCN for now!')

        else:
            vm.network.vcn.AddSecurityListIngressRule(start_port, end_port=end_port, source_range=source_range)
            vm.network.vcn.WaitForRouteTableStatus(["AVAILABLE"])