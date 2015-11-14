# Copyright 2015 PerfKitBenchmarker Authors. All rights reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""Cloudstack utils"""

import urllib
from csapi import API
from perfkitbenchmarker import flags

try:
    from requests.packages import urllib3
    urllib3.disable_warnings()
except ImportError:
    pass

FLAGS = flags.FLAGS


class CsClient(object):

    def __init__(self, url, api_key, secret):

        self._cs = API(api_key,
                       secret,
                       url,
                       logging=False)

    def get_zone(self, zone_name):

        cs_args = {
            'command': 'listZones'
        }

        zones = self._cs.request(cs_args)

        if zones and 'zone' in zones:
            for zone in zones['zone']:
                if zone['name'] == zone_name:
                    return zone

        return None

    def get_template(self, template_name, project_id=None):

        cs_args = {
            'command': 'listTemplates',
            'templatefilter': 'executable'
        }

        if project_id:
            cs_args.update({'projectid': project_id})


        templates = self._cs.request(cs_args)

        if templates and 'template' in templates:
            for templ in templates['template']:
                if templ['name'] == template_name:
                    return templ

        return None

    def get_serviceoffering(self, service_offering_name):

        cs_args = {
            'command': 'listServiceOfferings',
        }

        service_offerings = self._cs.request(cs_args)

        if service_offerings and 'serviceoffering' in service_offerings:
            for servo in service_offerings['serviceoffering']:
                if servo['name'] == service_offering_name:
                    return servo

        return None


    def get_project(self, project_name):

        cs_args = {
            'command': 'listProjects'
        }

        projects = self._cs.request(cs_args)

        if projects and 'project' in projects:
            for proj in projects['project']:
                if proj['name'] == project_name:
                    return proj

        return None

    def get_network(self, network_name, project_id=None, vpc_id=None):

        cs_args = {
            'command': 'listNetworks',
        }

        if project_id:
            cs_args.update({"projectid": project_id})

        if vpc_id:
            cs_args.update({"vpcid": vpc_id})

        networks = self._cs.request(cs_args)

        if networks and 'network' in networks:
            for network in networks['network']:
                if network['name'] == network_name:
                    return network

        return None

    def get_network_offering(self, network_offering_name, project_id):
        cs_args = {
            'command': 'listNetworkOfferings',
        }

        nw_offerings = self._cs.request(cs_args)

        if nw_offerings and 'networkoffering' in nw_offerings:
            for nw_off in nw_offerings['networkoffering']:
                if nw_off['name'] == network_offering_name:
                    return nw_off

        return None

    def get_vpc(self, vpc_name, project_id=None):

        cs_args = {
            'command': 'listVPCs',
        }

        if project_id:
            cs_args.update({"projectid": project_id})

        vpcs = self._cs.request(cs_args)

        if vpcs and 'vpc' in vpcs:
            for vpc in vpcs['vpc']:
                if vpc['name'] == vpc_name:
                    return vpc

        return None

    def get_vpc_offering(self, vpc_offering_name):

        cs_args = {
            'command': 'listVPCOfferings',
        }

        vpc_offerings = self._cs.request(cs_args)

        if vpc_offerings and 'vpcoffering' in vpc_offerings:
            for vpc_off in vpc_offerings['vpcoffering']:
                if vpc_off['name'] == vpc_offering_name:
                    return vpc_off

        return None

    def get_virtual_machine(self, vm_name, project_id=None):

        cs_args = {
            'command': 'listVirtualMachines',
        }

        if project_id:
            cs_args.update({"projectid": project_id})

        vms = self._cs.request(cs_args)

        if vms and 'virtualmachine' in vms:
            for vm in vms['virtualmachine']:
                if vm['name'] == vm_name:
                    return vm

        return None

    def create_vm(self,
                  name,
                  zone_id,
                  service_offering_id,
                  template_id,
                  network_ids=None,
                  keypair=None,
                  project_id=None):

        create_vm_args = {
            'command': 'deployVirtualMachine',
            'serviceofferingid': service_offering_id,
            'templateid': template_id,
            'zoneid': zone_id,
            'name': name,
        }

        if network_ids:
            create_vm_args.update({"networkids": network_ids})

        if keypair:
            create_vm_args.update({'keypair': keypair})

        if project_id:
            create_vm_args.update({"projectid": project_id})

        vm = self._cs.request(create_vm_args)

        return vm

    def delete_vm(self, vm_id):

        cs_args = {
            'command': 'destroyVirtualMachine',
            'id': vm_id,
            'expunge': 'true'  # Requres root/domain admin
        }

        res = self._cs.request(cs_args)
        return res

    def create_vpc(self, name, zone_id, cidr, vpc_offering_id, project_id=None):

        cs_args = {
            'command': 'createVPC',
            'name': name,
            'displaytext': name,
            'vpcofferingid': vpc_offering_id,
            'cidr': cidr,
            'zoneid': zone_id,
        }

        if project_id:
            cs_args.update({"projectid": project_id})

        vpc = self._cs.request(cs_args)

        if vpc and 'vpc' in vpc:
            return vpc['vpc']

        return None

    def delete_vpc(self, vpc_id):

        cs_args = {
            'command': 'deleteVPC',
            'id': vpc_id
        }

        res = self._cs.request(cs_args)

        return res

    def create_network(self,
                       name,
                       network_offering_id,
                       zone_id,
                       project_id=None,
                       vpc_id=None,
                       gateway=None,
                       netmask=None,
                       acl_id=None):

        cs_args = {
            'command': 'createNetwork',
            'name': name,
            'displaytext': name,
            'zoneid': zone_id,
            'networkofferingid': network_offering_id,
        }

        if project_id:
            cs_args.update({"projectid": project_id})

        if vpc_id:
            cs_args.update({
                'vpcid': vpc_id,
                'gateway': gateway,
                'netmask': netmask,
                'aclid': acl_id
            })

        nw = self._cs.request(cs_args)

        if nw and 'network' in nw:
            return nw['network']

        return nw

    def delete_network(self, network_id):

        cs_args = {
            'command': 'deleteNetwork',
            'id': network_id,
        }

        res = self._cs.request(cs_args)
        return res

    def alloc_public_ip(self, network_id, is_vpc=False):

        cs_args = {
            'command': 'associateIpAddress',
        }

        if is_vpc:
            cs_args.update({'vpcid': network_id})
        else:
            cs_args.update({'networkid': network_id})

        res = self._cs.request(cs_args)

        if res and 'ipaddress' in res:
            return res['ipaddress']

        return None

    def release_public_ip(self, ipaddress_id):

        cs_args = {
            'command': 'disassociateIpAddress',
            'id': ipaddress_id
        }

        res = self._cs.request(cs_args)

        return res

    def enable_static_nat(self, ip_address_id, vm_id, network_id):

        cs_args = {
            'command': 'enableStaticNat',
            'ipaddressid': ip_address_id,
            'virtualmachineid': vm_id
        }

        if network_id:
            cs_args.update({'networkid': network_id})

        res = self._cs.request(cs_args)

        if res and 'success' in res:
            return res['success']

        return None

    def snat_rule_exists(self, ip_address_id, vm_id):

        cs_args = {
            'command': 'listPublicIpAddresses',
            'id': ip_address_id
        }

        res = self._cs.request(cs_args)

        assert 'publicipaddress' in res, "No public IP address found"
        assert len(res['publicipaddress']) == 1, "More than One\
                Public IP address"

        res = res['publicipaddress'][0]

        if res and 'virtualmachineid' in res and \
           res['virtualmachineid'] == vm_id:
            return True

        return False


    def register_ssh_keypair(self, name, public_key, project_id=None):

        cs_args = {
            'command': 'registerSSHKeyPair',
            'name': name,
            'publickey': urllib.quote(public_key),
        }

        if project_id:
            cs_args.update({"projectid": project_id})

        res = self._cs.request(cs_args, method='post')
        return res


    def unregister_ssh_keypair(self, name, project_id=None):

        cs_args = {
            'command': 'deleteSSHKeyPair',
            'name': name,
        }

        if project_id:
            cs_args.update({"projectid": project_id})

        res = self._cs.request(cs_args)
        return res

    def get_ssh_keypair(self, name, project_id=None):

        cs_args = {
            'command': 'listSSHKeyPairs',
            'name': name,
        }

        if project_id:
            cs_args.update({"projectid": project_id})

        kps = self._cs.request(cs_args)

        if kps and 'sshkeypair' in kps:
            for kp in kps['sshkeypair']:
                if kp['name'] == name:
                    return kp

        return None

    def get_network_acl(self, name, project_id=None):

        cs_args = {
            'command': 'listNetworkACLLists',
        }

        if project_id:
            cs_args.update({"projectid": project_id})

        acllist = self._cs.request(cs_args)

        for acl in acllist['networkacllist']:
            if acl['name'] == name:
                return acl

        return None

    def create_volume(self, name, diskoffering_id, zone_id, project_id=None):

        cs_args = {
            'command': 'createVolume',
            'diskofferingid': diskoffering_id,
            'zoneid': zone_id,
            'name': name
        }

        if project_id:
            cs_args.update({'projectid': project_id})

        vol = self._cs.request(cs_args)

        if vol and 'volume' in vol:
            return vol['volume']

        return None


    def get_volume(self, name, project_id=None):

        cs_args = {
            'command': 'listVolumes',
        }

        if project_id:
            cs_args.update({"projectid": project_id})

        vols = self._cs.request(cs_args)

        if vols and 'volume' in vols:
            for v in vols['volume']:
                if v['name'] == name:
                    return v

        return None

    def delete_volume(self, volume_id):

        cs_args = {
            'command': 'deleteVolume',
            'id': volume_id
        }

        res = self._cs.request(cs_args)
        return res

    def attach_volume(self, vol_id, vm_id):

        cs_args = {
            'command': 'attachVolume',
            'id': vol_id,
            'virtualmachineid': vm_id
        }

        res = self._cs.request(cs_args)

        if res and 'volume' in res:
            return res['volume']

        return None


    def detach_volume(self, vol_id):

        cs_args = {
            'command': 'detachVolume',
            'id': vol_id,
        }

        res = self._cs.request(cs_args)
        return res

    def list_disk_offerings(self):

        cs_args = {
            'command': 'listDiskOfferings',
        }

        disk_off = self._cs.request(cs_args)

        if disk_off and 'diskoffering' in disk_off:
            return disk_off['diskoffering']

        return None
