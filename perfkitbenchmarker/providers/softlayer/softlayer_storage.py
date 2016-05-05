"""
Order a block storage (performance ISCSI).

Important manual pages:
http://sldn.softlayer.com/reference/services/SoftLayer_Product_Order
http://sldn.softlayer.com/reference/services/SoftLayer_Product_Order/verifyOrder
http://sldn.softlayer.com/reference/services/SoftLayer_Product_Order/placeOrder
http://sldn.softlayer.com/reference/services/SoftLayer_Product_Package
http://sldn.softlayer.com/reference/services/SoftLayer_Product_Package/getItems
http://sldn.softlayer.com/reference/services/SoftLayer_Location
http://sldn.softlayer.com/reference/services/SoftLayer_Location/getDatacenters
http://sldn.softlayer.com/reference/services/SoftLayer_Network_Storage_Iscsi_OS_Type
http://sldn.softlayer.com/reference/services/SoftLayer_Network_Storage_Iscsi_OS_Type/getAllObjects
http://sldn.softlayer.com/reference/datatypes/SoftLayer_Location
http://sldn.softlayer.com/reference/datatypes/SoftLayer_Container_Product_Order_Network_Storage_Enterprise
http://sldn.softlayer.com/reference/datatypes/SoftLayer_Product_Item_Price
http://sldn.softlayer.com/blog/cmporter/Location-based-Pricing-and-You
http://sldn.softlayer.com/blog/bpotter/Going-Further-SoftLayer-API-Python-Client-Part-3
http://sldn.softlayer.com/article/Object-Filters
http://sldn.softlayer.com/article/Python
http://sldn.softlayer.com/article/Object-Masks

License: http://sldn.softlayer.com/article/License
Author: SoftLayer Technologies, Inc. <sldn@softlayer.com>
"""

import SoftLayer
import json

# Values "AMS01", "AMS03", "CHE01", "DAL05", "DAL06" "FRA02", "HKG02", "LON02", etc.
location = "AMS01"

# Values "20", "40", "80", "100", etc.
storageSize = "40"

# Values between "100" and "6000" by intervals of 100.
iops = "100"

# Values "Hyper-V", "Linux", "VMWare", "Windows 2008+", "Windows GPT", "Windows 2003", "Xen"
os = "Linux"

PACKAGE_ID = 222

client = SoftLayer.create_client_from_env()
productOrderService = client['SoftLayer_Product_Order']
packageService = client['SoftLayer_Product_Package']
locationService = client['SoftLayer_Location']
osService = client['SoftLayer_Network_Storage_Iscsi_OS_Type']

objectFilterDatacenter = {"name": {"operation": location.lower()}}
objectFilterStorageNfs = {"items": {"categories": {"categoryCode": {"operation": "performance_storage_iscsi"}}}}
objectFilterOsType = {"name": {"operation": os}}

try:
    # Getting the datacenter.
    datacenter = locationService.getDatacenters(filter=objectFilterDatacenter)
    # Getting the performance storage NFS prices.
    itemsStorageNfs = packageService.getItems(id=PACKAGE_ID, filter=objectFilterStorageNfs)
    # Getting the storage space prices
    objectFilter = {
        "itemPrices": {
            "item": {
                "capacity": {
                    "operation": storageSize
                }
            },
            "categories": {
                "categoryCode": {
                    "operation": "performance_storage_space"
                }
            },
            "locationGroupId": {
                "operation": "is null"
            }
        }
    }
    pricesStorageSpace = packageService.getItemPrices(id=PACKAGE_ID, filter=objectFilter)
    # If the prices list is empty that means that the storage space value is invalid.
    if len(pricesStorageSpace) == 0:
        raise ValueError('The storage space value: ' + storageSize + ' GB, is not valid.')
    # Getting the IOPS prices
    objectFilter = {
        "itemPrices": {
            "item": {
                "capacity": {
                    "operation": iops
                }
            },
            "attributes": {
                "value": {
                    "operation": storageSize
                }
            },
            "categories": {
                "categoryCode": {
                    "operation": "performance_storage_iops"
                }
            },
            "locationGroupId": {
                "operation": "is null"
            }
        }
    }
    pricesIops = packageService.getItemPrices(id=PACKAGE_ID, filter=objectFilter)
    # If the prices list is empty that means that the IOPS value is invalid for the configured storage space.
    if len(pricesIops) == 0:
        raise ValueError('The IOPS value: ' + iops + ', is not valid for the storage space: ' + storageSize + ' GB.')
    # Getting the OS.
    os = osService.getAllObjects(filter=objectFilterOsType)
    # Building the order template.
    orderData = {
        "complexType": "SoftLayer_Container_Product_Order_Network_PerformanceStorage_Iscsi",
        "packageId": PACKAGE_ID,
        "location": datacenter[0]['id'],
        "quantity": 1,
        "prices": [
            {
                "id": itemsStorageNfs[0]['prices'][0]['id']
            },
            {
                "id": pricesStorageSpace[0]['id']
            },
            {
                "id": pricesIops[0]['id']
            }
        ],
        "osFormatType": os[0]
    }
    # verifyOrder() will check your order for errors. Replace this with a call to
    # placeOrder() when you're ready to order. Both calls return a receipt object
    # that you can use for your records.
    response = productOrderService.verifyOrder(orderData)
    print(json.dumps(response, sort_keys=True, indent=2, separators=(',', ': ')))
except SoftLayer.SoftLayerAPIError as e:
    print("Unable to place the order. faultCode=%s, faultString=%s" % (e.faultCode, e.faultString))