//Parameters

param blobAccountName string 

@description('The name of the virtual network to create.')
param vnetName string 

@description('The name of the public subnet to create.')
param publicSubnetName string 

param containerName string 

@description('The name of the subnet to create the private endpoint in.')
param PrivateEndpointSubnetName string 

@description('Resource ID of vnet')
param vnetResourceId string

//Variables

var privateEndpointNamestorage = 'storage-pvtEndpoint'

var privateDnsZoneNamestorage = 'privatelink.dfs.${environment().suffixes.storage}'

var pvtEndpointDnsGroupNamestorage = 'storage-pvtEndpoint/mydnsgroupname'

var targetSubResourceDfs = 'dfs'

var location = resourceGroup().location

//Resources

resource blobAccountPrivateHybrid 'Microsoft.Storage/storageAccounts@2021-04-01' = {
  name: blobAccountName
  location: location
  kind: 'StorageV2'
  sku: {
    name: 'Standard_LRS'
  }
  properties: {
    isHnsEnabled: true
    networkAcls: {
      bypass: 'AzureServices'
      virtualNetworkRules: [
        {
          id: resourceId('Microsoft.Network/virtualNetworks/subnets', vnetName, publicSubnetName)
          action: 'Allow'
          state: 'succeeded'
        }
      ]
      ipRules: []
      defaultAction: 'Allow'
    }
    minimumTlsVersion: 'TLS1_2'
    allowBlobPublicAccess: false
    supportsHttpsTrafficOnly: true
    accessTier: 'Hot'
  }
}

resource blobAccountName_default_container 'Microsoft.Storage/storageAccounts/blobServices/containers@2021-06-01' = {
  name: '${blobAccountName}/default/${containerName}'
  dependsOn: [
    blobAccountPrivateHybrid
  ]
}

resource privateEndpointstorage 'Microsoft.Network/privateEndpoints@2021-08-01' = {
  name: privateEndpointNamestorage
  location: location
  properties: {
    subnet: {
      id: resourceId('Microsoft.Network/virtualNetworks/subnets', vnetName, PrivateEndpointSubnetName)
    }
    privateLinkServiceConnections: [
      {
        name: privateEndpointNamestorage
        properties: {
          privateLinkServiceId: blobAccountPrivateHybrid.id
          groupIds: [
            targetSubResourceDfs
          ]
        }
      }
    ]
  }
}

resource privateDnsZonestorage 'Microsoft.Network/privateDnsZones@2020-06-01' = {
  name: privateDnsZoneNamestorage
  location: 'global'
  dependsOn: [
    privateEndpointstorage
  ]
}

resource privateDnsZoneName_privateDnsZoneName_link_storage 'Microsoft.Network/privateDnsZones/virtualNetworkLinks@2020-06-01' = {
  parent: privateDnsZonestorage
  name: '${privateDnsZoneNamestorage}-link'
  location: 'global'
  properties: {
    registrationEnabled: false
    virtualNetwork: {
      id: vnetResourceId
    }
  }
}

resource pvtEndpointDnsGroupstorage 'Microsoft.Network/privateEndpoints/privateDnsZoneGroups@2021-12-01' = {
  name: pvtEndpointDnsGroupNamestorage
  properties: {
    privateDnsZoneConfigs: [
      {
        name: 'config1storage'
        properties: {
          privateDnsZoneId: privateDnsZonestorage.id
        }
      }
    ]
  }
  dependsOn: [
    privateEndpointstorage
  ]
}

//Outputs

output blobAccountResourceOp string = 'Name: ${blobAccountPrivateHybrid.name} - Type: ${blobAccountPrivateHybrid.type} || Name: ${privateEndpointstorage.name} - Type: ${privateEndpointstorage.type} || Name: ${privateDnsZonestorage.name} - Type: ${privateDnsZonestorage.type} || Name: ${privateDnsZoneName_privateDnsZoneName_link_storage.name} - Type: ${privateDnsZoneName_privateDnsZoneName_link_storage.type} || Name: ${pvtEndpointDnsGroupstorage} - Type: ${pvtEndpointDnsGroupstorage}'
