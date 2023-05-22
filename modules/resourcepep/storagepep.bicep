//Parameters

@description('The name of the virtual network to create.')
param vnetName string 

@description('The name of the subnet to create the private endpoint in.')
param PrivateEndpointSubnetName string 

@description('Resource ID of blob account')
param blobAccountResourceId string

@description('Resource ID of vnet')
param vnetResourceId string

//Variables

var privateEndpointNamestorage = 'storage-pvtEndpoint'

var privateDnsZoneNamestorage = 'privatelink.dfs.${environment().suffixes.storage}'

var pvtEndpointDnsGroupNamestorage = 'storage-pvtEndpoint/mydnsgroupname'

var targetSubResourceDfs = 'dfs'

var location = resourceGroup().location


//Resources

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
          privateLinkServiceId: blobAccountResourceId
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

