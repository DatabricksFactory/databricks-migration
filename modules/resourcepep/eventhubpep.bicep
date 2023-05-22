//Parameters
@description('The name of the virtual network to create.')
param vnetName string 

@description('The name of the subnet to create the private endpoint in.')
param PrivateEndpointSubnetName string 

@description('Resource ID of vnet')
param vnetResourceId string

@description('Resource ID of EventHub')
param eventhubResourceId string

//Variables

var privateEndpointNameeventhub = 'eventhub-pvtEndpoint'

var privateDnsZoneNameeventhub = 'privatelink.servicebus.windows.net'

var pvtEndpointDnsGroupNameeventhub = 'eventhub-pvtEndpoint/mydnsgroupname'

var targetSubResourceEventHub = 'namespace'

var location = resourceGroup().location

//Resources

resource privateEndpointeventhub 'Microsoft.Network/privateEndpoints@2021-08-01' = {
  name: privateEndpointNameeventhub
  location: location
  properties: {
    subnet: {
      id: resourceId('Microsoft.Network/virtualNetworks/subnets', vnetName, PrivateEndpointSubnetName)
    }
    privateLinkServiceConnections: [
      {
        name: privateEndpointNameeventhub
        properties: {
          privateLinkServiceId: eventhubResourceId
          groupIds: [
            targetSubResourceEventHub
          ]
        }
      }
    ]
  }
}

resource privateDnsZoneeventhub 'Microsoft.Network/privateDnsZones@2020-06-01' = {
  name: privateDnsZoneNameeventhub
  location: 'global'
  dependsOn: [
    privateEndpointeventhub
  ]
}

resource privateDnsZoneName_privateDnsZoneName_link_eventhub 'Microsoft.Network/privateDnsZones/virtualNetworkLinks@2020-06-01' = {
  parent: privateDnsZoneeventhub
  name: '${privateDnsZoneNameeventhub}-link'
  location: 'global'
  properties: {
    registrationEnabled: false
    virtualNetwork: {
      id: vnetResourceId
    }
  }
}

resource pvtEndpointDnsGroupeventhub 'Microsoft.Network/privateEndpoints/privateDnsZoneGroups@2021-12-01' = {
  name: pvtEndpointDnsGroupNameeventhub
  properties: {
    privateDnsZoneConfigs: [
      {
        name: 'config1eventhub'
        properties: {
          privateDnsZoneId: privateDnsZoneeventhub.id
        }
      }
    ]
  }
  dependsOn: [
    privateEndpointeventhub
  ]
}
