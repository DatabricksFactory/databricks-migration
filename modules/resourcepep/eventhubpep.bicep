//Parameters
@description('The name of the virtual network to create.')
param vnetName string 

@description('The name of the subnet to create the private endpoint in.')
param PrivateEndpointSubnetName string 

@description('Resource ID of vnet')
param vnetResourceId string

@description('Resource ID of EventHub')
param eventhubResourceId string

param ctrlDeployEventHub bool

//Variables

var privateEndpointNameeventhub = 'eventhub-pvtEndpoint'

var privateDnsZoneNameeventhub = 'privatelink.servicebus.windows.net'

var pvtEndpointDnsGroupNameeventhub = 'eventhub-pvtEndpoint/mydnsgroupname'

var targetSubResourceEventHub = 'namespace'

var location = resourceGroup().location

//Resources

resource privateEndpointeventhub 'Microsoft.Network/privateEndpoints@2021-08-01' = if(ctrlDeployEventHub) {
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

resource privateDnsZoneeventhub 'Microsoft.Network/privateDnsZones@2020-06-01' = if(ctrlDeployEventHub) {
  name: privateDnsZoneNameeventhub
  location: 'global'
  dependsOn: [
    privateEndpointeventhub
  ]
}

resource privateDnsZoneName_privateDnsZoneName_link_eventhub 'Microsoft.Network/privateDnsZones/virtualNetworkLinks@2020-06-01' = if(ctrlDeployEventHub) {
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

resource pvtEndpointDnsGroupeventhub 'Microsoft.Network/privateEndpoints/privateDnsZoneGroups@2021-12-01' = if(ctrlDeployEventHub) {
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

//Outputs

output eventhubpepResourceOp string = 'Name: ${privateEndpointeventhub.name} - Type: ${privateEndpointeventhub.type} || Name: ${privateDnsZoneeventhub.name} - Type: ${privateDnsZoneeventhub.type} || Name: ${privateDnsZoneName_privateDnsZoneName_link_eventhub.name} - Type: ${privateDnsZoneName_privateDnsZoneName_link_eventhub.type} || Name: ${pvtEndpointDnsGroupeventhub.name} - Type: ${pvtEndpointDnsGroupeventhub.type}'
