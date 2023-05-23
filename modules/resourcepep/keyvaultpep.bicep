//Parameters
@description('The name of the virtual network to create.')
param vnetName string 

@description('The name of the subnet to create the private endpoint in.')
param PrivateEndpointSubnetName string 

@description('Resource ID of vnet')
param vnetResourceId string

@description('Resource ID of EventHub')
param keyvaultResourceId string

//Variables

var privateEndpointNamevault = 'keyvault-pvtEndpoint'

var privateDnsZoneNamevault = 'privatelink.vaultcore.azure.net'

var pvtEndpointDnsGroupNamevault = 'keyvault-pvtEndpoint/mydnsgroupname'

var targetSubResourceVault = 'vault'

var location = resourceGroup().location

//Resources

resource privateEndpointvault 'Microsoft.Network/privateEndpoints@2021-08-01' = {
  name: privateEndpointNamevault
  location: location
  properties: {
    subnet: {
      id: resourceId('Microsoft.Network/virtualNetworks/subnets', vnetName, PrivateEndpointSubnetName)
    }
    privateLinkServiceConnections: [
      {
        name: privateEndpointNamevault
        properties: {
          privateLinkServiceId: keyvaultResourceId
          groupIds: [
            targetSubResourceVault
          ]
        }
      }
    ]
  }
}

resource privateDnsZonevault 'Microsoft.Network/privateDnsZones@2020-06-01' = {
  name: privateDnsZoneNamevault
  location: 'global'
  dependsOn: [
    privateEndpointvault
  ]
}

resource privateDnsZoneName_privateDnsZoneName_link_vault 'Microsoft.Network/privateDnsZones/virtualNetworkLinks@2020-06-01' = {
  parent: privateDnsZonevault
  name: '${privateDnsZoneNamevault}-link'
  location: 'global'
  properties: {
    registrationEnabled: false
    virtualNetwork: {
      id: vnetResourceId
    }
  }
}

resource pvtEndpointDnsGroupvault 'Microsoft.Network/privateEndpoints/privateDnsZoneGroups@2021-12-01' = {
  name: pvtEndpointDnsGroupNamevault
  properties: {
    privateDnsZoneConfigs: [
      {
        name: 'config1vault'
        properties: {
          privateDnsZoneId: privateDnsZonevault.id
        }
      }
    ]
  }
  dependsOn: [
    privateEndpointvault
  ]
}

//Outputs

output keyvaultpepResourceOp string = 'Name: ${privateEndpointvault.name} - Type: ${privateEndpointvault.type} || Name: ${privateDnsZonevault.name} - Type: ${privateDnsZonevault.type} || Name: ${privateDnsZoneName_privateDnsZoneName_link_vault.name} - Type: ${privateDnsZoneName_privateDnsZoneName_link_vault.type} || Name: ${pvtEndpointDnsGroupvault.name} - Type: ${pvtEndpointDnsGroupvault.type}'
