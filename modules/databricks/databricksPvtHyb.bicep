//Parameters

@description('The name of the Azure Databricks workspace to create.')
param workspaceName string 

param pricingTier string 

@description('The name of the public subnet to create.')
param publicSubnetName string 

@description('The name of the private subnet to create.')
param privateSubnetName string

@description('Specifies whether to deploy Azure Databricks workspace with secure cluster connectivity (SCC) enabled or not (No Public IP).')
param disablePublicIp bool

param requiredNsgRules string 

param customVirtualNetworkResourceId string 

@description('The name of the virtual network to create.')
param vnetName string 

@description('The name of the subnet to create the private endpoint in.')
param PrivateEndpointSubnetName string 

@description('Resource ID of VNet')
param vnetResourceId string

//Variables

var location = resourceGroup().location

var managedResourceGroupName = 'databricks-rg-${workspaceName}-${uniqueString(workspaceName, resourceGroup().id)}'

var trimmedMRGName = substring(managedResourceGroupName, 0, min(length(managedResourceGroupName), 90))

var managedResourceGroupId = '${subscription().id}/resourceGroups/${trimmedMRGName}'

var privateEndpointName = '${workspaceName}-pvtEndpoint'

var privateDnsZoneName = 'privatelink.azuredatabricks.net'

var pvtEndpointDnsGroupName = '${privateEndpointName}/mydnsgroupname'

//Resources

resource databricks_privateEndpointHybrid 'Microsoft.Databricks/workspaces@2018-04-01' = {
  location: location
  name: workspaceName
  sku: {
    name: pricingTier
  }
  properties: {
    managedResourceGroupId: managedResourceGroupId
    parameters: {
      customVirtualNetworkId: {
        value: customVirtualNetworkResourceId
      }
      customPublicSubnetName: {
        value: publicSubnetName
      }
      customPrivateSubnetName: {
        value: privateSubnetName
      }
      enableNoPublicIp: {
        value: disablePublicIp
      }
    }
    publicNetworkAccess: 'Enabled'
    requiredNsgRules: requiredNsgRules
  }
}

resource privateEndpoint 'Microsoft.Network/privateEndpoints@2021-08-01' = {
  name: privateEndpointName
  location: location
  properties: {
    subnet: {
      id: resourceId('Microsoft.Network/virtualNetworks/subnets', vnetName, PrivateEndpointSubnetName)
    }
    privateLinkServiceConnections: [
      {
        name: privateEndpointName
        properties: {
          privateLinkServiceId: databricks_privateEndpointHybrid.id
          groupIds: [
            'databricks_ui_api'
          ]
        }
      }
    ]
  }
}

resource privateDnsZoneName_privateDnsZoneName_link 'Microsoft.Network/privateDnsZones/virtualNetworkLinks@2020-06-01' = {
  parent: privateDnsZone
  name: '${privateDnsZoneName}-link'
  location: 'global'
  properties: {
    registrationEnabled: false
    virtualNetwork: {
      id: vnetResourceId
    }
  }
}

resource privateDnsZone 'Microsoft.Network/privateDnsZones@2020-06-01' = {
  name: privateDnsZoneName
  location: 'global'
  dependsOn: [
    privateEndpoint
  ]
}

resource pvtEndpointDnsGroup 'Microsoft.Network/privateEndpoints/privateDnsZoneGroups@2021-12-01' = {
  name: pvtEndpointDnsGroupName
  properties: {
    privateDnsZoneConfigs: [
      {
        name: 'config1'
        properties: {
          privateDnsZoneId: privateDnsZone.id
        }
      }
    ]
  }
  dependsOn: [
    privateEndpoint
  ]
}

//Outputs

output databricksPvtHybResourceId string = databricks_privateEndpointHybrid.id

output databricksPvtHybResourceOp string = 'Name: ${databricks_privateEndpointHybrid.name} - Type: ${databricks_privateEndpointHybrid.type} \n Name: ${privateEndpoint.name} - Type: ${privateEndpoint.type} \n Name: ${privateDnsZoneName_privateDnsZoneName_link.name} - Type: ${privateDnsZoneName_privateDnsZoneName_link.type} \n Name: ${privateDnsZone.name} - Type: ${privateDnsZone.type} \n Name: ${pvtEndpointDnsGroup.name} - Type: ${pvtEndpointDnsGroup.type}'

