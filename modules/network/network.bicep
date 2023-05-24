//Parameters

@description('The name of the network security group to create.')
param nsgName string

@description('The name of the virtual network to create.')
param vnetName string 

@description('CIDR range for the vnet.')
param vnetCidr string 

@description('The name of the public subnet to create.')
param publicSubnetName string 

@description('CIDR range for the public subnet.')
param publicSubnetCidr string 

@description('The name of the private subnet to create.')
param privateSubnetName string 

@description('CIDR range for the private subnet.')
param privateSubnetCidr string 

@description('The name of the subnet to create the private endpoint in.')
param PrivateEndpointSubnetName string 

@description('CIDR range for the private endpoint subnet..')
param privateEndpointSubnetCidr string 

param endpointType string

//Variables

var location = resourceGroup().location

// NSG

resource nsg 'Microsoft.Network/networkSecurityGroups@2021-03-01' = if(endpointType == 'PrivateMode' || endpointType == 'HybridMode') {
  name: nsgName
  location: location
  properties: {
    securityRules: [
      {
        name: 'Microsoft.Databricks-workspaces_UseOnly_databricks-worker-to-worker-inbound'
        properties: {
          description: 'Required for worker nodes communication within a cluster.'
          protocol: '*'
          sourcePortRange: '*'
          destinationPortRange: '*'
          sourceAddressPrefix: 'VirtualNetwork'
          destinationAddressPrefix: 'VirtualNetwork'
          access: 'Allow'
          priority: 100
          direction: 'Inbound'
        }
      }
      {
        name: 'Microsoft.Databricks-workspaces_UseOnly_databricks-worker-to-databricks-webapp'
        properties: {
          description: 'Required for workers communication with Databricks Webapp.'
          protocol: 'Tcp'
          sourcePortRange: '*'
          destinationPortRange: '443'
          sourceAddressPrefix: 'VirtualNetwork'
          destinationAddressPrefix: 'AzureDatabricks'
          access: 'Allow'
          priority: 100
          direction: 'Outbound'
        }
      }
      {
        name: 'Microsoft.Databricks-workspaces_UseOnly_databricks-worker-to-sql'
        properties: {
          description: 'Required for workers communication with Azure SQL services.'
          protocol: 'Tcp'
          sourcePortRange: '*'
          destinationPortRange: '3306'
          sourceAddressPrefix: 'VirtualNetwork'
          destinationAddressPrefix: 'Sql'
          access: 'Allow'
          priority: 101
          direction: 'Outbound'
        }
      }
      {
        name: 'Microsoft.Databricks-workspaces_UseOnly_databricks-worker-to-storage'
        properties: {
          description: 'Required for workers communication with Azure Storage services.'
          protocol: 'Tcp'
          sourcePortRange: '*'
          destinationPortRange: '443'
          sourceAddressPrefix: 'VirtualNetwork'
          destinationAddressPrefix: 'Storage'
          access: 'Allow'
          priority: 102
          direction: 'Outbound'
        }
      }
      {
        name: 'Microsoft.Databricks-workspaces_UseOnly_databricks-worker-to-worker-outbound'
        properties: {
          description: 'Required for worker nodes communication within a cluster.'
          protocol: '*'
          sourcePortRange: '*'
          destinationPortRange: '*'
          sourceAddressPrefix: 'VirtualNetwork'
          destinationAddressPrefix: 'VirtualNetwork'
          access: 'Allow'
          priority: 103
          direction: 'Outbound'
        }
      }
      {
        name: 'Microsoft.Databricks-workspaces_UseOnly_databricks-worker-to-eventhub'
        properties: {
          description: 'Required for worker communication with Azure Eventhub services.'
          protocol: 'Tcp'
          sourcePortRange: '*'
          destinationPortRange: '9093'
          sourceAddressPrefix: 'VirtualNetwork'
          destinationAddressPrefix: 'EventHub'
          access: 'Allow'
          priority: 104
          direction: 'Outbound'
        }
      }
    ]
  }
}

// V-Net

resource vnet 'Microsoft.Network/virtualNetworks@2021-08-01' = if(endpointType == 'PrivateMode' || endpointType == 'HybridMode') {
  name: vnetName
  location: location
  properties: {
    addressSpace: {
      addressPrefixes: [
        vnetCidr
      ]
    }
    subnets: [
      {
        name: publicSubnetName
        properties: {
          addressPrefix: publicSubnetCidr
          networkSecurityGroup: {
            id: nsg.id
          }
          serviceEndpoints: [
            {
              service: 'Microsoft.Storage'
              locations: [resourceGroup().location]
            }
          ]
          delegations: [
            {
              name: 'databricks-del-public'
              properties: {
                serviceName: 'Microsoft.Databricks/workspaces'
              }
            }
          ]
        }
      }
      {
        name: privateSubnetName
        properties: {
          addressPrefix: privateSubnetCidr
          networkSecurityGroup: {
            id: nsg.id
          }
          delegations: [
            {
              name: 'databricks-del-private'
              properties: {
                serviceName: 'Microsoft.Databricks/workspaces'
              }
            }
          ]
        }
      }
      {
        name: PrivateEndpointSubnetName
        properties: {
          addressPrefix: privateEndpointSubnetCidr
          privateEndpointNetworkPolicies: 'Disabled'
        }
      }
    ]
  }
}

//Outputs

@description('Resource ID of the VNet')
output vnetResourceId string = vnet.id

output networkResourceOp string = 'Name: ${nsg.name} - Type: ${nsg.type} || Name: ${vnet.name} - Type: ${vnet.type}'


