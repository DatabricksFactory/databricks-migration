//Parameters

param blobAccountName string 

@description('The name of the virtual network to create.')
param vnetName string 

@description('The name of the public subnet to create.')
param publicSubnetName string 

param containerName string 

//Variables

var location = resourceGroup().location

//Resources

resource blobAccount 'Microsoft.Storage/storageAccounts@2021-04-01' = {
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
  // dependsOn: [
  //   vnet
  // ]
}

resource blobAccountName_default_container 'Microsoft.Storage/storageAccounts/blobServices/containers@2021-06-01' = {
  name: '${blobAccountName}/default/${containerName}'
  dependsOn: [
    blobAccount
  ]
}

//Outputs

output blobAccountResourceId string = blobAccount.id
