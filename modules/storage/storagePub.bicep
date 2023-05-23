//Parameters

param blobAccountName string 

param containerName string 

//Variables

var location = resourceGroup().location

//Resources

resource blobAccountPublic 'Microsoft.Storage/storageAccounts@2021-04-01' = {
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
      virtualNetworkRules: []
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
    blobAccountPublic
  ]
}

//Outputs

output blobAccountResourceOp string = 'Name: ${blobAccountPublic.name} - Type: ${blobAccountPublic.type}'
