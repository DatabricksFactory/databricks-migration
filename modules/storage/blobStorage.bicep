// Parameters

param blobStorageName string

param blobContainerName string

//Variables

var location = resourceGroup().location

// Resources

resource blobStorageAccount 'Microsoft.Storage/storageAccounts@2022-05-01' = {
  name: blobStorageName
  location: location
  kind: 'StorageV2'
  sku: {
    name: 'Standard_LRS'
  }
  properties: {
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

resource blobStorageName_default_container 'Microsoft.Storage/storageAccounts/blobServices/containers@2021-06-01' = {
  name: '${blobStorageName}/default/${blobContainerName}'
  dependsOn: [
    blobStorageAccount
  ]
}

// Outputs

output blobStorageResourceOp string = 'Name: ${blobStorageAccount.name} - Type: ${blobStorageAccount.type}'
