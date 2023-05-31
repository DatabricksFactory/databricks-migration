//Parameters

param utcValue string 

param ctrlDeployKeyVault bool

//Variables

var location = resourceGroup().location

//Resources

resource vault_utcValue 'Microsoft.KeyVault/vaults@2021-04-01-preview' = if(ctrlDeployKeyVault) {
  name: 'vault${utcValue}'
  location: location
  properties: {
    sku: {
      family: 'A'
      name: 'standard'
    }
    softDeleteRetentionInDays: 7
    networkAcls: {
      defaultAction: 'Deny'
      bypass: 'AzureServices'
      ipRules: []
      virtualNetworkRules: []
    }
    tenantId: subscription().tenantId
    accessPolicies: [
    ]
  }
}

//Outputs

output keyvaultResourceId string = vault_utcValue.id

output keyvaultResourceOp string = 'Name: ${vault_utcValue.name} - Type: ${vault_utcValue.type}'
