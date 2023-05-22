//Parameters

param utcValue string 

//Variables

var location = resourceGroup().location

//Resources

resource vault_utcValue 'Microsoft.KeyVault/vaults@2021-04-01-preview' = {
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
