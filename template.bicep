param containerName string = 'data'

@allowed([
  'Basic'
  'Standard'
])
param eventHubSku string = 'Standard'
param blobAccountName string = 'adls${uniqueString(resourceGroup().id)}'

@description('The URI of script file to upload blob container')
param fileuploaduri string = 'https://raw.githubusercontent.com/DatabricksFactory/databricks-migration/main/clusterDeploy.ps1'

@description('Name of identity')
param identityName string = 'PostDeploymentScriptuserAssignedName'

@description('Unique Suffix')
param uniqueSuffix string = substring(uniqueString(resourceGroup().id), 0, 6)

@description('firstuniquestring')
param firstuniquestring string = 'firstunique${uniqueSuffix}'

@description('seconduniquestring')
param seconduniquestring string = 'secondunique${uniqueSuffix}'
param utcValue string = utcNow()
param ctrlStorageAccount bool = true
param ctrlKeyVault bool = true
param ctrlEventHub bool = true

var fileuploadurivariable = fileuploaduri
var databricksName = 'databricks_${randomString}'
var scriptParametersToUploadFile = '-RG_NAME ${resourceGroup().name} -REGION ${location} -WORKSPACE_NAME ${databricksName}'
var contributorRoleDefinitionId = 'B24988ac-6180-42a0-ab88-20f7382dd24c'
var bootstrapRoleAssignmentId_var = guid(firstuniquestring, seconduniquestring)
var randomString = substring(guid(resourceGroup().id), 0, 6)
var location = resourceGroup().location
var managedResourceGroupName = 'db-rg-${databricksName}'
var managedResourceGroupId = '${subscription().id}/resourceGroups/${managedResourceGroupName}'
var eventHubNamespaceName = 'streamdata-${randomString}-ns'

resource databricks 'Microsoft.Databricks/workspaces@2018-04-01' = {
  location: location
  name: databricksName
  sku: {
    name: 'premium'
  }
  properties: {
    managedResourceGroupId: managedResourceGroupId
  }
}

resource PostDeploymentScriptForFileUpload 'Microsoft.Resources/deploymentScripts@2020-10-01' = {
  name: 'PostDeploymentScriptForFileUpload'
  location: resourceGroup().location
  kind: 'AzurePowerShell'
  identity: {
    type: 'UserAssigned'
    userAssignedIdentities: {
      '${identity.id}': {}
    }
  }
  properties: {
    azPowerShellVersion: '7.2.4'
    cleanupPreference: 'OnSuccess'
    retentionInterval: 'P1D'
    timeout: 'PT30M'
    arguments: scriptParametersToUploadFile
    primaryScriptUri: fileuploadurivariable
  }
  dependsOn: [

    databricks
  ]
}

resource identity 'Microsoft.ManagedIdentity/userAssignedIdentities@2018-11-30' = {
  name: identityName
  location: resourceGroup().location
}

resource bootstrapRoleAssignmentId 'Microsoft.Authorization/roleAssignments@2018-09-01-preview' = {
  name: bootstrapRoleAssignmentId_var
  properties: {
    roleDefinitionId: resourceId('Microsoft.Authorization/roleDefinitions', contributorRoleDefinitionId)
    principalId: reference(identity.id, '2018-11-30').principalId
    scope: resourceGroup().id
    principalType: 'ServicePrincipal'
  }
}

resource blobAccount 'Microsoft.Storage/storageAccounts@2021-04-01' = if (ctrlStorageAccount) {
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

resource blobAccountName_default_container 'Microsoft.Storage/storageAccounts/blobServices/containers@2021-06-01' = if (ctrlStorageAccount) {
  name: '${blobAccountName}/default/${containerName}'
  dependsOn: [
    blobAccount
  ]
}

resource eventHubNamespace 'Microsoft.EventHub/namespaces@2021-11-01' = if (ctrlEventHub) {
  name: eventHubNamespaceName
  location: location
  sku: {
    name: eventHubSku
    tier: eventHubSku
    capacity: 1
  }
  properties: {
    isAutoInflateEnabled: false
    maximumThroughputUnits: 0
  }
}

resource vault_utcValue 'Microsoft.KeyVault/vaults@2021-04-01-preview' = if (ctrlKeyVault) {
  name: 'vault${utcValue}'
  location: location
  properties: {
    sku: {
      family: 'A'
      name: 'standard'
    }
    tenantId: subscription().tenantId
    accessPolicies: []
  }
}