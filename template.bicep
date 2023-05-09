param containerName string = 'data'

@allowed([
  'Basic'
  'Standard'
])
param eventHubSku string = 'Standard'
param blobAccountName string = 'adls${uniqueString(resourceGroup().id)}'

@description('The URI of script file to upload blob container')
param fileuploaduri string = 'https://raw.githubusercontent.com/DatabricksFactory/databricks-migration/main/deployClusterNotebook.ps1?token=GHSAT0AAAAAACB4K7TQZ2T3M7HL6BCZJTHEZCZ55CQ'

@description('Name of identity')
param identityName string = 'PostDeploymentScriptuserAssignedName'

@description('Unique Suffix')
param uniqueSuffix string = substring(uniqueString(resourceGroup().id), 0, 6)

@description('firstuniquestring')
param firstuniquestring string = 'firstunique${uniqueSuffix}'

@description('seconduniquestring')
param seconduniquestring string = 'secondunique${uniqueSuffix}'
param utcValue string = utcNow()
param ctrlDeployStorageAccount bool = true
param ctrlDeployKeyVault bool = true
param ctrlDeployEventHub bool = true

@description('Controls the execution of cluster deployment script')
param ctrlDeployCluster bool

@description('Controls the execution of notebook deployment script')
param ctrlDeployNotebook bool

@description('Time to live of the Databricks token in seconds')
param lifetimeSeconds int = 1200

@description('Side note on the token generation')
param comment string = 'ARM deployment'

@description('Name of the Databricks cluster')
param clusterName string = 'dbcluster'

@description('Version of Spark in the cluster')
param sparkVersion string = '11.3.x-scala2.12'

@description('Cluster terminates after specified minutes of inactivity')
param autoTerminationMinutes int = 30

@description('Number of worker nodes')
param numWorkers string = '2'

@description('Type of worker node')
param nodeTypeId string = 'Standard_DS3_v2'

@description('Type of driver node')
param driverNodeTypeId string = 'Standard_DS3_v2'

@description('Max number of retries')
param retryLimit int = 15

@description('Interval between each retries in seconds')
param retryTime int = 60

@description('Path of the notebook to be uploaded')
param notebookPath string = 'https://raw.githubusercontent.com/ksameer18/azure-synapse-labs/main/environments/env1/Sample/Artifacts/Notebooks/01-UsingOpenDatasetsSynapse.ipynb'

var fileuploadurivariable = fileuploaduri
var databricksName = 'databricks_${randomString}'
var scriptParametersToUploadFile = '-RG_NAME ${resourceGroup().name} -REGION ${location} -WORKSPACE_NAME ${databricksName} -LIFETIME_SECONDS ${lifetimeSeconds} -COMMENT ${comment} -CLUSTER_NAME ${clusterName} -SPARK_VERSION ${sparkVersion} -AUTOTERMINATION_MINUTES ${autoTerminationMinutes} -NUM_WORKERS ${numWorkers} -NODE_TYPE_ID ${nodeTypeId} -DRIVER_NODE_TYPE_ID ${driverNodeTypeId} -RETRY_LIMIT ${retryLimit} -RETRY_TIME ${retryTime} -CTRL_DEPLOY_CLUSTER ${(ctrlDeployCluster ? '$true' : '$false')} -CTRL_DEPLOY_NOTEBOOK ${(ctrlDeployNotebook ? '$true' : '$false')} -NOTEBOOK_PATH ${notebookPath}'
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

resource blobAccount 'Microsoft.Storage/storageAccounts@2021-04-01' = if (ctrlDeployStorageAccount) {
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

resource blobAccountName_default_container 'Microsoft.Storage/storageAccounts/blobServices/containers@2021-06-01' = if (ctrlDeployStorageAccount) {
  name: '${blobAccountName}/default/${containerName}'
  dependsOn: [
    blobAccount
  ]
}

resource eventHubNamespace 'Microsoft.EventHub/namespaces@2021-11-01' = if (ctrlDeployEventHub) {
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

resource vault_utcValue 'Microsoft.KeyVault/vaults@2021-04-01-preview' = if (ctrlDeployKeyVault) {
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