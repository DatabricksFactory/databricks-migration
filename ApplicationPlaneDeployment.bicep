@description('The URI of script file to upload blob container')
param fileuploaduri string = 'https://raw.githubusercontent.com/DatabricksFactory/databricks-migration/main/ApplicationPlaneDeployment.ps1'

@description('Name of identity')
param identityName string = 'PostDeploymentScriptuserAssignedName'

@description('Unique Suffix')
param uniqueSuffix string = substring(uniqueString(resourceGroup().id), 0, 6)

@description('firstuniquestring')
param firstuniquestring string = 'firstunique${uniqueSuffix}'

@description('seconduniquestring')
param seconduniquestring string = 'secondunique${uniqueSuffix}'

@description('Controls the execution of notebook deployment script')
param ctrlDeployNotebook bool

@description('Controls the execution of pipeline deployment script')
param ctrlDeployPipeline bool

@description('Time to live of the Databricks token in seconds')
param lifetimeSeconds int = 1200

@description('NAme of the pipeline')
param pipelineName string = 'Sample Pipeline'

@description('Path where DLT will be created')
param storagePath string = 'dbfs:/user/hive/warehouse'

@description('Target schema name')
param targetSchemaName string = 'Sample'

@description('Side note on the token generation')
param comment string = 'ARM deployment'

@description('Min workers')
param minWorkers int = 1

@description('Max workers')
param maxWorkers int = 5


@description('Path of the notebook to be uploaded')
param notebookPath string = 'https://raw.githubusercontent.com/DatabricksFactory/databricks-migration/main/Artifacts'

var fileuploadurivariable = fileuploaduri
var scriptParametersToUploadFile = '-RG_NAME ${resourceGroup().name} -REGION ${location} -LIFETIME_SECONDS ${lifetimeSeconds} -MINWORKERS ${minWorkers} -MAXWORKERS ${maxWorkers} -COMMENT ${comment} -PIPELINENAME ${pipelineName} -STORAGE ${storagePath} -TARGETSCHEMA ${targetSchemaName} -CTRL_DEPLOY_NOTEBOOK ${(ctrlDeployNotebook ? '$true' : '$false')} -CTRL_DEPLOY_PIPELINE ${(ctrlDeployPipeline ? '$true' : '$false')} -NOTEBOOK_PATH ${notebookPath}'
var contributorRoleDefinitionId = 'B24988ac-6180-42a0-ab88-20f7382dd24c'
var bootstrapRoleAssignmentId_var = guid(firstuniquestring, seconduniquestring)
var location = resourceGroup().location


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


