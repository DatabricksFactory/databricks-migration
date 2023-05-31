//Parameters

@description('The name of the Azure Databricks workspace to create.')
param workspaceName string 

@description('Controls the execution of cluster deployment script')
param ctrlDeployCluster bool 

@description('Controls the execution of notebook deployment script')
param ctrlDeployNotebook bool

@description('Controls the execution of pipeline deployment script')
param ctrlDeployPipeline bool

@description('Time to live of the Databricks token in seconds')
param lifetimeSeconds int 

@description('Side note on the token generation')
param comment string 

@description('Name of the Databricks cluster')
param clusterName string 

@description('Version of Spark in the cluster')
param sparkVersion string 

@description('Cluster terminates after specified minutes of inactivity')
param autoTerminationMinutes int 

@description('Number of worker nodes')
param numWorkers string 

@description('Type of worker node')
param nodeTypeId string 

@description('Type of driver node')
param driverNodeTypeId string 

@description('Max number of retries')
param retryLimit int 

@description('Interval between each retries in seconds')
param retryTime int 

@description('NAme of the pipeline')
param pipelineName string 

@description('Path where DLT will be created')
param storagePath string 

@description('Target schema name')
param targetSchemaName string 

@description('Min workers')
param minWorkers int 

@description('Max workers')
param maxWorkers int 

@description('Path of the notebook to be uploaded')
param notebookPath string 

@description('The URI of script file to upload blob container')
param fileuploaduri string 

@description('Name of identity')
param identityName string 

@description('firstuniquestring')
param firstuniquestring string 

@description('seconduniquestring')
param seconduniquestring string 

param endpointType string

param Ctrl_Syntax_Type string

param Ctrl_Import_Notebook string

param sa_name string

param saExists bool

//Variables

var location = resourceGroup().location

var fileuploadurivariable = fileuploaduri

var scriptParametersToUploadFile = '-RG_NAME ${resourceGroup().name} -REGION ${location} -WORKSPACE_NAME ${workspaceName} -LIFETIME_SECONDS ${lifetimeSeconds} -COMMENT ${comment} -CLUSTER_NAME ${clusterName} -SPARK_VERSION ${sparkVersion} -AUTOTERMINATION_MINUTES ${autoTerminationMinutes} -NUM_WORKERS ${numWorkers} -NODE_TYPE_ID ${nodeTypeId} -DRIVER_NODE_TYPE_ID ${driverNodeTypeId} -RETRY_LIMIT ${retryLimit} -RETRY_TIME ${retryTime} -CTRL_DEPLOY_CLUSTER ${(ctrlDeployCluster ? '$true' : '$false')} -MINWORKERS ${minWorkers} -MAXWORKERS ${maxWorkers} -PIPELINENAME ${pipelineName} -STORAGE ${storagePath} -TARGETSCHEMA ${targetSchemaName} -CTRL_DEPLOY_NOTEBOOK ${(ctrlDeployNotebook ? '$true' : '$false')} -CTRL_DEPLOY_PIPELINE ${(ctrlDeployPipeline ? '$true' : '$false')} -NOTEBOOK_PATH ${notebookPath} -SRC_FILESOURCE ${Ctrl_Import_Notebook == 'RawFileSource' ? '$true' : '$false'} -SRC_AZSQL ${Ctrl_Import_Notebook == 'AzureSQL' ? '$true' : '$false'} -SRC_AZMYSQL ${Ctrl_Import_Notebook == 'AzureMySQL' ? '$true' : '$false'} -SRC_AZPSQL ${Ctrl_Import_Notebook == 'AzurePostgreSQL' ? '$true' : '$false'} -SRC_SQL_ONPREM ${Ctrl_Import_Notebook == 'SQL_On_Prem' ? '$true' : '$false'} -SRC_PSQL_ONPREM ${Ctrl_Import_Notebook == 'PostgreSQL_On_Prem' ? '$true' : '$false'} -SRC_ORACLE ${Ctrl_Import_Notebook == 'Oracle' ? '$true' : '$false'} -SRC_EVENTHUB ${Ctrl_Import_Notebook == 'Eventhub' ? '$true' : '$false'} -CTRL_SYNTAX ${Ctrl_Syntax_Type} -SA_NAME ${sa_name} -SA_EXISTS ${saExists ? '$true' : '$false'}'

var bootstrapRoleAssignmentId_var = guid(firstuniquestring, seconduniquestring)

var contributorRoleDefinitionId = 'B24988ac-6180-42a0-ab88-20f7382dd24c'

//Resources

resource PostDeploymentScriptForFileUpload 'Microsoft.Resources/deploymentScripts@2020-10-01' = if(endpointType == 'PublicMode' || endpointType == 'HybridMode') {
  name: 'PostDeploymentScriptForFileUpload'
  location: location
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

resource identity 'Microsoft.ManagedIdentity/userAssignedIdentities@2018-11-30' = if(endpointType == 'PublicMode' || endpointType == 'HybridMode') {
  name: identityName
  location: location
}

resource bootstrapRoleAssignmentId 'Microsoft.Authorization/roleAssignments@2018-09-01-preview' = if(endpointType == 'PublicMode' || endpointType == 'HybridMode') {
  name: bootstrapRoleAssignmentId_var
  properties: {
    roleDefinitionId: resourceId('Microsoft.Authorization/roleDefinitions', contributorRoleDefinitionId)
    principalId: reference(identity.id, '2018-11-30').principalId
    scope: resourceGroup().id
    principalType: 'ServicePrincipal'
  }
}

//Outputs

output postDeploymentsResourceOp string = 'Name: ${PostDeploymentScriptForFileUpload.name} - Type: ${PostDeploymentScriptForFileUpload.type} || Name: ${identity.name} - Type: ${identity.type} || Name: ${bootstrapRoleAssignmentId.name} - Type: ${bootstrapRoleAssignmentId.type}'
