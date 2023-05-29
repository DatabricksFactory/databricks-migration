//Parameters

@allowed([
  'PublicMode'
  'PrivateMode'
  'HybridMode'
])
param endpointType string = 'HybridMode'

@allowed([
  'Basic'
  'Standard'
])
param eventHubSku string = 'Standard'

param blobAccountName string = 'adls${uniqueString(resourceGroup().id)}'

param containerName string = 'data'

@description('')
param eHRuleName string = 'rule'

@description('The URI of script file to upload blob container')
param fileuploaduri string = 'https://raw.githubusercontent.com/DatabricksFactory/databricks-migration/main/OneClickDeploy.ps1'

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

@description('Controls the execution of notebook deployment script')
param ctrlDeployNotebook bool = true

@description('Controls the execution of pipeline deployment script')
param ctrlDeployPipeline bool = true

@description('Controls the execution of cluster deployment script')
param ctrlDeployCluster bool = true

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

@description('NAme of the pipeline')
param pipelineName string = 'Sample Pipeline'

@description('Path where DLT will be created')
param storagePath string = 'dbfs:/user/hive/warehouse'

@description('Target schema name')
param targetSchemaName string = 'Sample'

@description('Min workers')
param minWorkers int = 1

@description('Max workers')
param maxWorkers int = 5

@description('Path of the notebook to be uploaded')
param notebookPath string = 'https://raw.githubusercontent.com/DatabricksFactory/databricks-migration/main/Artifacts'

@description('Specifies whether to deploy Azure Databricks workspace with secure cluster connectivity (SCC) enabled or not (No Public IP).')
param disablePublicIp bool = true

@description('The name of the network security group to create.')
param nsgName string = 'databricks-nsg'

@description('The pricing tier of workspace.')
@allowed([
  'trial'
  'standard'
  'premium'
])
param pricingTier string = 'premium'

@description('CIDR range for the private subnet.')
param privateSubnetCidr string = '10.179.0.0/18'

@description('The name of the private subnet to create.')
param privateSubnetName string = 'private-subnet'

@description('CIDR range for the public subnet.')
param publicSubnetCidr string = '10.179.64.0/18'

@description('CIDR range for the private endpoint subnet..')
param privateEndpointSubnetCidr string = '10.179.128.0/24'

@description('The name of the public subnet to create.')
param publicSubnetName string = 'public-subnet'

@description('Indicates whether to retain or remove the AzureDatabricks outbound NSG rule - possible values are AllRules or NoAzureDatabricksRules.')
@allowed([
  'AllRules'
  'NoAzureDatabricksRules'
])
param requiredNsgRules string = 'NoAzureDatabricksRules'

@description('CIDR range for the vnet.')
param vnetCidr string = '10.179.0.0/16'

@description('The name of the virtual network to create.')
param vnetName string = 'databricks-vnet'

@description('The name of the subnet to create the private endpoint in.')
param PrivateEndpointSubnetName string = 'default'

@description('The name of the Azure Databricks workspace to create.')
param workspaceName string = 'default'

@allowed([
  'DeltaLiveTable'
  'DeltaTable'
])
param Ctrl_Syntax_Type string = 'DeltaLiveTable'

@allowed([
        'RawFileSource'
        'AzureSQL'
        'AzureMySQL'
        'AzurePostgreSQL'
        'SQL_On_Prem'
        'PostgreSQL_On_Prem'
        'Oracle'
        'Eventhub'
])
param Ctrl_Import_Notebook string = 'RawFileSource'

// Resources

module networkModule 'modules/network/network.bicep' = {
  name: 'Network_Deployment'
  params: {
    PrivateEndpointSubnetName: PrivateEndpointSubnetName
    nsgName: nsgName
    privateEndpointSubnetCidr: privateEndpointSubnetCidr
    privateSubnetCidr: privateSubnetCidr
    privateSubnetName: privateSubnetName
    publicSubnetCidr: publicSubnetCidr
    publicSubnetName: publicSubnetName
    vnetCidr: vnetCidr
    vnetName: vnetName
    endpointType: endpointType
  }
}

module databricksPublicModule 'modules/databricks/databricksPub.bicep' = if(endpointType == 'PublicMode') {
  name: 'Databricks_Public_Deployment'
  params: {
    pricingTier: pricingTier
    workspaceName: workspaceName
  }
}

module databricksPrivateModule 'modules/databricks/databricksPvt.bicep' = if(endpointType == 'PrivateMode') {
  name: 'Databricks_Private_Deployment'
  params: {
    customVirtualNetworkResourceId: networkModule.outputs.vnetResourceId
    disablePublicIp: disablePublicIp
    pricingTier: pricingTier
    privateSubnetName: privateSubnetName
    publicSubnetName: publicSubnetName
    requiredNsgRules: requiredNsgRules
    workspaceName: workspaceName
    PrivateEndpointSubnetName: PrivateEndpointSubnetName
    vnetName: vnetName
    vnetResourceId: networkModule.outputs.vnetResourceId
  }
}

module databricksPrivateHybridModule 'modules/databricks/databricksPvtHyb.bicep' = if(endpointType == 'HybridMode') {
  name: 'Databricks_Private_Hybrid_Deployment'
  params: {
    customVirtualNetworkResourceId: networkModule.outputs.vnetResourceId
    disablePublicIp: disablePublicIp
    pricingTier: pricingTier
    privateSubnetName: privateSubnetName
    publicSubnetName: publicSubnetName
    requiredNsgRules: requiredNsgRules
    workspaceName: workspaceName
    PrivateEndpointSubnetName: PrivateEndpointSubnetName
    vnetName: vnetName
    vnetResourceId: networkModule.outputs.vnetResourceId
  }
}

module storagePublicModule 'modules/storage/storagePub.bicep' = if(ctrlDeployStorageAccount && endpointType == 'PublicMode') {
  name: 'Storage_Account_Public_Deployment'
  params: {
    blobAccountName: blobAccountName
    containerName: containerName
  }
}

module storagePrivateModule 'modules/storage/storagePvt.bicep' = if(ctrlDeployStorageAccount && endpointType == 'PrivateMode') {
  name: 'Storage_Account_Private_Deployment'
  params: {
    blobAccountName: blobAccountName
    containerName: containerName
    publicSubnetName: publicSubnetName
    vnetName: vnetName
    PrivateEndpointSubnetName: PrivateEndpointSubnetName
    vnetResourceId: networkModule.outputs.vnetResourceId
  }
  dependsOn: [networkModule]
}

module storagePrivateHybridModule 'modules/storage/storagePvtHyb.bicep' = if(ctrlDeployStorageAccount && endpointType == 'HybridMode') {
  name: 'Storage_Account_Private_Hybrid_Deployment'
  params: {
    blobAccountName: blobAccountName
    containerName: containerName
    publicSubnetName: publicSubnetName
    vnetName: vnetName
    PrivateEndpointSubnetName: PrivateEndpointSubnetName
    vnetResourceId: networkModule.outputs.vnetResourceId
  }
  dependsOn: [networkModule]
}

module eventhubModule './modules/eventhub/eventhub.bicep' = if(ctrlDeployEventHub) {
  name: 'EventHub_Deployment'
  params: {
    eHRuleName: eHRuleName
    eventHubSku: eventHubSku
  }
}

module keyvaultModule './modules/keyvault/keyvault.bicep' = if(ctrlDeployKeyVault) {
  name: 'Key_Vault_Deployment'
  params: {
    utcValue: utcValue
  }
}

module eventhubPrivateEndpointModule './modules/resourcepep/eventhubpep.bicep' = if(endpointType == 'PrivateMode' || endpointType == 'HybridMode') {
  name: 'EventHub_Private_Endpoint_Deployment'
  params: {
    PrivateEndpointSubnetName: PrivateEndpointSubnetName
    eventhubResourceId: eventhubModule.outputs.eventhubResourceId
    vnetName: vnetName
    vnetResourceId: networkModule.outputs.vnetResourceId
  }
  dependsOn: [eventhubModule]
}

module keyvaultPrivateEndpointModule './modules/resourcepep/keyvaultpep.bicep' = if(endpointType == 'PrivateMode' || endpointType == 'HybridMode') {
  name: 'Key_Vault_Private_Endpoint_Deployment'
  params: {
    PrivateEndpointSubnetName: PrivateEndpointSubnetName
    keyvaultResourceId: keyvaultModule.outputs.keyvaultResourceId
    vnetName: vnetName
    vnetResourceId: networkModule.outputs.vnetResourceId
  }
  dependsOn: [keyvaultModule]
}

module deploymentScriptPublicModule './modules/deploymentScripts/deploymentScripts.bicep' = if(endpointType == 'PublicMode') { 
  name: 'Post-Deployment_Scripts_Public'
  params: {
    autoTerminationMinutes: autoTerminationMinutes
    clusterName: clusterName
    comment: comment
    ctrlDeployCluster: ctrlDeployCluster
    ctrlDeployNotebook: ctrlDeployNotebook
    ctrlDeployPipeline: ctrlDeployPipeline
    driverNodeTypeId: driverNodeTypeId
    fileuploaduri: fileuploaduri
    firstuniquestring: firstuniquestring
    identityName: identityName
    lifetimeSeconds: lifetimeSeconds
    nodeTypeId: nodeTypeId
    numWorkers: numWorkers
    retryLimit: retryLimit
    retryTime: retryTime
    seconduniquestring: seconduniquestring
    sparkVersion: sparkVersion
    workspaceName: workspaceName
    pipelineName: pipelineName
    notebookPath: notebookPath
    storagePath: storagePath
    targetSchemaName: targetSchemaName
    minWorkers: minWorkers
    maxWorkers: maxWorkers
    endpointType: endpointType
    Ctrl_Syntax_Type: Ctrl_Syntax_Type
    Ctrl_Import_Notebook: Ctrl_Import_Notebook
  }
  dependsOn: [databricksPublicModule]
}

module deploymentScriptPrivateHybridModule './modules/deploymentScripts/deploymentScripts.bicep' = if(endpointType == 'HybridMode') { 
  name: 'Post-Deployment_Scripts_Private_Hybrid'
  params: {
    autoTerminationMinutes: autoTerminationMinutes
    clusterName: clusterName
    comment: comment
    ctrlDeployCluster: ctrlDeployCluster
    ctrlDeployNotebook: ctrlDeployNotebook
    ctrlDeployPipeline: ctrlDeployPipeline
    driverNodeTypeId: driverNodeTypeId
    fileuploaduri: fileuploaduri
    firstuniquestring: firstuniquestring
    identityName: identityName
    lifetimeSeconds: lifetimeSeconds
    nodeTypeId: nodeTypeId
    numWorkers: numWorkers
    retryLimit: retryLimit
    retryTime: retryTime
    seconduniquestring: seconduniquestring
    sparkVersion: sparkVersion
    workspaceName: workspaceName
    pipelineName: pipelineName
    notebookPath: notebookPath
    storagePath: storagePath
    targetSchemaName: targetSchemaName
    minWorkers: minWorkers
    maxWorkers: maxWorkers
    endpointType: endpointType
    Ctrl_Syntax_Type: Ctrl_Syntax_Type
    Ctrl_Import_Notebook: Ctrl_Import_Notebook
  }
  dependsOn: [databricksPrivateHybridModule]
}

//Outputs

output resourceList array = [
  endpointType == 'PublicMode' ? databricksPublicModule.outputs.databricksPublicResourceOp : (endpointType == 'PrivateMode' ? databricksPrivateModule.outputs.databricksPvtResourceOp : databricksPrivateHybridModule.outputs.databricksPvtHybResourceOp)
  endpointType == 'PublicMode' ? storagePublicModule.outputs.blobAccountResourceOp : (endpointType == 'PrivateMode' ? storagePrivateModule.outputs.blobAccountResourceOp : storagePrivateHybridModule.outputs.blobAccountResourceOp)
  eventhubModule.outputs.eventHubResourceOp
  endpointType != 'PublicMode' ? eventhubPrivateEndpointModule.outputs.eventhubpepResourceOp : ''
  keyvaultModule.outputs.keyvaultResourceOp
  endpointType != 'PublicMode' ? keyvaultPrivateEndpointModule.outputs.keyvaultpepResourceOp : ''
  endpointType == 'PublicMode' ? deploymentScriptPublicModule.outputs.postDeploymentsResourceOp : (endpointType == 'HybridMode' ? deploymentScriptPrivateHybridModule.outputs.postDeploymentsResourceOp : '')
  endpointType != 'PublicMode' ? networkModule.outputs.networkResourceOp : ''
]
