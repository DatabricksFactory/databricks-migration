//Parameters

@description('Mode of deployment')
@allowed([
  'PublicMode'
  'PrivateMode'
  'HybridMode'
])
param endpointType string = 'HybridMode'

// Databricks params
@description('The pricing tier of the Azure Databricks workspace.')
@allowed([
  'trial'
  'standard'
  'premium'
])
param pricingTier string = 'premium'

@description('The name of the Azure Databricks workspace to create (Note: Valid characters are alphanumerics, underscores, and hyphens and length between 3 and 51)')
@minLength(3)
@maxLength(51)
param workspaceName string = 'default'

// EventHub params
@description('Controls the deployment of EventHub')
param ctrlDeployEventHub bool = true

@description('EventHub SKU')
@allowed([
  'Basic'
  'Standard'
])
param eventHubSku string = 'Standard'

@description('EventHub Rule name')
param eHRuleName string = 'rule'

// SA params
@description('Storage Account name. (Note: Valid characters are lowercase letters and numbers and length between 3 and 11)')
@minLength(3)
@maxLength(11)
param userBlobAccountName string = 'adls'

// KeyVault params
@description('Controls the deployment of KeyVault')
param ctrlDeployKeyVault bool = true

@description('UTC datetime (Note: Default value should not be altered)')
param utcValue string = utcNow() 

// Script params

@description('Controls the deployment of Blob Storage and Example Notebooks')
param ctrlDeploySample bool = true

@description('Controls the execution of pipeline deployment script')
param ctrlDeployPipeline bool = true

// Pipeline params
@description('NAme of the pipeline')
param pipelineName string = 'Sample Pipeline'

@description('Either DeltaLiveTable or DeltaTable Notebooks will be imported')
@allowed([
  'DeltaLiveTable'
  'DeltaTable'
])
param ctrlSyntaxType string = 'DeltaLiveTable'

@description('Data source')
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
param ctrlSourceNotebook string = 'RawFileSource'

// Cluster params
@description('Controls the execution of cluster deployment script')
param ctrlDeployCluster bool = true

@description('Name of the Databricks cluster')
param clusterName string = 'dbcluster'

@description('LTS version of Spark in the cluster')
@allowed([
  '12.2.x-scala2.12'
  '11.3.x-scala2.12'
  '10.4.x-scala2.12'
])
param sparkVersion string = '11.3.x-scala2.12'

@description('Cluster terminates after specified minutes of inactivity. Threshold must be between 10 and 10000 minutes. Users can also set this value to 0 to explicitly disable automatic termination.')
param autoTerminationMinutes int = 30

@description('Number of worker nodes')
param numWorkers string = '2'

@description('Type of worker node')
param nodeTypeId string = 'Standard_DS3_v2'

@description('Type of driver node')
param driverNodeTypeId string = 'Standard_DS3_v2'

// Network params
@description('The name of the network security group to create.')
param nsgName string = 'databricks-nsg'

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

// Variables 

@description('Reference branch in Git')
var refBranch = 'main'

@description('Controls the deployment of SA')
var ctrlDeployStorageAccount = true

@description('Controls the execution of notebook deployment script')
var ctrlDeployNotebook = true

@description('Creating a unique SA name')
var blobAccountName = '${userBlobAccountName}${uniqueString(resourceGroup().id)}'

@description('Container name')
var containerName = 'data' 

@description('Name for the Blob Storage')
var blobStorageName = 'samplesblob${uniqueString(resourceGroup().id)}'

@description('Name for the blob container')
var blobContainerName = 'data'

@description('URI of the automation script')
var fileuploaduri = 'https://raw.githubusercontent.com/DatabricksFactory/databricks-migration/${refBranch}/OneClickDeploy.ps1'

@description('Relative path of the notebooks to be uploaded')
var notebookPath = 'https://raw.githubusercontent.com/DatabricksFactory/databricks-migration/${refBranch}/Artifacts'

@description('Name of identity')
var identityName = 'PostDeploymentScriptuserAssignedName' 

@description('Unique Suffix')
var uniqueSuffix = substring(uniqueString(resourceGroup().id), 0, 6) 

@description('firstuniquestring')
var firstuniquestring = 'firstunique${uniqueSuffix}' 

@description('seconduniquestring')
var seconduniquestring = 'secondunique${uniqueSuffix}' 

@description('Side note on the token generation')
var comment = 'ARM deployment' 

@description('Max number of retries to check if the cluster is running')
var retryLimit = 15

@description('Interval between each retries in seconds')
var retryTime = 60

@description('Time to live of the Databricks token in seconds')
var lifetimeSeconds = 1200

@description('Path where DLT will be created')
var storagePath = 'dbfs:/user/hive/warehouse' 

@description('Target schema name')
var targetSchemaName = 'Sample' 

@description('Min workers')
var minWorkers = 1 

@description('Max workers')
var maxWorkers = 5 

@description('Specifies whether to deploy Azure Databricks workspace with secure cluster connectivity (SCC) enabled or not (No Public IP).')
var disablePublicIp = true 

@description('Subscription ID')
var subscriptionId = subscription().subscriptionId

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

module databricksHybridModule 'modules/databricks/databricksPvtHyb.bicep' = if(endpointType == 'HybridMode') {
  name: 'Databricks_Hybrid_Deployment'
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

module storagePublicModule 'modules/storage/storagePub.bicep' = if(endpointType == 'PublicMode') {
  name: 'Storage_Account_Public_Deployment'
  params: {
    blobAccountName: blobAccountName
    containerName: containerName
    ctrlDeployStorageAccount: ctrlDeployStorageAccount 
  }
}

module storagePrivateModule 'modules/storage/storagePvt.bicep' = if(endpointType == 'PrivateMode') {
  name: 'Storage_Account_Private_Deployment'
  params: {
    blobAccountName: blobAccountName
    containerName: containerName
    publicSubnetName: publicSubnetName
    vnetName: vnetName
    PrivateEndpointSubnetName: PrivateEndpointSubnetName
    vnetResourceId: networkModule.outputs.vnetResourceId
    ctrlDeployStorageAccount: ctrlDeployStorageAccount
  }
  dependsOn: [networkModule]
}

module storageHybridModule 'modules/storage/storagePvtHyb.bicep' = if(endpointType == 'HybridMode') {
  name: 'Storage_Account_Hybrid_Deployment'
  params: {
    blobAccountName: blobAccountName
    containerName: containerName
    publicSubnetName: publicSubnetName
    vnetName: vnetName
    PrivateEndpointSubnetName: PrivateEndpointSubnetName
    vnetResourceId: networkModule.outputs.vnetResourceId
    ctrlDeployStorageAccount: ctrlDeployStorageAccount
  }
  dependsOn: [networkModule]
}

module blobStorageModule 'modules/storage/blobStorage.bicep' = if(ctrlDeploySample) {
  name: 'Blob_Storage_Deployment'
  params: {
    blobStorageName: blobStorageName
    blobContainerName: blobContainerName
  }
}

module eventhubModule './modules/eventhub/eventhub.bicep' = {
  name: 'EventHub_Deployment'
  params: {
    eHRuleName: eHRuleName
    eventHubSku: eventHubSku
    ctrlDeployEventHub: ctrlDeployEventHub
  }
}

module keyvaultModule './modules/keyvault/keyvault.bicep' = {
  name: 'KeyVault_Deployment'
  params: {
    utcValue: utcValue
    ctrlDeployKeyVault: ctrlDeployKeyVault
  }
}

module eventhubPrivateEndpointModule './modules/resourcepep/eventhubpep.bicep' = if((endpointType == 'PrivateMode' || endpointType == 'HybridMode')) {
  name: 'EventHub_Private_Endpoint_Deployment'
  params: {
    PrivateEndpointSubnetName: PrivateEndpointSubnetName
    eventhubResourceId: eventhubModule.outputs.eventhubResourceId
    vnetName: vnetName
    vnetResourceId: networkModule.outputs.vnetResourceId
    ctrlDeployEventHub: ctrlDeployEventHub
  }
  dependsOn: [eventhubModule]
}

module keyvaultPrivateEndpointModule './modules/resourcepep/keyvaultpep.bicep' = if((endpointType == 'PrivateMode' || endpointType == 'HybridMode')) {
  name: 'KeyVault_Private_Endpoint_Deployment'
  params: {
    PrivateEndpointSubnetName: PrivateEndpointSubnetName
    keyvaultResourceId: keyvaultModule.outputs.keyvaultResourceId
    vnetName: vnetName
    vnetResourceId: networkModule.outputs.vnetResourceId
    ctrlDeployKeyVault: ctrlDeployKeyVault
  }
  dependsOn: [keyvaultModule]
}

module deploymentScriptPublicModule './modules/deploymentScripts/deploymentScripts.bicep' = if(endpointType == 'PublicMode') { 
  name: 'Post-Deployment_Scripts_Public_OneClickDeploy'
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
    ctrlSyntaxType: ctrlSyntaxType
    ctrlSourceNotebook: ctrlSourceNotebook
    sa_name: blobAccountName
    saExists: ctrlDeployStorageAccount
    subscriptionId: subscriptionId
    ctrlDeploySample: ctrlDeploySample
  }
  dependsOn: [databricksPublicModule]
}

module deploymentScriptHybridModule './modules/deploymentScripts/deploymentScripts.bicep' = if(endpointType == 'HybridMode') { 
  name: 'Post-Deployment_Scripts_Hybrid_OneClickDeploy'
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
    ctrlSyntaxType: ctrlSyntaxType
    ctrlSourceNotebook: ctrlSourceNotebook
    sa_name: blobAccountName
    saExists: ctrlDeployStorageAccount
    subscriptionId: subscriptionId
    ctrlDeploySample: ctrlDeploySample
  }
  dependsOn: [databricksHybridModule]
}

//Outputs

output resourceList array = [
  endpointType == 'PublicMode' ? databricksPublicModule.outputs.databricksPublicResourceOp : (endpointType == 'PrivateMode' ? databricksPrivateModule.outputs.databricksPvtResourceOp : databricksHybridModule.outputs.databricksPvtHybResourceOp)
  endpointType == 'PublicMode' ? storagePublicModule.outputs.blobAccountResourceOp : (endpointType == 'PrivateMode' ? storagePrivateModule.outputs.blobAccountResourceOp : storageHybridModule.outputs.blobAccountResourceOp)
  eventhubModule.outputs.eventHubResourceOp
  endpointType != 'PublicMode' ? eventhubPrivateEndpointModule.outputs.eventhubpepResourceOp : ''
  keyvaultModule.outputs.keyvaultResourceOp
  endpointType != 'PublicMode' ? keyvaultPrivateEndpointModule.outputs.keyvaultpepResourceOp : ''
  endpointType == 'PublicMode' ? deploymentScriptPublicModule.outputs.postDeploymentsResourceOp : (endpointType == 'HybridMode' ? deploymentScriptHybridModule.outputs.postDeploymentsResourceOp : '')
  endpointType != 'PublicMode' ? networkModule.outputs.networkResourceOp : ''
  ctrlDeploySample ? blobStorageModule.outputs.blobStorageResourceOp : ''
]
