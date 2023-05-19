@allowed([
  'Basic'
  'Standard'
])
param eventHubSku string = 'Standard'

param blobAccountName string = 'adls${uniqueString(resourceGroup().id)}'

param containerName string = 'data'

@description('')
param eHRuleName string = 'rule'

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

@description('Indicates whether public network access is allowed to the workspace with private endpoint - possible values are Enabled or Disabled.')
@allowed([
  'Enabled'
  'Disabled'
])
param publicNetworkAccess string = 'Enabled'

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

@description('The URI of script file to upload blob container')
param fileuploaduri string = 'https://raw.githubusercontent.com/DatabricksFactory/databricks-migration/main/allin1.ps1'

@description('Controls the execution of notebook deployment script')
param ctrlDeployNotebook bool

@description('Controls the execution of pipeline deployment script')
param ctrlDeployPipeline bool

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
 
var fileuploadurivariable = fileuploaduri
var scriptParametersToUploadFile = '-RG_NAME ${resourceGroup().name} -REGION ${location} -WORKSPACE_NAME ${workspaceName} -LIFETIME_SECONDS ${lifetimeSeconds} -COMMENT ${comment} -CLUSTER_NAME ${clusterName} -SPARK_VERSION ${sparkVersion} -AUTOTERMINATION_MINUTES ${autoTerminationMinutes} -NUM_WORKERS ${numWorkers} -NODE_TYPE_ID ${nodeTypeId} -DRIVER_NODE_TYPE_ID ${driverNodeTypeId} -RETRY_LIMIT ${retryLimit} -RETRY_TIME ${retryTime} -CTRL_DEPLOY_CLUSTER ${(ctrlDeployCluster ? '$true' : '$false')} -MINWORKERS ${minWorkers} -MAXWORKERS ${maxWorkers} -PIPELINENAME ${pipelineName} -STORAGE ${storagePath} -TARGETSCHEMA ${targetSchemaName} -CTRL_DEPLOY_NOTEBOOK ${(ctrlDeployNotebook ? '$true' : '$false')} -CTRL_DEPLOY_PIPELINE ${(ctrlDeployPipeline ? '$true' : '$false')} -NOTEBOOK_PATH ${notebookPath}'
var contributorRoleDefinitionId = 'B24988ac-6180-42a0-ab88-20f7382dd24c'
var bootstrapRoleAssignmentId_var = guid(firstuniquestring, seconduniquestring)
var location = resourceGroup().location

var randomString = substring(guid(resourceGroup().id), 0, 6)
var eventHubNamespaceName = 'streamdata-${randomString}-ns'
var eventHubName = 'streamdata-${randomString}-ns'
var managedResourceGroupName = 'databricks-rg-${workspaceName}-${uniqueString(workspaceName, resourceGroup().id)}'
var trimmedMRGName = substring(managedResourceGroupName, 0, min(length(managedResourceGroupName), 90))
var managedResourceGroupId = '${subscription().id}/resourceGroups/${trimmedMRGName}'

var privateEndpointName = '${workspaceName}-pvtEndpoint'
var privateDnsZoneName = 'privatelink.azuredatabricks.net'
var pvtEndpointDnsGroupName = '${privateEndpointName}/mydnsgroupname'

var privateEndpointNamestorage = 'storage-pvtEndpoint'
var privateDnsZoneNamestorage = 'privatelink.dfs.${environment().suffixes.storage}'
var pvtEndpointDnsGroupNamestorage = 'storage-pvtEndpoint/mydnsgroupname'

var privateEndpointNamevault = 'keyvault-pvtEndpoint'
var privateDnsZoneNamevault = 'privatelink.vaultcore.azure.net'
var pvtEndpointDnsGroupNamevault = 'keyvault-pvtEndpoint/mydnsgroupname'

var privateEndpointNameeventhub = 'eventhub-pvtEndpoint'
var privateDnsZoneNameeventhub = 'privatelink.servicebus.windows.net'
var pvtEndpointDnsGroupNameeventhub = 'eventhub-pvtEndpoint/mydnsgroupname'

var targetSubResourceDfs = 'dfs'
var targetSubResourceVault = 'vault'
var targetSubResourceEventHub = 'namespace'

resource nsg 'Microsoft.Network/networkSecurityGroups@2021-03-01' = {
  name: nsgName
  location: location
  properties: {
    securityRules: [
      {
        name: 'Microsoft.Databricks-workspaces_UseOnly_databricks-worker-to-worker-inbound'
        properties: {
          description: 'Required for worker nodes communication within a cluster.'
          protocol: '*'
          sourcePortRange: '*'
          destinationPortRange: '*'
          sourceAddressPrefix: 'VirtualNetwork'
          destinationAddressPrefix: 'VirtualNetwork'
          access: 'Allow'
          priority: 100
          direction: 'Inbound'
        }
      }
      {
        name: 'Microsoft.Databricks-workspaces_UseOnly_databricks-worker-to-databricks-webapp'
        properties: {
          description: 'Required for workers communication with Databricks Webapp.'
          protocol: 'Tcp'
          sourcePortRange: '*'
          destinationPortRange: '443'
          sourceAddressPrefix: 'VirtualNetwork'
          destinationAddressPrefix: 'AzureDatabricks'
          access: 'Allow'
          priority: 100
          direction: 'Outbound'
        }
      }
      {
        name: 'Microsoft.Databricks-workspaces_UseOnly_databricks-worker-to-sql'
        properties: {
          description: 'Required for workers communication with Azure SQL services.'
          protocol: 'Tcp'
          sourcePortRange: '*'
          destinationPortRange: '3306'
          sourceAddressPrefix: 'VirtualNetwork'
          destinationAddressPrefix: 'Sql'
          access: 'Allow'
          priority: 101
          direction: 'Outbound'
        }
      }
      {
        name: 'Microsoft.Databricks-workspaces_UseOnly_databricks-worker-to-storage'
        properties: {
          description: 'Required for workers communication with Azure Storage services.'
          protocol: 'Tcp'
          sourcePortRange: '*'
          destinationPortRange: '443'
          sourceAddressPrefix: 'VirtualNetwork'
          destinationAddressPrefix: 'Storage'
          access: 'Allow'
          priority: 102
          direction: 'Outbound'
        }
      }
      {
        name: 'Microsoft.Databricks-workspaces_UseOnly_databricks-worker-to-worker-outbound'
        properties: {
          description: 'Required for worker nodes communication within a cluster.'
          protocol: '*'
          sourcePortRange: '*'
          destinationPortRange: '*'
          sourceAddressPrefix: 'VirtualNetwork'
          destinationAddressPrefix: 'VirtualNetwork'
          access: 'Allow'
          priority: 103
          direction: 'Outbound'
        }
      }
      {
        name: 'Microsoft.Databricks-workspaces_UseOnly_databricks-worker-to-eventhub'
        properties: {
          description: 'Required for worker communication with Azure Eventhub services.'
          protocol: 'Tcp'
          sourcePortRange: '*'
          destinationPortRange: '9093'
          sourceAddressPrefix: 'VirtualNetwork'
          destinationAddressPrefix: 'EventHub'
          access: 'Allow'
          priority: 104
          direction: 'Outbound'
        }
      }
    ]
  }
}

resource vnet 'Microsoft.Network/virtualNetworks@2021-08-01' = {
  name: vnetName
  location: location
  properties: {
    addressSpace: {
      addressPrefixes: [
        vnetCidr
      ]
    }
    subnets: [
      {
        name: publicSubnetName
        properties: {
          addressPrefix: publicSubnetCidr
          networkSecurityGroup: {
            id: nsg.id
          }
          serviceEndpoints: [
            {
              service: 'Microsoft.Storage'
              locations: [resourceGroup().location]
            }
          ]
          delegations: [
            {
              name: 'databricks-del-public'
              properties: {
                serviceName: 'Microsoft.Databricks/workspaces'
              }
            }
          ]
        }
      }
      {
        name: privateSubnetName
        properties: {
          addressPrefix: privateSubnetCidr
          networkSecurityGroup: {
            id: nsg.id
          }
          delegations: [
            {
              name: 'databricks-del-private'
              properties: {
                serviceName: 'Microsoft.Databricks/workspaces'
              }
            }
          ]
        }
      }
      {
        name: PrivateEndpointSubnetName
        properties: {
          addressPrefix: privateEndpointSubnetCidr
          privateEndpointNetworkPolicies: 'Disabled'
        }
      }
    ]
  }
}

resource databricks 'Microsoft.Databricks/workspaces@2018-04-01' = {
  location: location
  name: workspaceName
  sku: {
    name: pricingTier
  }
  properties: {
    managedResourceGroupId: managedResourceGroupId
    parameters: {
      customVirtualNetworkId: {
        value: vnet.id
      }
      customPublicSubnetName: {
        value: publicSubnetName
      }
      customPrivateSubnetName: {
        value: privateSubnetName
      }
      enableNoPublicIp: {
        value: disablePublicIp
      }
    }
    publicNetworkAccess: publicNetworkAccess
    requiredNsgRules: requiredNsgRules
  }
}

resource privateEndpoint 'Microsoft.Network/privateEndpoints@2021-08-01' = {
  name: privateEndpointName
  location: location
  properties: {
    subnet: {
      id: resourceId('Microsoft.Network/virtualNetworks/subnets', vnetName, PrivateEndpointSubnetName)
    }
    privateLinkServiceConnections: [
      {
        name: privateEndpointName
        properties: {
          privateLinkServiceId: databricks.id
          groupIds: [
            'databricks_ui_api'
          ]
        }
      }
    ]
  }
}

resource privateDnsZone 'Microsoft.Network/privateDnsZones@2020-06-01' = {
  name: privateDnsZoneName
  location: 'global'
  dependsOn: [
    privateEndpoint
  ]
}

resource privateDnsZoneName_privateDnsZoneName_link 'Microsoft.Network/privateDnsZones/virtualNetworkLinks@2020-06-01' = {
  parent: privateDnsZone
  name: '${privateDnsZoneName}-link'
  location: 'global'
  properties: {
    registrationEnabled: false
    virtualNetwork: {
      id: vnet.id
    }
  }
}

resource pvtEndpointDnsGroup 'Microsoft.Network/privateEndpoints/privateDnsZoneGroups@2021-12-01' = {
  name: pvtEndpointDnsGroupName
  properties: {
    privateDnsZoneConfigs: [
      {
        name: 'config1'
        properties: {
          privateDnsZoneId: privateDnsZone.id
        }
      }
    ]
  }
  dependsOn: [
    privateEndpoint
  ]
}

resource PostDeploymentScriptForFileUpload 'Microsoft.Resources/deploymentScripts@2020-10-01' = {
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
  dependsOn: [
    databricks
  ]
}

resource identity 'Microsoft.ManagedIdentity/userAssignedIdentities@2018-11-30' = {
  name: identityName
  location: location
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
      virtualNetworkRules: [
        {
          id: resourceId('Microsoft.Network/virtualNetworks/subnets', vnetName, publicSubnetName)
          action: 'Allow'
          state: 'succeeded'
        }
      ]
      ipRules: []
      defaultAction: 'Allow'
    }
    minimumTlsVersion: 'TLS1_2'
    allowBlobPublicAccess: false
    supportsHttpsTrafficOnly: true
    accessTier: 'Hot'
  }
  dependsOn: [
    vnet
  ]
}

resource blobAccountName_default_container 'Microsoft.Storage/storageAccounts/blobServices/containers@2021-06-01' = if (ctrlDeployStorageAccount) {
  name: '${blobAccountName}/default/${containerName}'
  dependsOn: [
    blobAccount
  ]
}

resource privateEndpointstorage 'Microsoft.Network/privateEndpoints@2021-08-01' = {
  name: privateEndpointNamestorage
  location: location
  properties: {
    subnet: {
      id: resourceId('Microsoft.Network/virtualNetworks/subnets', vnetName, PrivateEndpointSubnetName)
    }
    privateLinkServiceConnections: [
      {
        name: privateEndpointNamestorage
        properties: {
          privateLinkServiceId: blobAccount.id
          groupIds: [
            targetSubResourceDfs
          ]
        }
      }
    ]
  }
}

resource privateDnsZonestorage 'Microsoft.Network/privateDnsZones@2020-06-01' = {
  name: privateDnsZoneNamestorage
  location: 'global'
  dependsOn: [
    privateEndpointstorage
  ]
}

resource privateDnsZoneName_privateDnsZoneName_link_storage 'Microsoft.Network/privateDnsZones/virtualNetworkLinks@2020-06-01' = {
  parent: privateDnsZonestorage
  name: '${privateDnsZoneNamestorage}-link'
  location: 'global'
  properties: {
    registrationEnabled: false
    virtualNetwork: {
      id: vnet.id
    }
  }
}

resource pvtEndpointDnsGroupstorage 'Microsoft.Network/privateEndpoints/privateDnsZoneGroups@2021-12-01' = {
  name: pvtEndpointDnsGroupNamestorage
  properties: {
    privateDnsZoneConfigs: [
      {
        name: 'config1storage'
        properties: {
          privateDnsZoneId: privateDnsZonestorage.id
        }
      }
    ]
  }
  dependsOn: [
    privateEndpointstorage
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

resource eventHubNamespace_eventHubName 'Microsoft.EventHub/namespaces/eventhubs@2021-01-01-preview' = if (ctrlDeployEventHub) {
  parent: eventHubNamespace
  name: eventHubName
  properties: {
    messageRetentionInDays: 7
    partitionCount: 1
  }
  dependsOn: [
    eventHubNamespace
  ]
}


resource eventHubName_rule 'Microsoft.EventHub/namespaces/eventhubs/authorizationRules@2021-01-01-preview' = if (ctrlDeployEventHub) {
  name: '${eventHubNamespaceName}/${eventHubName}/${eHRuleName}'
  properties: {
    rights: [
      'Send'
      'Listen'
    ]
  }
  dependsOn: [
    eventHubNamespace_eventHubName
  ]
}

resource privateEndpointeventhub 'Microsoft.Network/privateEndpoints@2021-08-01' = {
  name: privateEndpointNameeventhub
  location: location
  properties: {
    subnet: {
      id: resourceId('Microsoft.Network/virtualNetworks/subnets', vnetName, PrivateEndpointSubnetName)
    }
    privateLinkServiceConnections: [
      {
        name: privateEndpointNameeventhub
        properties: {
          privateLinkServiceId: eventHubNamespace.id
          groupIds: [
            targetSubResourceEventHub
          ]
        }
      }
    ]
  }
}

resource privateDnsZoneeventhub 'Microsoft.Network/privateDnsZones@2020-06-01' = {
  name: privateDnsZoneNameeventhub
  location: 'global'
  dependsOn: [
    privateEndpointeventhub
  ]
}

resource privateDnsZoneName_privateDnsZoneName_link_eventhub 'Microsoft.Network/privateDnsZones/virtualNetworkLinks@2020-06-01' = {
  parent: privateDnsZoneeventhub
  name: '${privateDnsZoneNameeventhub}-link'
  location: 'global'
  properties: {
    registrationEnabled: false
    virtualNetwork: {
      id: vnet.id
    }
  }
}

resource pvtEndpointDnsGroupeventhub 'Microsoft.Network/privateEndpoints/privateDnsZoneGroups@2021-12-01' = {
  name: pvtEndpointDnsGroupNameeventhub
  properties: {
    privateDnsZoneConfigs: [
      {
        name: 'config1eventhub'
        properties: {
          privateDnsZoneId: privateDnsZoneeventhub.id
        }
      }
    ]
  }
  dependsOn: [
    privateEndpointeventhub
  ]
}


resource vault_utcValue 'Microsoft.KeyVault/vaults@2021-04-01-preview' = if (ctrlDeployKeyVault) {
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

resource privateEndpointvault 'Microsoft.Network/privateEndpoints@2021-08-01' = {
  name: privateEndpointNamevault
  location: location
  properties: {
    subnet: {
      id: resourceId('Microsoft.Network/virtualNetworks/subnets', vnetName, PrivateEndpointSubnetName)
    }
    privateLinkServiceConnections: [
      {
        name: privateEndpointNamevault
        properties: {
          privateLinkServiceId: vault_utcValue.id
          groupIds: [
            targetSubResourceVault
          ]
        }
      }
    ]
  }
}

resource privateDnsZonevault 'Microsoft.Network/privateDnsZones@2020-06-01' = {
  name: privateDnsZoneNamevault
  location: 'global'
  dependsOn: [
    privateEndpointvault
  ]
}

resource privateDnsZoneName_privateDnsZoneName_link_vault 'Microsoft.Network/privateDnsZones/virtualNetworkLinks@2020-06-01' = {
  parent: privateDnsZonevault
  name: '${privateDnsZoneNamevault}-link'
  location: 'global'
  properties: {
    registrationEnabled: false
    virtualNetwork: {
      id: vnet.id
    }
  }
}

resource pvtEndpointDnsGroupvault 'Microsoft.Network/privateEndpoints/privateDnsZoneGroups@2021-12-01' = {
  name: pvtEndpointDnsGroupNamevault
  properties: {
    privateDnsZoneConfigs: [
      {
        name: 'config1vault'
        properties: {
          privateDnsZoneId: privateDnsZonevault.id
        }
      }
    ]
  }
  dependsOn: [
    privateEndpointvault
  ]
}
