//Parameters

@description('The name of the Azure Databricks workspace to create.')
param workspaceName string

param pricingTier string

//Variables

var location = resourceGroup().location

var managedResourceGroupName = 'databricks-rg-${workspaceName}-${uniqueString(workspaceName, resourceGroup().id)}'

var trimmedMRGName = substring(managedResourceGroupName, 0, min(length(managedResourceGroupName), 90))

var managedResourceGroupId = '${subscription().id}/resourceGroups/${trimmedMRGName}'

//Resources

resource databricks_public 'Microsoft.Databricks/workspaces@2018-04-01' = {
  location: location
  name: workspaceName
  sku: {
    name: pricingTier
  }
  properties: {
    managedResourceGroupId: managedResourceGroupId
  }
}
