# Azure Databricks

This bicep deployment allows the user to deploy environment of Azure Databricks with cluster, pipeline and notebook.

## Prerequisites

To deploy this Bicep file, you need **owner role** as we are assigning RBAC roles and write access on the resources you're deploying and access to all operations on the Microsoft.Resources/deployments resource type.

## Deployment Steps

1. Open **Windows Powershell** or **Azure CLI** and login to your azure account using command:

```
az login
```

2. Next, Create a resource group using command:

```
az group create --name <resource-group-name> --location <location>
```

3. Save the Bicep file as **template.bicep** to your local computer. Run the following command to deploy the bicep file:

```
az deployment group create --resource-group <resource-group-name> --template-file <path-to-bicep>
```

Provide the values for:
- Option (true/false) for Cluster deployment
- Option (true/false) for Notebook deployment
- Option (true/false) for Pipeline deployment

You can also provide the values for below parameters. If following parameter values are not provided explicitly, it will consider default values.
- Option (true/false) for Storage account deployment (Default value is true)
- Option (true/false) for Key Vault deployment (Default value is true)
- Option (true/false) for Event Hub deployment (Default value is true)
- Event Hub Namespace (Default value is 'Standard')
- Storage account name (Default value is random unique string)
- Container name (Default value is 'data')
- Identity Name for post deployment script (Default value is 'PostDeploymentScriptuserAssignedName')
- Unique Suffix (Default value is random unique string)
- URI for post deployment powershell script for deploying cluster, notebook and pipeline (Default value is raw git link)
- Time Zone (utcNow)
- Databricks token lifetime (Default value is 1200)
- Name of the Databricks cluster (Default value is 'dbcluster')
- Cluster Spark version (Default value is '11.3.x-scala2.12')
- Cluster terminates after specified minutes of inactivity (Default value is 30)
- Number of worker nodes (Default value is 2)
- Type of worker node (Default value is 'Standard_DS3_v2')
- Type of driver node (Default value is 'Standard_DS3_v2')
- Max number of retries (Default value is 15)
- Interval between each retries in seconds (Default value is 60)
- Path of the notebook to be uploaded (Default value is raw git link)

The deployment can take a few minutes to complete. When it finishes, you see a message that includes the result:

```
"provisioningState": "Succeeded",
```

## Post Deployment

The **deployClusterNotebook.ps1** script is used to deploy a Cluster and a Notebook in the Databricks Workspace . It takes the following parameters:

 * $RG_NAME - Resource Group Name containing the Databricks Workspace.
 * $REGION - Resource Group Region
 * $WORKSPACE_NAME - Databricks Workspace Name
 * $LIFETIME_SECONDS - Lifetime of the Databricks token in seconds
 * $COMMENT - Side note on the token generation
 * $CLUSTER_NAME - Databricks Cluster Name 
 * $SPARK_VERSION - The version of Spark in the Cluster
 * $AUTOTERMINATION_MINUTES - Cluster terminates after specified minutes of inactivity
 * $NUM_WORKERS - Number of worker nodes in the Cluster
 * $NODE_TYPE_ID - Type of worker node
 * $DRIVER_NODE_TYPE_ID - Type of driver node
 * $RETRY_LIMIT - Max number of retries.
 * $RETRY_TIME - Interval between each retries in seconds.
 * $CTRL_DEPLOY_CLUSTER - True or false
 * $CTRL_DEPLOY_NOTEBOOK - True or false
 * $NOTEBOOK_PATH - Path of the notebook
 
## Azure Services being deployed

Use the Azure portal, Azure CLI, or Azure PowerShell to list the deployed resources in the resource group.

```
az resource list --resource-group <resource-group-name>
```

After deployment, the following resources get created:

1. **Databricks Workspace**: Here we can develop a DLT pipeline and process our data. An All Purpose Cluster will also be created using post deployment script ```clusterDeploy.ps1``` which uses REST API to create the cluster.

2. **Eventhub**: To capture streaming data from our source into the ADLS storage via a stream analytics job.

3. **ADLS Gen 2 Storage with a Container**: This will serve as our staging layer where data coming from Eventhub will be stored .

4. **Key Vault**: Used for data security .

