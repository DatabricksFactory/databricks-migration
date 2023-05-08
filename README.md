# Azure Databricks

Azure Databricks is a unified set of tools for building, deploying, sharing, and maintaining enterprise-grade data solutions at scale. The Azure Databricks Lakehouse Platform integrates with cloud storage and security in your cloud account, and manages and deploys cloud infrastructure on your behalf.

## How to deploy resources with Bicep file and Azure CLI/Powershell

To deploy this Bicep file, you need **owner role** as we are assigning RBAC roles and write access on the resources you're deploying and access to all operations on the Microsoft.Resources/deployments resource type.

## Steps to deploy bicep file ```template.bicep```

   Run the **deployClusterNotebook** to deploy a Cluster and a Notebook in the Databricks Workspace . It takes the following parameters :

 * $RG_NAME - Resource Group Name containing the Databricks Workspace.
 * $REGION - Resource Group Region
 * $WORKSPACE_NAME - Databricks Workspace Name
 * $LIFETIME_SECONDS,
 * $COMMENT,
 * $CLUSTER_NAME - Databricks Cluster Name 
 * $SPARK_VERSION - The version of Spark in the Cluster
 * $AUTOTERMINATION_MINUTES,
 * $NUM_WORKERS - Number of workers in the Cluster
 * $NODE_TYPE_ID,
 * $DRIVER_NODE_TYPE_ID,
 * $RETRY_LIMIT,
 * $RETRY_TIME,
 * $tenant_id - The Azure Active Directory tenant ID.
 * $client_id - The Azure Active Directory application client ID.
 * $client_secret - The Azure Active Directory application client secret.
 * $subscription_id - The Azure subscription ID.
 * $resourceGroup - The name of the resource group containing the Databricks workspace.
 * $workspaceName - The name of the Databricks workspace.
 * $notebookPathUnderWorkspace - The path to the directory in the workspace where the notebooks should be deployed.

 > EXAMPLE
    .\deploy_notebooks.ps1 -tenant_id "12345678-1234-1234-1234-123456789012" -client_id "12345678-1234-1234-1234-123456789012" -client_secret "my_client_secret" -subscription_id "12345678-1234-1234-1234-123456789012" -resourceGroup "my_resource_group" -workspaceName "my_workspace" -notebookPathUnderWorkspace "/my_notebooks"



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

The deployment can take a few minutes to complete. When it finishes, you see a message that includes the result:

```
"provisioningState": "Succeeded",
```

## Deployed resources

Use the Azure portal, Azure CLI, or Azure PowerShell to list the deployed resources in the resource group.

```
az resource list --resource-group <resource-group-name>
```

After deployment, the following resources get created:

1. **Databricks Workspace**: Here we can develop a DLT pipeline and process our data. An All Purpose Cluster will also be created using post deployment script ```clusterDeploy.ps1``` which uses REST API to create the cluster.

2. **Eventhub**: To capture streaming data from our source into the ADLS storage via a stream analytics job.

3. **ADLS Gen 2 Storage with a Container**: This will serve as our staging layer where data coming from Eventhub will be stored .

4. **Key Vault**: Used for data security .

