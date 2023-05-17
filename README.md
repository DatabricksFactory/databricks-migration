# Azure Databricks

This bicep deployment allows the user to deploy environment of Azure Databricks with cluster, pipeline and notebook.

## Workflow

![Flow diagram](./Assets/Databricks_Deployment_Workflow.png)

## Overall Flow

![Flow diagram](./Assets/Development_FlowChart.png)

## Prerequisites

To deploy Bicep templates, you need **owner role** as we are assigning RBAC roles and write access on the resources you're deploying and access to all operations on the Microsoft.Resources/deployments resource type.

## Deployment Steps

### Infrastructure Plane Automation (```InfrastructurePlaneDeployment.bicep```)

Deploying infrastructure resources for Databricks migration: **Databricks workspace**, **Databrciks Cluster**, **Storage account**, **Event Hub** and **Key Vault**.

1. Open **Windows Powershell** or **Azure CLI** and login to your azure account using command:

```
az login
```

2. Next, Create a resource group using command:

```
az group create --name <resource-group-name> --location <location>
```

3. Save the ```InfrastructurePlaneDeployment.bicep``` file as **template1.bicep** to your local computer. Run the following command to deploy the bicep file:

```
az deployment group create --resource-group <resource-group-name> --template-file <path-to-template1.bicep>
```

Provide the values for:
- Option (true/false) for Cluster deployment

You can also provide the values for below parameters. If following parameter values are not provided explicitly, it will consider default values.
- Option (true/false) for Storage account deployment (Default value is true)
- Option (true/false) for Key Vault deployment (Default value is true)
- Option (true/false) for Event Hub deployment (Default value is true)
- Event Hub Namespace (Default value is 'Standard')
- Storage account name (Default value is random unique string)
- Container name (Default value is 'data')
- Identity Name for post deployment script (Default value is 'PostDeploymentScriptuserAssignedName')
- Unique Suffix (Default value is random unique string)
- URI for post deployment powershell script for deploying cluster
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

The deployment can take a few minutes to complete. When it finishes, you see a message that includes the result:

```
"provisioningState": "Succeeded",
```

### Application Plane Automation (```ApplicationPlaneDeployment.bicep```)

Deploying application resources for Databricks migration: **Notebooks** and **Pipelines**.

Save the ```ApplicationPlaneDeployment.bicep``` file as **template2.bicep** to your local computer. Deploy the bicep file following the above steps you followed for running ```template1.bicep```:

```
az deployment group create --resource-group <resource-group-name> --template-file <path-to-template2.bicep>
```

Use the same **resource group name** you used to deploy ```InfrastructurePlaneDeployment.bicep```.

Provide the values for:
- Option (true/false) for Notebook deployment
- Option (true/false) for Pipeline deployment

You can also provide the values for below parameters. If following parameter values are not provided explicitly, it will consider default values.
- Identity Name for post deployment script (Default value is 'PostDeploymentScriptuserAssignedName')
- Unique Suffix (Default value is random unique string)
- URI for post deployment powershell script for deploying notebook and pipeline
- Databricks token lifetime (Default value is 1200)
- Name of the pipeline (Default value is 'Sample Pipeline')
- Storage path where DLT will be created (Default value is 'dbfs:/user/hive/warehouse')
- Target schema name (Default value is 'Sample')
- Min workers (Default value is 1)
- Max workers (Default value is 5)
- URI path of the notebooks to be uploaded

The deployment can take a few minutes to complete. When it finishes, you see a message that includes the result:

```
"provisioningState": "Succeeded",
```

## Post Deployment

The **InfrastructurePlaneDeployment.ps1** script is used to deploy a **Cluster** in the Databricks Workspace . It takes the following parameters:

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

The **ApplicationPlaneDeployment.ps1** script is used to deploy **Notebooks** and **Pipeline** in the Databricks Workspace . It takes the following parameters:

 * $RG_NAME - Resource Group Name containing the Databricks Workspace
 * $REGION - Resource Group Region
 * $LIFETIME_SECONDS - Lifetime of the Databricks token in seconds
 * $COMMENT - Side note on the token generation
 * $CTRL_DEPLOY_NOTEBOOK - True or false
 * $CTRL_DEPLOY_PIPELINE - True or false
 * $PIPELINENAME -  Name of the pipeline
 * $STORAGE - Storage path for DLT
 * $TARGETSCHEMA - Target Schema name
 * $MINWORKERS - Min workers for Cluster
 * $MAXWORKERS - Max workers for Cluster
 * $NOTEBOOK_PATH - Path of notebooks to be deployed in workspace
 
## Azure Services being deployed

Use the Azure portal, Azure CLI, or Azure PowerShell to list the deployed resources in the resource group.

```
az resource list --resource-group <resource-group-name>
```

After ```InfrastructurePlaneDeployment.bicep``` template deployment, the following resources get created:

1. **Databricks Workspace**: Here we can develop a DLT pipeline and process our data. An All Purpose Cluster will also be created using post deployment script ```clusterDeploy.ps1``` which uses REST API to create the cluster.

2. **Eventhub**: To capture streaming data from our source into the ADLS storage via a stream analytics job.

3. **ADLS Gen 2 Storage with a Container**: This will serve as our staging layer where data coming from Eventhub will be stored .

4. **Key Vault**: Used for data security .

After ```ApplicationPlaneDeployment.bicep``` template deployment, the following resources get created in the workspace:

1. **Notebooks** 

2. **Pipeline**
