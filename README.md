# Azure Databricks

This bicep deployment allows the user to deploy environment of Azure Databricks with cluster, pipeline and notebook.

## Overall Flow

![Flow diagram](./Assets/Development_FlowChart.png)

## Infrastructure and Application Plane Flow

![Flow diagram](./Assets/Databricks_Deployment_Workflow.png)

## Prerequisites

To deploy Bicep templates, you need **owner role** as we are assigning RBAC roles and write access on the resources you're deploying and access to all operations on the Microsoft.Resources/deployments resource type.

## Deployment Steps

1. Click 'Deploy To Azure' button given below to deploy all the resources.

[![Deploy to Azure](https://aka.ms/deploytoazurebutton)](https://portal.azure.com/#create/Microsoft.Template/uri/https%3A%2F%2Fraw.githubusercontent.com%2FDatabricksFactory%2Fdatabricks-migration%2Fmain%2Fmain.json)

Provide the values for the following parameters or default values will be considered:
- Resource group (create new)
- Region (Default value is 'east us')
- Event Hub SKU (Default value is 'Standard')
- Blob storage account name (Default value is random unique string)
- Container name (Default value is 'data')
- Event Hub Rule Name (Default value is 'rule')
- Identity Name for post deployment script (Default value is 'PostDeploymentScriptuserAssignedName')
- Unique Suffix (Default value is random unique string)
- Firstuniquestring (Default value is random unique string)
- Seconduniquestring (Default value is random unique string)
- Utc Value (utcNow)
- Option (true/false) for Cluster deployment (Default value is true)
- Option (true/false) for Storage account deployment (Default value is true)
- Option (true/false) for Key Vault deployment (Default value is true)
- Option (true/false) for Event Hub deployment (Default value is true)
- Lifetime Seconds(Databricks token) (Default value is 1200)
- Comment (Default value is 'ARM deployment')
- Cluster name (Default value is 'dbcluster')
- Spark Version (Default value is '11.3.x-scala2.12')
- Auto Termination Minutes(cluster) (Default value is 30)
- Num Workers (Default value is 2)
- Node Type Id (Default value is 'Standard_DS3_v2')
- Driver Node Type Id (Default value is 'Standard_DS3_v2')
- Retry Limit (Default value is 15)
- Retry Time (Default value is 60)
- Option (true/false) for Disable public IP (Default value is true)
- Nsg Name (Default value is 'databricks-nsg')
- Price Tier (Default value is 'Premium')
- Private Subnet Cidr (Default value is '10.179.0.0/18')
- Private Subnet Name (Default value is 'private-subnet')
- Option (Enabled/Disabled) for Public Network Acess (Default value is 'Enabled')
- Public Subnet Cidr (Default value is '10.179.64.0/18')
- Private Endpoint Subnet Cidr (Default value is '10.179.128.0/24')
- Public Subnet Name (Default value is 'public-subnet')
- Required Nsg Rules (Default value is 'NoAzureDatabricksRules')
- Vnet Cidr (Default value is '10.179.0.0/16')
- Vnet Name (Default value is 'databricks-vnet')
- Private Endpoint Subnet Name (Default value is 'default')
- Workspace Name (Default value is 'default')
- Fileuploaduri (path for post deployment powershell script for deploying cluster, notebook and pipeline)
- Option (true/false) for Ctrl Deploy Notebook
- Option (true/false) for Ctrl Deploy Pipeline 
- Pipeline Name (Default value is 'Sample Pipeline')
- Storage Path (Default value is 'dbfs:/user/hive/warehouse')
- Target Schema Name (Default value is 'Sample')
- Min Workers (Default value is 1)
- Max Workers (Default value is 5)
- Notebook Path (URI path of the notebooks to be uploaded)

2. Click **'Review + Create'**.

3. On successful validation, click **'Create'**.

## Post Deployment

The **allin1.ps1** script is used to deploy a **Cluster**, upload **notebooks** and create a **pipeline** in the Databricks Workspace . It takes the following parameters:

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
 * $CTRL_DEPLOY_PIPELINE - True or false
 * $PIPELINENAME -  Name of the pipeline
 * $STORAGE - Storage path for DLT
 * $TARGETSCHEMA - Target Schema name
 * $MINWORKERS - Min workers for Cluster
 * $MAXWORKERS - Max workers for Cluster
 * $NOTEBOOK_PATH - Path of notebooks to be deployed in workspace
 
## Azure Services being deployed

1. **Databricks Workspace**: Here we can develop a DLT pipeline and process our data.
2. **Eventhub**: To capture streaming data from our source into the ADLS storage via a stream analytics job.
3. **ADLS Gen 2 Storage with a Container**: This will serve as our staging layer where data coming from Eventhub will be stored .
4. **Key Vault**: Used for data security.
5. **Cluster** is created in databricks workspace.
5. **Notebooks** are uploaded in databricks workspace.
6. **Pipeline** is created in databricks workspace.
