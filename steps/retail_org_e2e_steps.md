1.	In steps 2-12, we connect our storage accounts to our Databricks workspace. Refer [this document](https://github.com/DatabricksFactory/databricks-migration/blob/dev/steps/access_storage_with_AAD.md) for detailed instructions.
2.	Create a new **App Registration**.
3.	Make a note of *Client ID* and *Directory ID* for future reference.
4.	Create a new **Client Secret**.
5.	Copy the *Client Secret* Value for future reference.
6.	Add a **role assignment** to your ADLS storage account and Blob Data storage account. Give **Storage Blob Data Contributor** access to your app registration.
7.	Create a new **Access Policy** in the Key Vault to give *Get* and *List* permissions to your app registration.
8.	Run the [azure-key-vaults-assign-access-policies.ps1](https://raw.githubusercontent.com/DatabricksFactory/databricks-migration/dev/azure-key-vaults-assign-access-policies.ps1) script in Azure CLI by updating with key vault name and user email id.
9.	Change the Firewall rule of Key Vault to ***“Allow public access from all networks”*** in Networking.
10.	Generate a **Secret** in the Key Vault for the *Client ID value* that you copied earlier.
11.	Launch the Databricks Workspace and edit the URL as follows https://adb.......azuredatabricks.net/#secrets/createScope
12.	Fill in the details as follows :  
Scope Name – `keyvaultdb`  
DNS Name – Key Vault &#8594; Properties &#8594; Vault URI  
Resource - Key Vault &#8594; Properties &#8594;  Resource ID  
12.	Start the Cluster.
13.	Edit the **blob_to_adls_copy** notebook as follows.  
•	Replace *databricksscopename* in cmd2 with your scope name.  
•	Replace *databricksservicekey* in cmd2 with the name of the secret in the Key Vault.  
•	Replace the *Client ID* with your client ID in cmd 2 and cmd3.  
•	Replace the *Directory ID* with your directory ID in cmd 2 and cmd3.  
•	Replace the *blob storage account name* and *container name* in the cmd2 and cmd4 with your Blob Storage Account name.  
•	Replace the *ADLS storage account name* and *container name* in the cmd3 and cmd4 with your ADLS Storage Account name.  
14.	Run the Job **blob_to_adls_copy**.
15.	Edit the **bronze-layer-notebook** notebook as follows:  
•	Replace *databricksscopename* in cmd2 with your scope name.  
•	Replace *databricksservicekey* in cmd2 with the name of the secret in the Key Vault.  
•	Replace the *Client ID* with your client ID in cmd 2.  
•	Replace the *Directory ID* with your directory ID in cmd 2.  
•	Replace the *ADLS storage account name* and *container name* in the cmd2 and cmd3 with your ADLS Storage Account name.  
15.	 Run the Job **retail_org_batch_dlt**.

