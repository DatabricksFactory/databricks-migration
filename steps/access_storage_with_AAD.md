# Construction of Bronze Layer

As the batch and stream data are ingested inside the adls gen2 storage account. Let's mount the tables in the DBFS file system and ADLS Gen2 using Delta Live Tables.

Delta Live Tables is a framework for building reliable, maintainable, and testable data processing pipelines. You define the transformations to perform on your data, and Delta Live Tables manages task orchestration, cluster management, monitoring, data quality, and error handling.

### Objective:
Creating raw/bronze layer from staging using Delta Live Tables.

### Pre-requistes:
* Adls Gen 2 account populated with batch and stream data
* Databricks workspace alongwith cluster
* Azure Active Directory
* Key Vault

## Creating raw/bronze layer using Delta Live Tables

Let's begin with app registration process.

By **registering an app** in **Azure Active Directory** provides you with Application ID which can be used in client application's *(here, Databricks)* 
authentication code. 

1.	Go to **Azure Active Directory** and click on **App Registrations** under **Manage** on the left pane.
3.	Now click on **"+ New Registration"** to create a new **App Registration**.

    ![appReg](./assets/1c-app_reg.jpg "app reg")

5.	Give the name as ``app_registration_dblab`` and click on **Register**.

    ![appName](./assets/2c-app_name.jpg "app name")

7.	Once created, copy the **Client ID** and **Directory ID** from the overview page to a notepad for future reference.

    ![appId](./assets/3c-app_id.jpg "app id")

9.	Now click on **Certificates and secrets** under the **Manage** in the left pane.
11.	Create a new **Client Secret** by naming it ``client_secret_dblab`` and click on **Add**.
15.	Once done, copy the **Client Secret Value** for future reference.

    ![cs](./assets/4c-cs.jpg "Cs")
    
    ![appClientValue](./assets/32c-app_client_value.jpg "App Client Value")
    
17.	  Go to your storage account.  
      **adls-{Random-String}** --> **Access Control** in the left pane -- > **Add role assignment**  
      
      ![iam](./assets/5c-iam.jpg "Iam")
      
18. Search for **Storage Blob Data Contributor** --> Next --> Select **Members** --> Search for **app_registration_dblab** --> Select --> Review and Assign.
    ![sbc](./assets/6c-sbc.jpg "Sbc")
      
    ![appRegMem](./assets/7c-app_reg_mem.jpg "App Reg Mem")
    
    ![revAssign](./assets/8c-rev_assign.jpg "Rev Assign")
    
19.	Go to your Key Vault which goes by the name **keyvault{utcNow}** present in your resource group and click on **Access Policies** in the left pane. **Create** a new access policy for the app_registration_dblab.
    
    ![kv](./assets/9c-kv.jpg "Kv")
    
22. Select **Get** and **List** under *Key permissions* and *Secret permissions*.
    
    ![kvPer](./assets/10c-kvper.jpg "Kv Per")
    
21. Select **app_registration_dblab** in *Principal* tab.
    
    ![kvPrin](./assets/11c-kvprin.jpg "Kv Prin")
    
23. Review and create.
    
    ![kvCreate](./assets/12c-kv_create.jpg "Kv Create")
    
25.	  Go to **Secrets** under *Objects*. Click on **+Generate/Import** and fill in the following details:  
      **Name** – ``secret``  
      **Secret value** – The Client Secret Value that you had copied earlier. (Refer Step 7)
      And click create.

      ![kvSecret](./assets/13c-kv_secret.jpg "Kv Secret")
      
      ![kvSecretCreate](./assets/14c-kv_secret_create.jpg "Kv Secret Create")
    
11.	Go to your resource group and open the **Databricks Workspace** which goes by the name {databricks-{randomstring}}.
13.	Edit the URL of the workspace as ``https://adb.......azuredatabricks.net/#secrets/createScope``.

    ![dbUrl](./assets/15c-db_url.jpg "Db Url")
    
    ![dburlScope](./assets/16c-dburl_Scope.jpg "Dburl Scope")
    
15.	  You should see the **Create Secret Scope** page. Fill in the following details and click on Create.  
      **Scope Name** – ``keyvaultdb``  
      **DNS Name** – Go to your resource group, **Key Vault** --> Click on **properties** under Settings --> copy the **Vault URI** and paste it.  
      **Resource** -  Go to your resource group, **Key Vault** --> Click on **properties** under Settings --> copy the **Resource ID** and paste it.  

      ![kvProp](./assets/17c-kv_prop.jpg "Kv Prop")
      
      ![scopeCerd](./assets/18c-scope_cerd.jpg "Scope Cerd")
      
      Click **Ok** in the below pop-up.
      
      ![ScopePopup](./assets/19c-scope_popup.jpg "Scope Popup")
    
14.	Go to the **Databricks Workspace** and go to **Compute** in the left pane. Click the cluster **dbcluster**.

    ![clusterOn](./assets/20c-cluster_on.jpg "Cluster On")
    
15. You may note that the *Policy* selected is *Unrestricted* in the **Configurations** tab. Click on the **Edit** icon on the right to change the policy.
    
    ![dbClusterEdit](./assets/28c-db_cluster_edit.jpg "Db Cluster Edit")
    
17. Change the *Policy* to **Personal Compute**, **Confirm and Restart**.
    
    ![dbPolicyUpdate](./assets/29c-db_policy_update.jpg "Db Policy Update")
    
    ![dbClusterConfirm](./assets/30c-db_cluster_confirm.jpg "Db Cluster Confirm")
    
19. Once the Policy is created, proceed with creating the notebook.
    
    ![dbClusterView](./assets/31c-db_cluster_view.jpg "Db Cluster View")
    
21.	  Click on **+New** in the left pane, select **Notebook**. Give the following details.  
  	  **Name of the Notebook** as ``retailorg``  
      **Language** as ``Python``  
      **Cluster** as ``dbcluster``

      ![notebook](./assets/21c-notebook.jpg "Notebook")
      
      ![notebookCreate](./assets/22c-notebook_create.jpg "Notebook Create")
    
18.	Run the following code to mount the files on the **DBFS** and **ADLS**:

    **Cmd1 :**
    ```python
    configs ={"fs.azure.account.auth.type": "OAuth",
              "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
              "fs.azure.account.oauth2.client.id":"{Client ID}",
              "fs.azure.account.oauth2.client.secret": dbutils.secrets.get(scope="{keyvault name}",key="{client secret name}"),
              "fs.azure.account.oauth2.client.endpoint": "https://login.microsoftonline.com/{Directory ID}/oauth2/token" }
    try:
        dbutils.fs.mount(
        source="abfss://data@{Storage Account Name}.dfs.core.windows.net/",
        mount_point="/mnt/data/",
        extra_configs = configs)
    except Exception as e:
        print ("Error: {} already mounted.Run unmount first")
    ```

    **Replace the Client ID, Key Vault Name, Client Secret Name, Directory ID, Storage Account Name in the above code with yours.**  
    **Refer Step 4 for Client ID and Directory ID**.  
    **Refer Step 6 for Key Vault Name**.
    
    >If you get an error message saying the data is already mounted, unmount the data using the command **dbutils.fs.unmount("/mnt/data")**.

    **Cmd2:**
    ```python
    import dlt
    from pyspark.sql.functions import *
    from pyspark.sql.types import *
    import datetime
    
    #create streaming delta live table
    salesorderstreampath ='/mnt/data/'
    df = spark.read.json(salesorderstreampath)
    @dlt.table(
        comment="the streaming raw dataset."
        )
        
    def sales_orders_stream_raw():
        return spark.readStream.schema(df.schema).json(salesorderstreampath)

    ```

    **Cmd3:**
    ```python
    # customers raw delta live table 
    customerpath='/mnt/data/customers/customers.parquet'
    @dlt.table(
        comment="the customers raw dataset."
        )
    def customers_raw():
        return (spark.read.parquet(customerpath,header=True))
    ```

    **Cmd4:**
    ```python
    #products raw delta live table
    productpath ='/mnt/data/products/products.parquet' 
    @dlt.table(
        comment="the prdduct raw dataset."
        )
    def products_raw():
        return (spark.read.parquet(productpath,header=True))
    ```

    **Cmd5:**
    ```python
    # sales order batch raw delta live table
    salesorderbatchpath='/mnt/data/sales_orders/sales_orders.parquet' 
    @dlt.table(
        comment="the sales order batch raw dataset.."
        )
    def sales_orders_batch_raw():
        return (spark.read.parquet(salesorderbatchpath,header=True))
    ```
    
   24. After typing the above commands, go to **Workflows** which you will find in the left pane. Click on **Delta Live Tables**. Click on **Create           Pipeline**.
   
       ![workflowTab](./assets/33c-workflow_tab.jpg "Workflow tab")
       
   25. Fill the following details and **Save** the Pipeline.  
        **General**  
        **Pipeline Name** - ``retail-test``  
        **Product edition** - ``Advanced`` 
        
        **Source code**   
        **Notebook Library**- ``/Users/{user account}/test-mount``   
        
        **Destination**    
        **Storage Location** - ``/mnt/data/retailorg/``  
        **Target Schema** - ``retail_org``  
        
        ![genralWf](./assets/34c-general_wf.jpg "General Wf")
        
        ![savePipelineWf](./assets/35c-save_pipeline_wf.jpg "Save Pipeline Wf")
        
   26. Click on **Development** and **Start** the pipeline.
       
       ![startPipelineWf](./assets/36c-start_pipeline_wf.jpg "Start Pipeline Wf")


27. Go to you Storage Account adls-{random-string} --> data and make sure the following folders are present.
    * customers_raw
    * products_raw
    * sales_orders_batch_raw
    * sales_orders_stream_raw

28. Go to your Databricks Workspace databricks-{random-string} --> Data tab --> hive_metastore catalog --> retail_org and make sure the raw tables are present.
