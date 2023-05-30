
## App registration process

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
  
