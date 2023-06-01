param(
    [string] $RG_NAME,
    [string] $REGION,
    [string] $WORKSPACE_NAME,
    [string] $SA_NAME,
    [bool] $SA_EXISTS,
    [int] $LIFETIME_SECONDS,
    [string] $COMMENT,
    [string] $CLUSTER_NAME,
    [string] $SPARK_VERSION,
    [int] $AUTOTERMINATION_MINUTES,
    [string] $NUM_WORKERS,
    [string] $NODE_TYPE_ID,
    [string] $DRIVER_NODE_TYPE_ID,
    [int] $RETRY_LIMIT,
    [int] $RETRY_TIME,
    [bool] $CTRL_DEPLOY_CLUSTER,
    [int] $MINWORKERS,
    [int] $MAXWORKERS,
    [string] $PIPELINENAME,
    [string] $STORAGE,
    [string] $TARGETSCHEMA,
    [bool] $CTRL_DEPLOY_NOTEBOOK,
    [bool] $CTRL_DEPLOY_PIPELINE,
    [string] $NOTEBOOK_PATH,
    [bool] $SRC_FILESOURCE,
    [bool] $SRC_AZSQL,
    [bool] $SRC_AZMYSQL,
    [bool] $SRC_AZPSQL,
    [bool] $SRC_SQL_ONPREM,
    [bool] $SRC_PSQL_ONPREM,
    [bool] $SRC_ORACLE,
    [bool] $SRC_EVENTHUB ,
    [string] $CTRL_SYNTAX,
    [string] $SUBSCRIPTION_ID
)

Write-Output "Task: Generating Databricks Token"

try {
    $token = (Get-AzAccessToken).Token
    $url = "https://management.azure.com/subscriptions/" + $SUBSCRIPTION_ID + "/resourceGroups/" + $RG_NAME + "/providers/Microsoft.Databricks/workspaces/" + $WORKSPACE_NAME + "?api-version=2023-02-01"
    $headers1 = @{ Authorization = "Bearer $token"; 'ContentType' = "application/json"}
    $res1 = Invoke-RestMethod -Method Get -ContentType "application/json" -Uri $url  -Headers $headers1
    $WorkspaceUrl =  $res1.properties.workspaceUrl
    Write-Host $WorkspaceUrl
}
catch {
    $errorMessage = $_.Exception.Message
    Write-Host "Error message: $errorMessage"
}

try {
    $WORKSPACE_ID = Get-AzResource -ResourceType Microsoft.Databricks/workspaces -ResourceGroupName $RG_NAME -Name $WORKSPACE_NAME
    $ACTUAL_WORKSPACE_ID = $WORKSPACE_ID.ResourceId
}
catch {
    Write-Host "Error while getting workspace ID"
    $errorMessage = $_.Exception.Message
    Write-Host "Error message: $errorMessage"
}
    
try {
    [string] $TOKEN = (Get-AzAccessToken -Resource '2ff814a6-3304-4ab8-85cb-cd0e6f879c1d').Token
    Write-Host "Resource Token: $token"
}
catch {
    Write-Host "Error while getting the resource token"
    $errorMessage = $_.Exception.Message
    Write-Host "Error message: $errorMessage"    
}
    
try {
    [string] $AZ_TOKEN = (Get-AzAccessToken -ResourceUrl 'https://management.core.windows.net/').Token   
    Write-Host "Management token: $AZ_TOKEN"
}
catch {
    Write-Host "Error while getting the management token"
    $errorMessage = $_.Exception.Message
    Write-Host "Error message: $errorMessage"    
}
    
$HEADERS = @{
    "Authorization"                            = "Bearer $TOKEN"
    "X-Databricks-Azure-SP-Management-Token"   = "$AZ_TOKEN"
    "X-Databricks-Azure-Workspace-Resource-Id" = "$ACTUAL_WORKSPACE_ID"
}
$BODY = @"
    { "lifetime_seconds": $LIFETIME_SECONDS, "comment": "$COMMENT" }
"@
    
try {
    Write-Host "Attempt 1 : generating Personal Access Token"
    $DB_PAT = ((Invoke-RestMethod -Method POST -Uri "https://$WorkspaceUrl/api/2.0/token/create" -Headers $HEADERS -Body $BODY).token_value)
    Write-Output "PAT: $DB_PAT"
}
catch {
    Write-Host "Attempt 1 : Error while calling the Databricks API for generating Personal Access Token"
    $errorMessage = $_.Exception.Message
    Write-Host "Error message: $errorMessage" 
    try {
    Write-Host "Attempt 2 : generating Personal Access Token"
    $DB_PAT = ((Invoke-RestMethod -Method POST -Uri "https://$WorkspaceUrl/api/2.0/token/create" -Headers $HEADERS -Body $BODY).token_value)
    Write-Output "PAT: $DB_PAT"
    }
    catch {
    Write-Host "Attempt 2 : Error while calling the Databricks API for generating Personal Access Token"
    $errorMessage = $_.Exception.Message
    Write-Host "Error message: $errorMessage" 
    }
}


if ($CTRL_DEPLOY_CLUSTER -and ($null -ne $DB_PAT) ) {
        
    Write-Output "Task: Creating cluster"
    
    $HEADERS = @{
        "Authorization" = "Bearer $DB_PAT"
        "Content-Type"  = "application/json"
    }
    $BODY = @"
            {"cluster_name": "$CLUSTER_NAME", "spark_version": "$SPARK_VERSION", "autotermination_minutes": $AUTOTERMINATION_MINUTES, "num_workers": "$NUM_WORKERS", "node_type_id": "$NODE_TYPE_ID", "driver_node_type_id": "$DRIVER_NODE_TYPE_ID" }
"@

    try {
        $CLUSTER_ID = ((Invoke-RestMethod -Method POST -Uri "https://$WorkspaceUrl/api/2.0/clusters/create" -Headers $HEADERS -Body $BODY).cluster_id)
    }
    catch {
        Write-Host "Error while calling the Databricks API for creating the cluster"
        $errorMessage = $_.Exception.Message
        Write-Host "Error message: $errorMessage"    
    }

    if ( $CLUSTER_ID -ne "null" ) {
        Write-Output "[INFO] Cluster created with CLUSTER_ID: $CLUSTER_ID"

        Write-Output "Task: Checking cluster"

        $RETRY_COUNT = 0
        for( $RETRY_COUNT = 1; $RETRY_COUNT -le $RETRY_LIMIT; $RETRY_COUNT++ ) {
            
            Write-Output "[INFO] Attempt $RETRY_COUNT of $RETRY_LIMIT"
            
            $HEADERS = @{
                "Authorization" = "Bearer $DB_PAT"
            }
            
            try {
                $STATE = ((Invoke-RestMethod -Method GET -Uri "https://$WorkspaceUrl/api/2.0/clusters/get?cluster_id=$CLUSTER_ID" -Headers $HEADERS).state)
            }
            catch {
                Write-Host "Error while calling the Databricks API for checking the clusters' state"
                $errorMessage = $_.Exception.Message
                Write-Host "Error message: $errorMessage"    
            }
            
            if ($STATE -eq "RUNNING") {
                Write-Output "[INFO] Cluster is running, pipeline has been completed successfully"
                break
            } else {
                Write-Output "[INFO] Cluster is still not ready, current state: $STATE Next check in $RETRY_TIME seconds.."
                Start-Sleep -Seconds $RETRY_TIME
                if ($RETRY_COUNT -eq $RETRY_LIMIT) {
                    Write-Output "[ERROR] No more attempts left, breaking.."
                }
            }
        }    
    } else {
        Write-Output "[ERROR] Cluster was not created"
    }
}


if ($null -ne $DB_PAT) {

    Write-Output "Task: Uploading notebook"
    # Set the headers
    $headers = @{
        "Authorization" = "Bearer $DB_PAT"
        "Content-Type"  = "application/json"
    }
    
    # Create folder based on the syntax
    Write-Host "Create folder based on the syntax"
    try {
        $requestBodyFolder = @{
            "path" = "/Shared/$CTRL_SYNTAX"
        }
        $jsonBodyFolder = ConvertTo-Json -Depth 100 $requestBodyFolder

        if ($CTRL_SYNTAX -eq "DeltaLiveTable") {
            Invoke-RestMethod -Method POST -Uri "https://$WorkspaceUrl/api/2.0/workspace/mkdirs" -Headers $headers -Body $jsonBodyFolder    
        }
        else {
            Invoke-RestMethod -Method POST -Uri "https://$WorkspaceUrl/api/2.0/workspace/mkdirs" -Headers $headers -Body $jsonBodyFolder    
        }
        $mkdirDelta = $true
    }
    catch {
        $mkdirDelta = $false
        Write-Host "Error while calling the Databricks API for creating the $CTRL_SYNTAX folder. Folder not created"
        $errorMessage = $_.Exception.Message
        Write-Host "Error message: $errorMessage"    
    }    

    # Create folder for examples
    Write-Host "Create folder for examples"
    try {
        $requestBodyFolder = @{
            "path" = "/Shared/Example"
        }
        $jsonBodyFolder = ConvertTo-Json -Depth 100 $requestBodyFolder
        Invoke-RestMethod -Method POST -Uri "https://$WorkspaceUrl/api/2.0/workspace/mkdirs" -Headers $headers -Body $jsonBodyFolder
        $mkdirExample = $true
    }
    catch {
        $mkdirExample = $false
        Write-Host "Error while calling the Databricks API for creating the Example folder. Folder not created"
        $errorMessage = $_.Exception.Message
        Write-Host "Error message: $errorMessage"
    }  
    
    # Upload example notebooks to Example folder
    if ($mkdirExample) {
        Write-Host "Upload example notebooks"

        $Artifactsuri = "https://api.github.com/repos/DatabricksFactory/databricks-migration/contents/Artifacts/Example?ref=dev"
        Write-Host $Artifactsuri
        
        try {
            $wr = Invoke-WebRequest -Uri $Artifactsuri
            $objects = $wr.Content | ConvertFrom-Json
            $fileNames = $objects | where { $_.type -eq "file" } | Select -exp name
            Write-Host $fileNames
            $getExmpFilenames = $true
        }
        catch {
            $getExmpFilenames = $false
            Write-Host "Error while calling the GitHub API for getting the filenames under Artifacts/Example"
            $errorMessage = $_.Exception.Message
            Write-Host "Error message: $errorMessage"
        }        

        if ($getExmpFilenames) {
            Foreach ($filename in $fileNames) {
            
                try {
                    # Set the path to the notebook to be imported
                    $url = "$NOTEBOOK_PATH/Example/$filename"
                    
                    # Get the notebook
                    $Webresults = Invoke-WebRequest $url -UseBasicParsing
                    
                    # Read the notebook file
                    $notebookContent = $Webresults.Content
                    
                    # Base64 encode the notebook content
                    $notebookBase64 = [System.Convert]::ToBase64String([System.Text.Encoding]::UTF8.GetBytes($notebookContent))
                        
                    # Set the path
                    $splitfilename = $filename.Split(".")
                    $filenamewithoutextension = $splitfilename[0]
                    $path = "/Shared/Example/$filenamewithoutextension";
                    Write-Output $filenamewithoutextension
                    
                    # Set the request body
                    $requestBody = @{
                        "content"  = $notebookBase64
                        "path"     = $path
                        "language" = "PYTHON"
                        "format"   = "JUPYTER"
                    }
                    
                    # Convert the request body to JSON
                    $jsonBody = ConvertTo-Json -Depth 100 $requestBody
                }
                catch {
                    Write-Host "Error while reading the raw example notebook: $filename"
                    $errorMessage = $_.Exception.Message
                    Write-Host "Error message: $errorMessage"
                }
                
                try {
                    # Make the HTTP request to import the notebook
                    $response = Invoke-RestMethod -Method POST -Uri "https://$WorkspaceUrl/api/2.0/workspace/import" -Headers $headers -Body $jsonBody  
                    Write-Output $response
                }
                catch {
                    Write-Host "Error while calling the Azure Databricks API for importing example notebook: $filename"
                    $errorMessage = $_.Exception.Message
                    Write-Host "Error message: $errorMessage"
                }            
            }
        }    
    }    

    # Upload Silver and Gold Layer notebooks for a batch source to its respective synatx folder
    Write-Host "Upload Silver and Gold Layer notebooks for a batch source"
    if (!$SRC_EVENTHUB -and $mkdirDelta) {
        $Artifactsuri = "https://api.github.com/repos/DatabricksFactory/databricks-migration/contents/Artifacts/" + $CTRL_SYNTAX + "?ref=dev"
        Write-Host $Artifactsuri

        try {
            $wr = Invoke-WebRequest -Uri $Artifactsuri
            $objects = $wr.Content | ConvertFrom-Json
            $fileNames = $objects | where { $_.type -eq "file" } | Select -exp name
            Write-Host $fileNames
            $getSGFilenames = $true
        }
        catch {
            $getSGFilenames = $false
            Write-Host "Error while calling the GitHub API for getting the filenames under Artifacts/$CTRL_SYNTAX"
            $errorMessage = $_.Exception.Message
            Write-Host "Error message: $errorMessage"
        }
    
        if ($getSGFilenames) {
            Foreach ($filename in $fileNames) {
        
                try {
                    # Set the path to the notebook to be imported
                    $url = "$NOTEBOOK_PATH/$CTRL_SYNTAX/$filename"
                    
                    # Get the notebook
                    $Webresults = Invoke-WebRequest $url -UseBasicParsing
                    
                    # Read the notebook file
                    $notebookContent = $Webresults.Content
                    
                    # Base64 encode the notebook content
                    $notebookBase64 = [System.Convert]::ToBase64String([System.Text.Encoding]::UTF8.GetBytes($notebookContent))
                        
                    # Set the path
                    $splitfilename = $filename.Split(".")
                    $filenamewithoutextension = $splitfilename[0]
                    $path = "/Shared/$CTRL_SYNTAX/$filenamewithoutextension";
                    Write-Output $filenamewithoutextension
                    
                    # Set the request body
                    $requestBody = @{
                        "content"  = $notebookBase64
                        "path"     = $path
                        "language" = "PYTHON"
                        "format"   = "JUPYTER"
                    }
                    
                    # Convert the request body to JSON
                    $jsonBody = ConvertTo-Json -Depth 100 $requestBody
                }
                catch {
                    Write-Host "Error while reading the notebook: $filename"
                    $errorMessage = $_.Exception.Message
                    Write-Host "Error message: $errorMessage"
                }
                
                try {
                    # Make the HTTP request to import the notebook
                    $response = Invoke-RestMethod -Method POST -Uri "https://$WorkspaceUrl/api/2.0/workspace/import" -Headers $headers -Body $jsonBody  
                    Write-Output $response
                }
                catch {
                    Write-Host "Error while calling the Azure Databricks API for importing notebook: $filename"
                    $errorMessage = $_.Exception.Message
                    Write-Host "Error message: $errorMessage"
                }
            }
        }   
    }
    
    # FileSource
    if ($SRC_FILESOURCE -and $mkdirDelta) {
        
        # Get files under directory
        $Artifactsuri = "https://api.github.com/repos/DatabricksFactory/databricks-migration/contents/Artifacts/$CTRL_SYNTAX/Batch/FileSource?ref=dev"
        
        try {
            $wr = Invoke-WebRequest -Uri $Artifactsuri
            $objects = $wr.Content | ConvertFrom-Json
            $fileNames = $objects | where { $_.type -eq "file" } | Select -exp name
            $getFSFilename = $true
        }
        catch {
            $getFSFilename = $false
            Write-Host "Error while calling the GitHub API for getting the filenames under Artifacts/$CTRL_SYNTAX/Batch/FileSource"
            $errorMessage = $_.Exception.Message
            Write-Host "Error message: $errorMessage"
        }
    
        if ($getFSFilename) {
            Foreach ($filename in $fileNames) { 
        
                try {
                    # Set the path to the notebook to be imported
                    $url = "$NOTEBOOK_PATH/$CTRL_SYNTAX/Batch/FileSource/$filename"
            
                    # Get the notebook
                    $Webresults = Invoke-WebRequest $url -UseBasicParsing
            
                    # Read the notebook file
                    $notebookContent = $Webresults.Content
            
                    # Base64 encode the notebook content
                    $notebookBase64 = [System.Convert]::ToBase64String([System.Text.Encoding]::UTF8.GetBytes($notebookContent))
                    
                    # Set the path
                    $splitfilename = $filename.Split(".")
                    $filenamewithoutextension = $splitfilename[0]
                    $path = "/Shared/$CTRL_SYNTAX/$filenamewithoutextension";
                    Write-Output $filenamewithoutextension
            
                    # Set the request body
                    $requestBody = @{
                        "content"  = $notebookBase64
                        "path"     = $path
                        "language" = "PYTHON"
                        "format"   = "JUPYTER"
                    }
            
                    # Convert the request body to JSON
                    $jsonBody = ConvertTo-Json -Depth 100 $requestBody
                }
                catch {
                    Write-Host "Error while reading the notebook: $filename"
                    $errorMessage = $_.Exception.Message
                    Write-Host "Error message: $errorMessage"
                }
        
                try {
                    # Make the HTTP request to import the notebook
                    $response = Invoke-RestMethod -Method POST -Uri "https://$WorkspaceUrl/api/2.0/workspace/import" -Headers $headers -Body $jsonBody  
                    Write-Output $response
                }
                catch {
                    Write-Host "Error while calling the Azure Databricks API for importing notebook: $filename"
                    $errorMessage = $_.Exception.Message
                    Write-Host "Error message: $errorMessage"
                }
            } 
        }
    }
    
    # Azure SQL 
    if ($SRC_AZSQL -and $mkdirDelta) {
        
        # Get files under directory
        $Artifactsuri = "https://api.github.com/repos/DatabricksFactory/databricks-migration/contents/Artifacts/$CTRL_SYNTAX/Batch/AzureSQLDb?ref=dev"
        
        try {
            $wr = Invoke-WebRequest -Uri $Artifactsuri
            $objects = $wr.Content | ConvertFrom-Json
            $fileNames = $objects | where { $_.type -eq "file" } | Select -exp name
            $getAZSQLFilenames = $true
        }
        catch {
            $getAZSQLFilenames = $false
            Write-Host "Error while calling the GitHub API for getting the filenames under Artifacts/$CTRL_SYNTAX/Batch/AzureSQLDb"
            $errorMessage = $_.Exception.Message
            Write-Host "Error message: $errorMessage"
        }
    
        if ($getAZSQLFilenames) {
            Foreach ($filename in $fileNames) { 
        
                try {
                    # Set the path to the notebook to be imported
                    $url = "$NOTEBOOK_PATH/$CTRL_SYNTAX/Batch/AzureSQLDb/$filename"
            
                    # Get the notebook
                    $Webresults = Invoke-WebRequest $url -UseBasicParsing
            
                    # Read the notebook file
                    $notebookContent = $Webresults.Content
            
                    # Base64 encode the notebook content
                    $notebookBase64 = [System.Convert]::ToBase64String([System.Text.Encoding]::UTF8.GetBytes($notebookContent))
                    
                    # Set the path
                    $splitfilename = $filename.Split(".")
                    $filenamewithoutextension = $splitfilename[0]
                    $path = "/Shared/$CTRL_SYNTAX/$filenamewithoutextension";
                    Write-Output $filenamewithoutextension
            
                    # Set the request body
                    $requestBody = @{
                        "content"  = $notebookBase64
                        "path"     = $path
                        "language" = "PYTHON"
                        "format"   = "JUPYTER"
                    }
            
                    # Convert the request body to JSON
                    $jsonBody = ConvertTo-Json -Depth 100 $requestBody
                }
                catch {
                    Write-Host "Error while reading the notebook: $filename"
                    $errorMessage = $_.Exception.Message
                    Write-Host "Error message: $errorMessage"
                }
        
                try {
                    # Make the HTTP request to import the notebook
                    $response = Invoke-RestMethod -Method POST -Uri "https://$WorkspaceUrl/api/2.0/workspace/import" -Headers $headers -Body $jsonBody  
                    Write-Output $response
                }
                catch {
                    Write-Host "Error while calling the Azure Databricks API for importing notebook: $filename"
                    $errorMessage = $_.Exception.Message
                    Write-Host "Error message: $errorMessage"
                }
            } 
        }
    }
    
    # Azure MySQL
    if ($SRC_AZMYSQL -and $mkdirDelta) {
        
        # Get files under directory
        $Artifactsuri = "https://api.github.com/repos/DatabricksFactory/databricks-migration/contents/Artifacts/$CTRL_SYNTAX/Batch/AzureMySQL?ref=dev"
        
        try {
            $wr = Invoke-WebRequest -Uri $Artifactsuri
            $objects = $wr.Content | ConvertFrom-Json
            $fileNames = $objects | where { $_.type -eq "file" } | Select -exp name
            $getAZMYSQLFilenames = $true
        }
        catch {
            $getAZMYSQLFilenames = $false
            Write-Host "Error while calling the GitHub API for getting the filenames under Artifacts/$CTRL_SYNTAX/Batch/AzureMySQL"
            $errorMessage = $_.Exception.Message
            Write-Host "Error message: $errorMessage"
        }
    
        if ($getAZMYSQLFilenames) {
            Foreach ($filename in $fileNames) { 
        
                try {
                    # Set the path to the notebook to be imported
                    $url = "$NOTEBOOK_PATH/$CTRL_SYNTAX/Batch/AzureMySQL/$filename"
            
                    # Get the notebook
                    $Webresults = Invoke-WebRequest $url -UseBasicParsing
            
                    # Read the notebook file
                    $notebookContent = $Webresults.Content
            
                    # Base64 encode the notebook content
                    $notebookBase64 = [System.Convert]::ToBase64String([System.Text.Encoding]::UTF8.GetBytes($notebookContent))
                    
                    # Set the path
                    $splitfilename = $filename.Split(".")
                    $filenamewithoutextension = $splitfilename[0]
                    $path = "/Shared/$CTRL_SYNTAX/$filenamewithoutextension";
                    Write-Output $filenamewithoutextension
            
                    # Set the request body
                    $requestBody = @{
                        "content"  = $notebookBase64
                        "path"     = $path
                        "language" = "PYTHON"
                        "format"   = "JUPYTER"
                    }
            
                    # Convert the request body to JSON
                    $jsonBody = ConvertTo-Json -Depth 100 $requestBody
                }
                catch {
                    Write-Host "Error while reading the notebook: $filename"
                    $errorMessage = $_.Exception.Message
                    Write-Host "Error message: $errorMessage"
                }
        
                try {
                    # Make the HTTP request to import the notebook
                    $response = Invoke-RestMethod -Method POST -Uri "https://$WorkspaceUrl/api/2.0/workspace/import" -Headers $headers -Body $jsonBody  
                    Write-Output $response
                }
                catch {
                    Write-Host "Error while calling the Azure Databricks API for importing notebook: $filename"
                    $errorMessage = $_.Exception.Message
                    Write-Host "Error message: $errorMessage"
                }
            } 
        }
    }
    
    # Azure PSQL
    if ($SRC_AZPSQL -and $mkdirDelta) {
        
        # Get files under directory
        $Artifactsuri = "https://api.github.com/repos/DatabricksFactory/databricks-migration/contents/Artifacts/$CTRL_SYNTAX/Batch/AzurePostgreSQL?ref=dev"
        
        try {
            $wr = Invoke-WebRequest -Uri $Artifactsuri
            $objects = $wr.Content | ConvertFrom-Json
            $fileNames = $objects | where { $_.type -eq "file" } | Select -exp name
            $getAZPSQLFilename = $true
        }
        catch {
            $getAZPSQLFilename = $false
            Write-Host "Error while calling the GitHub API for getting the filenames under Artifacts/$CTRL_SYNTAX/Batch/AzurePostgreSQL"
            $errorMessage = $_.Exception.Message
            Write-Host "Error message: $errorMessage"
        }
    
        if ($getAZPSQLFilename) {
            Foreach ($filename in $fileNames) { 
        
                try {
                    # Set the path to the notebook to be imported
                    $url = "$NOTEBOOK_PATH/$CTRL_SYNTAX/Batch/AzurePostgreSQL/$filename"
            
                    # Get the notebook
                    $Webresults = Invoke-WebRequest $url -UseBasicParsing
            
                    # Read the notebook file
                    $notebookContent = $Webresults.Content
            
                    # Base64 encode the notebook content
                    $notebookBase64 = [System.Convert]::ToBase64String([System.Text.Encoding]::UTF8.GetBytes($notebookContent))
                    
                    # Set the path
                    $splitfilename = $filename.Split(".")
                    $filenamewithoutextension = $splitfilename[0]
                    $path = "/Shared/$CTRL_SYNTAX/$filenamewithoutextension";
                    Write-Output $filenamewithoutextension
            
                    # Set the request body
                    $requestBody = @{
                        "content"  = $notebookBase64
                        "path"     = $path
                        "language" = "PYTHON"
                        "format"   = "JUPYTER"
                    }
            
                    # Convert the request body to JSON
                    $jsonBody = ConvertTo-Json -Depth 100 $requestBody
                }
                catch {
                    Write-Host "Error while reading the notebook: $filename"
                    $errorMessage = $_.Exception.Message
                    Write-Host "Error message: $errorMessage"
                }
        
                try {
                    # Make the HTTP request to import the notebook
                    $response = Invoke-RestMethod -Method POST -Uri "https://$WorkspaceUrl/api/2.0/workspace/import" -Headers $headers -Body $jsonBody  
                    Write-Output $response
                }
                catch {
                    Write-Host "Error while calling the Azure Databricks API for importing notebook: $filename"
                    $errorMessage = $_.Exception.Message
                    Write-Host "Error message: $errorMessage"
                }
            } 
        }
    }
    
    # SQL on-prem
    if ($SRC_SQL_ONPREM -and $mkdirDelta) {
        
        # Get files under directory
        $Artifactsuri = "https://api.github.com/repos/DatabricksFactory/databricks-migration/contents/Artifacts/$CTRL_SYNTAX/Batch/SQLDbOnPrem?ref=dev"
        
        try {
            $wr = Invoke-WebRequest -Uri $Artifactsuri
            $objects = $wr.Content | ConvertFrom-Json
            $fileNames = $objects | where { $_.type -eq "file" } | Select -exp name
            $getSQLOPFilenames = $true
        }
        catch {
            $getSQLOPFilenames = $false
            Write-Host "Error while calling the GitHub API for getting the filenames under Artifacts/$CTRL_SYNTAX/Batch/SQLDbOnPrem"
            $errorMessage = $_.Exception.Message
            Write-Host "Error message: $errorMessage"
        }
    
        if ($getSQLOPFilenames) {
            Foreach ($filename in $fileNames) { 
        
                try {
                    # Set the path to the notebook to be imported
                    $url = "$NOTEBOOK_PATH/$CTRL_SYNTAX/Batch/SQLDbOnPrem/$filename"
            
                    # Get the notebook
                    $Webresults = Invoke-WebRequest $url -UseBasicParsing
            
                    # Read the notebook file
                    $notebookContent = $Webresults.Content
            
                    # Base64 encode the notebook content
                    $notebookBase64 = [System.Convert]::ToBase64String([System.Text.Encoding]::UTF8.GetBytes($notebookContent))
                    
                    # Set the path
                    $splitfilename = $filename.Split(".")
                    $filenamewithoutextension = $splitfilename[0]
                    $path = "/Shared/$CTRL_SYNTAX/$filenamewithoutextension";
                    Write-Output $filenamewithoutextension
            
                    # Set the request body
                    $requestBody = @{
                        "content"  = $notebookBase64
                        "path"     = $path
                        "language" = "PYTHON"
                        "format"   = "JUPYTER"
                    }
            
                    # Convert the request body to JSON
                    $jsonBody = ConvertTo-Json -Depth 100 $requestBody
        
                }
                catch {
                    Write-Host "Error while reading the notebook: $filename"
                        $errorMessage = $_.Exception.Message
                        Write-Host "Error message: $errorMessage"
                }    
    
                try {
                    # Make the HTTP request to import the notebook
                    $response = Invoke-RestMethod -Method POST -Uri "https://$WorkspaceUrl/api/2.0/workspace/import" -Headers $headers -Body $jsonBody  
                    Write-Output $response
                }
                catch {
                    Write-Host "Error while calling the Azure Databricks API for importing notebook: $filename"
                    $errorMessage = $_.Exception.Message
                    Write-Host "Error message: $errorMessage"
                }
            }
        } 
    }
    
    # PSQL on-prem
    if ($SRC_PSQL_ONPREM -and $mkdirDelta) {
        
        # Get files under directory
        $Artifactsuri = "https://api.github.com/repos/DatabricksFactory/databricks-migration/contents/Artifacts/$CTRL_SYNTAX/Batch/PostgreSQL?ref=dev"
        
        try {
            $wr = Invoke-WebRequest -Uri $Artifactsuri
            $objects = $wr.Content | ConvertFrom-Json
            $fileNames = $objects | where { $_.type -eq "file" } | Select -exp name
            $getPSQLOPFilename = $true
        }
        catch {
            $getPSQLOPFilename = $false
            Write-Host "Error while calling the GitHub API for getting the filenames under Artifacts/$CTRL_SYNTAX/Batch/PostgreSQL"
            $errorMessage = $_.Exception.Message
            Write-Host "Error message: $errorMessage"
        }
    
        if ($getPSQLOPFilename) {
            Foreach ($filename in $fileNames) { 
        
                try {
                    # Set the path to the notebook to be imported
                    $url = "$NOTEBOOK_PATH/$CTRL_SYNTAX/Batch/PostgreSQL/$filename"
            
                    # Get the notebook
                    $Webresults = Invoke-WebRequest $url -UseBasicParsing
            
                    # Read the notebook file
                    $notebookContent = $Webresults.Content
            
                    # Base64 encode the notebook content
                    $notebookBase64 = [System.Convert]::ToBase64String([System.Text.Encoding]::UTF8.GetBytes($notebookContent))
                    
                    # Set the path
                    $splitfilename = $filename.Split(".")
                    $filenamewithoutextension = $splitfilename[0]
                    $path = "/Shared/$CTRL_SYNTAX/$filenamewithoutextension";
                    Write-Output $filenamewithoutextension
            
                    # Set the request body
                    $requestBody = @{
                        "content"  = $notebookBase64
                        "path"     = $path
                        "language" = "PYTHON"
                        "format"   = "JUPYTER"
                    }
            
                    # Convert the request body to JSON
                    $jsonBody = ConvertTo-Json -Depth 100 $requestBody
                }
                catch {
                    Write-Host "Error while reading the notebook: $filename"
                    $errorMessage = $_.Exception.Message
                    Write-Host "Error message: $errorMessage"
                }
        
                try {
                    # Make the HTTP request to import the notebook
                    $response = Invoke-RestMethod -Method POST -Uri "https://$WorkspaceUrl/api/2.0/workspace/import" -Headers $headers -Body $jsonBody  
                    Write-Output $response
                }
                catch {
                    Write-Host "Error while calling the Azure Databricks API for importing notebook: $filename"
                    $errorMessage = $_.Exception.Message
                    Write-Host "Error message: $errorMessage"
                }
            }
        } 
    }


    # Oracle
    if ($SRC_ORACLE) {
        # Get files under directory
        $Artifactsuri = "https://api.github.com/repos/DatabricksFactory/databricks-migration/contents/Artifacts/$CTRL_SYNTAX/Batch/Oracle?ref=dev"

        try {
            $wr = Invoke-WebRequest -Uri $Artifactsuri
            $objects = $wr.Content | ConvertFrom-Json
            $fileNames = $objects | where { $_.type -eq "file" } | Select -exp name
            $getOrclFilename = $true
        }
        catch {
            $getOrclFilename = $false
            Write-Host "Error while calling the GitHub API for getting the filenames under Artifacts/$CTRL_SYNTAX/Batch/Oracle"
            $errorMessage = $_.Exception.Message
            Write-Host "Error message: $errorMessage"
        }

        if ($getOrclFilename) {
            Foreach ($filename in $fileNames) { 

                try {
                    # Set the path to the notebook to be imported
                    $url = "$NOTEBOOK_PATH/$CTRL_SYNTAX/Batch/Oracle/$filename"

                    # Get the notebook
                    $Webresults = Invoke-WebRequest $url -UseBasicParsing

                    # Read the notebook file
                    $notebookContent = $Webresults.Content

                    # Base64 encode the notebook content
                    $notebookBase64 = [System.Convert]::ToBase64String([System.Text.Encoding]::UTF8.GetBytes($notebookContent))

                    # Set the path
                    $splitfilename = $filename.Split(".")
                    $filenamewithoutextension = $splitfilename[0]
                    $path = "/Shared/$CTRL_SYNTAX/$filenamewithoutextension";
                    Write-Output $filenamewithoutextension

                    # Set the request body
                    $requestBody = @{
                        "content"  = $notebookBase64
                        "path"     = $path
                        "language" = "PYTHON"
                        "format"   = "JUPYTER"
                    }

                    # Convert the request body to JSON
                    $jsonBody = ConvertTo-Json -Depth 100 $requestBody
                }
                catch {
                    Write-Host "Error while reading the notebook: $filename"
                    $errorMessage = $_.Exception.Message
                    Write-Host "Error message: $errorMessage"
                }

                try {
                    # Make the HTTP request to import the notebook
                    $response = Invoke-RestMethod -Method POST -Uri "https://$WorkspaceUrl/api/2.0/workspace/import" -Headers $headers -Body $jsonBody  
                    Write-Output $response
                }
                catch {
                    Write-Host "Error while calling the Azure Databricks API for importing notebook: $filename"
                    $errorMessage = $_.Exception.Message
                    Write-Host "Error message: $errorMessage"
                }
            }
        }         
    }
    
    # EventHub
    if ($SRC_EVENTHUB -and $mkdirDelta) {
        
        # Get files under directory
        $Artifactsuri = "https://api.github.com/repos/DatabricksFactory/databricks-migration/contents/Artifacts/$CTRL_SYNTAX/Stream/EventHub?ref=dev"
        
        try {
            $wr = Invoke-WebRequest -Uri $Artifactsuri
            $objects = $wr.Content | ConvertFrom-Json
            $fileNames = $objects | where { $_.type -eq "file" } | Select -exp name
            $getEHFilenames = $true
        }
        catch {
            $getEHFilenames = $false
            Write-Host "Error while calling the GitHub API for getting the filenames under Artifacts/$CTRL_SYNTAX/Stream/EventHub"
            $errorMessage = $_.Exception.Message
            Write-Host "Error message: $errorMessage"
        }
    
        if ($getEHFilenames) {
            Foreach ($filename in $fileNames) { 
        
                try {
                    # Set the path to the notebook to be imported
                    $url = "$NOTEBOOK_PATH/$CTRL_SYNTAX/Stream/EventHub/$filename"
            
                    # Get the notebook
                    $Webresults = Invoke-WebRequest $url -UseBasicParsing
            
                    # Read the notebook file
                    $notebookContent = $Webresults.Content
            
                    # Base64 encode the notebook content
                    $notebookBase64 = [System.Convert]::ToBase64String([System.Text.Encoding]::UTF8.GetBytes($notebookContent))
                    
                    # Set the path
                    $splitfilename = $filename.Split(".")
                    $filenamewithoutextension = $splitfilename[0]
                    $path = "/Shared/$CTRL_SYNTAX/$filenamewithoutextension";
                    Write-Output $filenamewithoutextension
            
                    # Set the request body
                    $requestBody = @{
                        "content"  = $notebookBase64
                        "path"     = $path
                        "language" = "PYTHON"
                        "format"   = "JUPYTER"
                    }
            
                    # Convert the request body to JSON
                    $jsonBody = ConvertTo-Json -Depth 100 $requestBody
                }
                catch {
                    Write-Host "Error while reading the notebook: $filename"
                    $errorMessage = $_.Exception.Message
                    Write-Host "Error message: $errorMessage"
                }
        
                try {
                    # Make the HTTP request to import the notebook
                    $response = Invoke-RestMethod -Method POST -Uri "https://$WorkspaceUrl/api/2.0/workspace/import" -Headers $headers -Body $jsonBody  
                    Write-Output $response
                }
                catch {
                    Write-Host "Error while calling the Azure Databricks API for importing notebook: $filename"
                    $errorMessage = $_.Exception.Message
                    Write-Host "Error message: $errorMessage"
                }
            } 
        }
    }
}

# Deploy pipeline
if ($CTRL_DEPLOY_PIPELINE -and ($null -ne $DB_PAT)) {
    Write-Host "Deploy pipeline"

    $headers = @{Authorization = "Bearer $DB_PAT" }

    $pipeline_notebook_path = '/Shared/dlt/azure_sql_db'

    # Create a pipeline
    $pipelineConfig = @{
        name = $PIPELINENAME
        storage = $STORAGE
        target = $TARGETSCHEMA
        clusters = @{
            label = 'default'
            autoscale = @{
                min_workers = $MINWORKERS
                max_workers = $MAXWORKERS
                mode = 'ENHANCED'
            }
        }
        libraries = @{
            notebook = @{
                path = $pipeline_notebook_path
            }
        }
        continuous = 'false'
        allow_duplicate_names = 'true' 
    }

    try {
        $createPipelineResponse = Invoke-RestMethod -Uri "https://$WorkspaceUrl/api/2.0/pipelines" -Method POST -Headers $headers -Body ($pipelineConfig | ConvertTo-Json -Depth 10)
        $createPipelineResponse
    }
    catch {
        Write-Host "Error while calling the Azure Databricks API for creating the pipeline"
        $errorMessage = $_.Exception.Message
        Write-Host "Error message: $errorMessage"
    }
}

# Upload data files
if ($SA_EXISTS) {
    try {
        $storageaccountkey = Get-AzStorageAccountKey -ResourceGroupName $RG_NAME -Name $SA_NAME;
    }
    catch {
        Write-Host "Error while getting Storage Account Key"
        $errorMessage = $_.Exception.Message
        Write-Host "Error message: $errorMessage"
    }
    
    try {
        $ctx = New-AzStorageContext -StorageAccountName $SA_NAME -StorageAccountKey $storageaccountkey.Value[0]
    }
    catch {
        Write-Host "Error while creating Azure Storage context"
        $errorMessage = $_.Exception.Message
        Write-Host "Error message: $errorMessage"
    }
    
    $Artifactsuri = "https://api.github.com/repos/DatabricksFactory/databricks-migration/contents/data?ref=dev" #change to main branch
    
    try {
        $wr = Invoke-WebRequest -Uri $Artifactsuri
        
        $objects = $wr.Content | ConvertFrom-Json
        
        $fileNames = $objects | where { $_.type -eq "file" } | Select -exp name
        
        Write-Host $fileNames

        $getCsvFilenames = $true
    }
    catch {
        $getCsvFilenames = $false
        Write-Host "Error while calling the GitHub API to get filenames under /data"
        $errorMessage = $_.Exception.Message
        Write-Host "Error message: $errorMessage"
    }
    
    if ($getCsvFilenames) {
        Foreach ($filename in $fileNames) {
            try {
                $url = "https://raw.githubusercontent.com/DatabricksFactory/databricks-migration/dev/data/$filename" #change to main branch
            
                $Webresults = Invoke-WebRequest $url -UseBasicParsing
            
                Invoke-WebRequest -Uri $url -OutFile $filename
            
                Set-AzStorageBlobContent -File $filename -Container "data" -Blob $filename -Context $ctx
            }
            catch {
                Write-Host "Error while uploading data file: $filename"
                $errorMessage = $_.Exception.Message
                Write-Host "Error message: $errorMessage"
            }
        }
    }
}
