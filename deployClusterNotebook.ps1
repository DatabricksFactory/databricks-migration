param(
    [string] $RG_NAME,
    [string] $REGION,
    [string] $WORKSPACE_NAME,
    [string] $CTRL_DEPLOY_CLUSTER,
    [string] $CTRL_DEPLOY_NOTEBOOK,
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
    [Parameter(Mandatory = $true)]
    [string]$tenant_id,
    [Parameter(Mandatory = $true)]
    [string]$client_id,
    [Parameter(Mandatory = $true)]
    [string]$client_secret,
    [Parameter(Mandatory = $true)]
    [string]$subscription_id,
    [Parameter(Mandatory = $true)]
    [string]$notebookPathUnderWorkspace
)

if ($CTRL_DEPLOY_NOTEBOOK -eq '$true') {
    $azure_databricks_resource_id = "2ff814a6-3304-4ab8-85cb-cd0e6f879c1d"
    $resourceId = "/subscriptions/$subscription_id/resourceGroups/$RG_NAME/providers/Microsoft.Databricks/workspaces/$WORKSPACE_NAME"
    
    ######################################################################################
    # Get access tokens for Databricks API
    ######################################################################################
    
    $accessToken = (Invoke-WebRequest -Method POST -Uri "https://login.microsoftonline.com/$tenant_id/oauth2/token" `
            -Body @{
            resource      = $azure_databricks_resource_id
            client_id     = $client_id
            grant_type    = "client_credentials"
            client_secret = $client_secret
        } `
            -UseBasicParsing `
        | ConvertFrom-Json).access_token
    
    $managementToken = (Invoke-WebRequest -Method POST -Uri "https://login.microsoftonline.com/$tenant_id/oauth2/token" `
            -Body @{
            resource      = "https://management.core.windows.net/"
            client_id     = $client_id
            grant_type    = "client_credentials"
            client_secret = $client_secret
        } `
            -UseBasicParsing `
        | ConvertFrom-Json).access_token
    
    ######################################################################################
    # Get Databricks workspace URL (e.g. adb-5946405904802522.2.azuredatabricks.net)
    ######################################################################################
    
    $workspaceUrl = (Invoke-WebRequest -Method GET -Uri "https://management.azure.com/subscriptions/$subscription_id/resourcegroups/$RG_NAME/providers/Microsoft.Databricks/workspaces/$WORKSPACE_NAME?api-version=2018-04-01" `
            -Headers @{
            "Content-Type"  = "application/json"
            "Authorization" = "Bearer $managementToken"
        } `
        | ConvertFrom-Json).properties.workspaceUrl
    
    Write-Host "Databricks workspaceUrl: $workspaceUrl"
    
    ######################################################################################
    # Recusively Create Paths 
    ######################################################################################
    
    $replaceSource = "./"
    $replaceDest = ""
    
    Get-ChildItem -Path . -Recurse -Directory | ForEach-Object {
        Write-Host "Processing directory: $($_.FullName)"
        $directoryName = $_.FullName.Replace($replaceSource, $replaceDest)
        Write-Host "New directoryName: $directoryName"
    
        if ($_.FullName -eq ".") {
            $pathOnDatabricks = $notebookPathUnderWorkspace
        }
        else {
            $pathOnDatabricks = "$notebookPathUnderWorkspace/$directoryName"
        }
        Write-Host "pathOnDatabricks: $pathOnDatabricks"
    
        $JSON = @{ path = $pathOnDatabricks } | ConvertTo-Json
        Write-Host "Creating Path: $JSON"
    
        Invoke-WebRequest -Method POST -Uri "https://$workspaceUrl/api/2.0/workspace/mkdirs" `
            -Headers @{
            "Authorization"                            = "Bearer $accessToken"
            "X-Databricks-Azure-SP-Management-Token"   = $managementToken
            "X-Databricks-Azure-Workspace-Resource-Id" = $resourceId
            "Content-Type"                             = "application/json"
        } `
            -Body $JSON
    }
    
    ######################################################################################
    # Deploy notebooks (resursively)
    ######################################################################################
    
    Get-ChildItem -Path . -Recurse -File | ForEach-Object {
        Write-Host "Processing file: $($_.FullName)"
        $filename = $_.FullName.Replace($replaceSource, $replaceDest)
        Write-Host "New filename: $filename"
    
        $language = ""
        if ($filename -like "*.sql") {
            $language = "SQL"
        }
    
        if ($filename -like "*.scala") {
            $language = "SCALA"
        }
    
        if ($filename -like "*.py") {
            $language = "PYTHON"
        }
    
        if ($filename -like "*.r") {
            $language = "R"
        }
    
        Write-Host "curl -F language=$language -F path=$notebookPathUnderWorkspace/$filename -F content=@$($_.FullName) https://$workspaceUrl/api/2.0/workspace/import"
    
        Invoke-WebRequest -Method POST -Uri "https://$workspaceUrl/api/2.0/workspace/import" `
            -Headers @{
            "Authorization"                            = "Bearer $accessToken"
            "X-Databricks-Azure-SP-Management-Token"   = $managementToken
            "X-Databricks-Azure-Workspace-Resource-Id" = $resourceId
        } `
            -ContentType "multipart/form-data" `
            -Body @{
            language  = $language
            overwrite = $true
            path      = "$notebookPathUnderWorkspace/$filename"
            content   = Get-Content -Path $_.FullName -Raw
        }
    }   
}

if ($CTRL_DEPLOY_CLUSTER -eq '$true') {
    Write-Output "Task: Generating Databricks Token"

    $WORKSPACE_ID = Get-AzResource -ResourceType Microsoft.Databricks/workspaces -ResourceGroupName $RG_NAME -Name $WORKSPACE_NAME
    $ACTUAL_WORKSPACE_ID = $WORKSPACE_ID.ResourceId
    $token = (Get-AzAccessToken -Resource '2ff814a6-3304-4ab8-85cb-cd0e6f879c1d').Token
    $AZ_TOKEN = (Get-AzAccessToken -ResourceUrl 'https://management.core.windows.net/').Token
    $HEADERS = @{
        "Authorization"                            = "Bearer $TOKEN"
        "X-Databricks-Azure-SP-Management-Token"   = "$AZ_TOKEN"
        "X-Databricks-Azure-Workspace-Resource-Id" = "$ACTUAL_WORKSPACE_ID"
    }
    $BODY = @"
    { "lifetime_seconds": $LIFETIME_SECONDS, "comment": "$COMMENT" }
"@
    $DB_PAT = ((Invoke-RestMethod -Method POST -Uri "https://$REGION.azuredatabricks.net/api/2.0/token/create" -Headers $HEADERS -Body $BODY).token_value)
    
    
    Write-Output "Task: Creating cluster"
    $HEADERS = @{
        "Authorization" = "Bearer $DB_PAT"
        "Content-Type" = "application/json"
    }
    $BODY = @"
    {"cluster_name": "$CLUSTER_NAME", "spark_version": "$SPARK_VERSION", "autotermination_minutes": $AUTOTERMINATION_MINUTES, "num_workers": "$NUM_WORKERS", "node_type_id": "$NODE_TYPE_ID", "driver_node_type_id": "$DRIVER_NODE_TYPE_ID" }
"@
    $CLUSTER_ID = ((Invoke-RestMethod -Method POST -Uri "https://$REGION.azuredatabricks.net/api/2.0/clusters/create" -Headers $HEADERS -Body $BODY).cluster_id)
    if ( $CLUSTER_ID -ne "null" ) {
        Write-Output "[INFO] CLUSTER_ID: $CLUSTER_ID"
    } else {
        Write-Output "[ERROR] cluster was not created"
        exit 1
    }
    
    Write-Output "Task: Checking cluster"
    $RETRY_COUNT = 0
    for( $RETRY_COUNT = 1; $RETRY_COUNT -le $RETRY_LIMIT; $RETRY_COUNT++ ) {
        Write-Output "[INFO] Attempt $RETRY_COUNT of $RETRY_LIMIT"
        $HEADERS = @{
            "Authorization" = "Bearer $DB_PAT"
        }
        $STATE = ((Invoke-RestMethod -Method GET -Uri "https://$REGION.azuredatabricks.net/api/2.0/clusters/get?cluster_id=$CLUSTER_ID" -Headers $HEADERS).state)
        if ($STATE -eq "RUNNING") {
            Write-Output "[INFO] Cluster is running, pipeline has been completed successfully"
            return
        } else {
            Write-Output "[INFO] Cluster is still not ready, current state: $STATE Next check in $RETRY_TIME seconds.."
            Start-Sleep -Seconds $RETRY_TIME
        }
    }
    Write-Output "[ERROR] No more attempts left, breaking.."
    exit 1  
}