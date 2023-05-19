param(
    [string] $RG_NAME,
    [string] $REGION,
    [string] $WORKSPACE_NAME,
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
    [string] $NOTEBOOK_PATH
)

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
    
if ($CTRL_DEPLOY_CLUSTER) {
        
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
        } else {
            Write-Output "[INFO] Cluster is still not ready, current state: $STATE Next check in $RETRY_TIME seconds.."
            Start-Sleep -Seconds $RETRY_TIME
        }
    }
    Write-Output "[ERROR] No more attempts left, breaking.."
}

if ($CTRL_DEPLOY_NOTEBOOK) {

Write-Output "Task: Uploading notebook"

$requestBodyFolder = @{
  
  "path" = "/Shared/Templates"
 
}

$jsonBodyFolder = ConvertTo-Json -Depth 100 $requestBodyFolder

# Set the headers
$headers = @{
  "Authorization" = "Bearer $DB_PAT"
  "Content-Type" = "application/json"
}

$responseFolder = Invoke-RestMethod -Method POST -Uri "https://$REGION.azuredatabricks.net/api/2.0/workspace/mkdirs" -Headers $headers -Body $jsonBodyFolder
 
 $Artifactsuri = "https://api.github.com/repos/DatabricksFactory/databricks-migration/contents/Artifacts"
 $wr = Invoke-WebRequest -Uri $Artifactsuri
 $objects = $wr.Content | ConvertFrom-Json
 $filesURL = $objects | where {$_.type -eq "file"} | Select -exp download_url
 $fileNames = $objects | where {$_.type -eq "file"} | Select -exp name

#Set the path to the notebook to be imported

Foreach($filename in $fileNames)  
{ 

# Set the path to the notebook to be imported
$url = "$NOTEBOOK_PATH/$filename"
$Webresults = Invoke-WebRequest $url -UseBasicParsing
# Read the notebook file
$notebookContent = $Webresults.Content
# Base64 encode the notebook content
$notebookBase64 = [System.Convert]::ToBase64String([System.Text.Encoding]::UTF8.GetBytes($notebookContent))
$splitfilename =$filename.Split(".")
$filenamewithoutextension = $splitfilename[0]

Write-Output $filenamewithoutextension

# Set the request body
$requestBody = @{
  "content" = $notebookBase64
  "path" = "/Shared/Templates/$filenamewithoutextension"
  "language" = "PYTHON"
  "format" = "JUPYTER"
}

# Convert the request body to JSON
$jsonBody = ConvertTo-Json -Depth 100 $requestBody

   # Make the HTTP request to import the notebook
   $response = Invoke-RestMethod -Method POST -Uri "https://$REGION.azuredatabricks.net/api/2.0/workspace/import" -Headers $headers -Body $jsonBody  

   Write-Output $response
  } 

}

if ($CTRL_DEPLOY_PIPELINE) {

  $headers = @{Authorization = "Bearer $DB_PAT"}

  $pipeline_notebook_path = '/Shared/Templates/02_silver-layer-notebook'

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

$createPipelineResponse = Invoke-RestMethod -Uri "https://$REGION.azuredatabricks.net/api/2.0/pipelines" -Method POST -Headers $headers -Body ($pipelineConfig | ConvertTo-Json -Depth 10)
$createPipelineResponse

}