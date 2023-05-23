param(
    [string] $RG_NAME,
    [string] $REGION,
    [int] $LIFETIME_SECONDS,
    [string] $COMMENT,
    [bool] $CTRL_DEPLOY_NOTEBOOK,
    [bool] $CTRL_DEPLOY_PIPELINE,
    [string] $PIPELINENAME,
    [string] $STORAGE,
    [string] $TARGETSCHEMA,
    [int] $MINWORKERS,
    [int] $MAXWORKERS,
    [string] $NOTEBOOK_PATH
)
Write-Output "Task: Generating Databricks Token"

    $WORKSPACE_ID = Get-AzResource -ResourceType Microsoft.Databricks/workspaces -ResourceGroupName $RG_NAME
    $ACTUAL_WORKSPACE_ID = $WORKSPACE_ID.ResourceId
    
    Write-Output $ACTUAL_WORKSPACE_ID
    
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

Write-Output $DB_PAT

if ($CTRL_DEPLOY_NOTEBOOK) {

Write-Output "Task: Uploading notebook"

# Set the headers
$headers = @{
  "Authorization" = "Bearer $DB_PAT"
  "Content-Type" = "application/json"
}


# 1st Folder structure

$requestBodyFolder1 = @{
  
  "path" = "/Shared/dlt/filesource/batch"
 
}

$jsonBodyFolder1 = ConvertTo-Json -Depth 100 $requestBodyFolder1

$responseFolder1 = Invoke-RestMethod -Method POST -Uri "https://eastus.azuredatabricks.net/api/2.0/workspace/mkdirs" -Headers $headers -Body $jsonBodyFolder1


# 2nd Folder structure

$requestBodyFolder2 = @{
  
  "path" = "/Shared/dlt/azuresqldbsource/batch"
 
}

$jsonBodyFolder2 = ConvertTo-Json -Depth 100 $requestBodyFolder2

$responseFolder2 = Invoke-RestMethod -Method POST -Uri "https://eastus.azuredatabricks.net/api/2.0/workspace/mkdirs" -Headers $headers -Body $jsonBodyFolder2

# 3rd Folder structure

$requestBodyFolder3 = @{
  
  "path" = "/Shared/dlt/eventhub/stream"
 
}

$jsonBodyFolder3 = ConvertTo-Json -Depth 100 $requestBodyFolder3

$responseFolder3 = Invoke-RestMethod -Method POST -Uri "https://eastus.azuredatabricks.net/api/2.0/workspace/mkdirs" -Headers $headers -Body $jsonBodyFolder3


# 4th Folder structure

$requestBodyFolder4 = @{
  
  "path" = "/Shared/dlt/postgresdbsource/batch"
 
}

$jsonBodyFolder4 = ConvertTo-Json -Depth 100 $requestBodyFolder4

$responseFolder4 = Invoke-RestMethod -Method POST -Uri "https://eastus.azuredatabricks.net/api/2.0/workspace/mkdirs" -Headers $headers -Body $jsonBodyFolder4


# 5th Folder structure

$requestBodyFolder5 = @{
  
  "path" = "/Shared/dlt/azuresqlonpremdbsource/batch"
 
}

$jsonBodyFolder5 = ConvertTo-Json -Depth 100 $requestBodyFolder5

$responseFolder5 = Invoke-RestMethod -Method POST -Uri "https://eastus.azuredatabricks.net/api/2.0/workspace/mkdirs" -Headers $headers -Body $jsonBodyFolder5

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
$path = "/Shared/$filenamewithoutextension";
if($filename.ToLower().Contains("EventHub".ToLower()))
{
Write-Output "Copying EventHub Files"
$path = "/Shared/dlt/eventhub/stream/"+$filenamewithoutextension
}
if($filename.ToLower().Contains("RawFiles".ToLower()))
{
Write-Output "Copying filesource Files"
$path = "/Shared/dlt/filesource/batch/"+$filenamewithoutextension
}
if($filename.ToLower().Contains("sql_db".ToLower()))
{
Write-Output "Copying azuresqldbsource Files"
$path = "/Shared/dlt/azuresqldbsource/batch/"+$filenamewithoutextension
}
if($filename.ToLower().Contains("sql_on_prem".ToLower()))
{
Write-Output "Copying azuresqlonpremdbsource Files"
$path = "/Shared/dlt/azuresqlonpremdbsource/batch/"+$filenamewithoutextension
}
if($filename.ToLower().Contains("postgres".ToLower()))
{
Write-Output "Copying postgresdbsource Files"
$path = "/Shared/dlt/postgresdbsource/batch/"+$filenamewithoutextension
}



# Set the request body
$requestBody = @{
  "content" = $notebookBase64
  "path" = $path
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
