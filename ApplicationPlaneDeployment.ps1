param(
    [string] $RG_NAME,
    [string] $REGION,
    [int] $LIFETIME_SECONDS,
    [string] $COMMENT,
    [bool] $CTRL_DEPLOY_NOTEBOOK,
    [string] $NOTEBOOK_PATH
)
Write-Output "Task: Generating Databricks Token"

    $WORKSPACE_ID = Get-AzResource -ResourceType Microsoft.Databricks/workspaces -ResourceGroupName $RG_NAME
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
$filenamewithoutext =$filename -split "\."


# Set the request body
$requestBody = @{
  "content" = $notebookBase64
  "path" = "/Shared/Templates/$filenamewithoutext[0]"
  "language" = "PYTHON"
  "format" = "JUPYTER"
}


# Convert the request body to JSON
$jsonBody = ConvertTo-Json -Depth 100 $requestBody

   # Make the HTTP request to import the notebook
   $response = Invoke-RestMethod -Method POST -Uri "https://$REGION.azuredatabricks.net/api/2.0/workspace/import" -Headers $headers -Body $jsonBody  
} 

}
