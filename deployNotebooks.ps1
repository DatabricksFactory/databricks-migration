<#
.SYNOPSIS
    Deploys notebooks to Azure Databricks workspace.
.DESCRIPTION
    This script deploys notebooks to an Azure Databricks workspace. It takes in the following parameters:
    - tenant_id: The Azure Active Directory tenant ID.
    - client_id: The Azure Active Directory application client ID.
    - client_secret: The Azure Active Directory application client secret.
    - subscription_id: The Azure subscription ID.
    - resourceGroup: The name of the resource group containing the Databricks workspace.
    - workspaceName: The name of the Databricks workspace.
    - notebookPathUnderWorkspace: The path to the directory in the workspace where the notebooks should be deployed.
    The script uses the Databricks API to create the necessary directories in the workspace and deploy the notebooks.
    Note: This script must be run in the directory with the notebooks (spaces in names in Bash can cause issues).
.PARAMETER tenant_id
    The Azure Active Directory tenant ID.
.PARAMETER client_id
    The Azure Active Directory application client ID.
.PARAMETER client_secret
    The Azure Active Directory application client secret.
.PARAMETER subscription_id
    The Azure subscription ID.
.PARAMETER resourceGroup
    The name of the resource group containing the Databricks workspace.
.PARAMETER workspaceName
    The name of the Databricks workspace.
.PARAMETER notebookPathUnderWorkspace
    The path to the directory in the workspace where the notebooks should be deployed.
.EXAMPLE
    .\deploy_notebooks.ps1 -tenant_id "12345678-1234-1234-1234-123456789012" -client_id "12345678-1234-1234-1234-123456789012" -client_secret "my_client_secret" -subscription_id "12345678-1234-1234-1234-123456789012" -resourceGroup "my_resource_group" -workspaceName "my_workspace" -notebookPathUnderWorkspace "/my_notebooks"
#>

param (
    [Parameter(Mandatory=$true)]
    [string]$tenant_id,
    [Parameter(Mandatory=$true)]
    [string]$client_id,
    [Parameter(Mandatory=$true)]
    [string]$client_secret,
    [Parameter(Mandatory=$true)]
    [string]$subscription_id,
    [Parameter(Mandatory=$true)]
    [string]$resourceGroup,
    [Parameter(Mandatory=$true)]
    [string]$workspaceName,
    [Parameter(Mandatory=$true)]
    [string]$notebookPathUnderWorkspace
)

$azure_databricks_resource_id = "2ff814a6-3304-4ab8-85cb-cd0e6f879c1d"
$resourceId = "/subscriptions/$subscription_id/resourceGroups/$resourceGroup/providers/Microsoft.Databricks/workspaces/$workspaceName"

######################################################################################
# Get access tokens for Databricks API
######################################################################################

$accessToken = (Invoke-WebRequest -Method POST -Uri "https://login.microsoftonline.com/$tenant_id/oauth2/token" `
    -Body @{
        resource = $azure_databricks_resource_id
        client_id = $client_id
        grant_type = "client_credentials"
        client_secret = $client_secret
    } `
    -UseBasicParsing `
    | ConvertFrom-Json).access_token

$managementToken = (Invoke-WebRequest -Method POST -Uri "https://login.microsoftonline.com/$tenant_id/oauth2/token" `
    -Body @{
        resource = "https://management.core.windows.net/"
        client_id = $client_id
        grant_type = "client_credentials"
        client_secret = $client_secret
    } `
    -UseBasicParsing `
    | ConvertFrom-Json).access_token

######################################################################################
# Get Databricks workspace URL (e.g. adb-5946405904802522.2.azuredatabricks.net)
######################################################################################

$workspaceUrl = (Invoke-WebRequest -Method GET -Uri "https://management.azure.com/subscriptions/$subscription_id/resourcegroups/$resourceGroup/providers/Microsoft.Databricks/workspaces/$workspaceName?api-version=2018-04-01" `
    -Headers @{
        "Content-Type" = "application/json"
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
    } else {
        $pathOnDatabricks = "$notebookPathUnderWorkspace/$directoryName"
    }
    Write-Host "pathOnDatabricks: $pathOnDatabricks"

    $JSON = @{ path = $pathOnDatabricks } | ConvertTo-Json
    Write-Host "Creating Path: $JSON"

    Invoke-WebRequest -Method POST -Uri "https://$workspaceUrl/api/2.0/workspace/mkdirs" `
        -Headers @{
            "Authorization" = "Bearer $accessToken"
            "X-Databricks-Azure-SP-Management-Token" = $managementToken
            "X-Databricks-Azure-Workspace-Resource-Id" = $resourceId
            "Content-Type" = "application/json"
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
            "Authorization" = "Bearer $accessToken"
            "X-Databricks-Azure-SP-Management-Token" = $managementToken
            "X-Databricks-Azure-Workspace-Resource-Id" = $resourceId
        } `
        -ContentType "multipart/form-data" `
        -Body @{
            language = $language
            overwrite = $true
            path = "$notebookPathUnderWorkspace/$filename"
            content = Get-Content -Path $_.FullName -Raw
        }
}
