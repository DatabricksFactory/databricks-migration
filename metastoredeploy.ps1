param(
   [string] $RESOURCEGROUPNAME,
   [string] $METASTORENAME
)

# Get workspace details
Write-Host "Getting Workspace Details"
$WORKSPACE = Get-AzResource -ResourceGroupName $RESOURCEGROUPNAME -Resourcetype Microsoft.Databricks/workspaces
$WORKSPACENAME = $WORKSPACE.Name
$RESOURCEID = $WORKSPACE.ResourceId
$REGION = $WORKSPACE.Location
$WORKSPACENAME

# Get workspace URI and ID
Write-Host "Getting Workspace URI and ID"
$WORKSPACEPROPERTIES=(Get-AzResource -ResourceId $RESOURCEID -ExpandProperties).Properties
$WORKSPACEID = $WORKSPACEPROPERTIES.workspaceId
$WORKSPACEURL = $WORKSPACEPROPERTIES.workspaceUrl

# Get storage account for metastore
Write-Host "Getting storage account for metastore"
$STORAGEACCOUNTNAMES = Get-AzStorageAccount -ResourceGroupName $RESOURCEGROUPNAME
foreach($i in $STORAGEACCOUNTNAMES)
{
    foreach($j in $i)
    {
        if($j.StorageAccountName -like "*metastore*")
        {
            $STORAGEACCOUNTNAME = $j.StorageAccountName
        }
    }
}
$STORAGEACCOUNTNAME
$CONTAINERNAME = "containerformetastore"

# Get databricks resource token
$TOKEN = (Get-AzAccessToken -Resource '2ff814a6-3304-4ab8-85cb-cd0e6f879c1d').Token

# Get azure access token
$AZ_TOKEN = (Get-AzAccessToken -ResourceUrl 'https://management.core.windows.net/').Token

# Get Databricks Personal access token
Write-Host "Getting Personal access token"
$HEADERS = @{
   "Authorization"                            = "Bearer $TOKEN"
   "X-Databricks-Azure-SP-Management-Token"   = "$AZ_TOKEN"
   "X-Databricks-Azure-Workspace-Resource-Id" = "$RESOURCEID"
}
$BODY = @"
   { "lifetime_seconds": 1200, "comment": "For Creating Metastore" }
"@

$PAT = ((Invoke-RestMethod -Method POST -Uri "https://$WORKSPACEURL/api/2.0/token/create" -Headers $HEADERS -Body $BODY).token_value)

# Create Metastore
Write-Host "Creating metastore"
$HEADERS = @{
   "Authorization" = "Bearer $PAT"
}
$BODY = @"
{
"name": "$METASTORENAME",
"storage_root": "abfss://$CONTAINERNAME@$STORAGEACCOUNTNAME.dfs.core.windows.net/",
"region": "$REGION"
}
"@

$METASTOREURI = "https://$WORKSPACEURL/api/2.1/unity-catalog/metastores"
$METASTOREURI
$RESPONSE = Invoke-RestMethod -Method POST -Uri $METASTOREURI -Headers $HEADERS -Body $BODY 
$METASTORE_ID = $RESPONSE.metastore_id


# Assign workspace
Write-Host "Assigning workspace to metastore"
$HEADERS = @{
   "Authorization" = "Bearer $PAT"
}
$BODY = @"
{
    "metastore_id": "$METASTORE_ID",
    "default_catalog_name": "$METASTORENAME"
}
"@

$ASSIGNWSURI = "https://$WORKSPACEURL/api/2.1/unity-catalog/workspaces/$WORKSPACEID/metastore"
$RESPONSE = Invoke-RestMethod -Method PUT -Uri $ASSIGNWSURI -Headers $HEADERS -Body $BODY
