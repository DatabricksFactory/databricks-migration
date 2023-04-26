param(
[string] $RG_NAME,
[string] $REGION,
[string] $WORKSPACE_NAME
)


Write-Output "Task: Generating Databricks Token"

$WORKSPACE_ID = Get-AzResource -ResourceType Microsoft.Databricks/workspaces -ResourceGroupName $RG_NAME -Name $WORKSPACE_NAME
$ACTUAL_WORKSPACE_ID = $WORKSPACE_ID.ResourceId
$token = (Get-AzAccessToken -Resource '2ff814a6-3304-4ab8-85cb-cd0e6f879c1d').Token
$AZ_TOKEN = (Get-AzAccessToken -ResourceUrl 'https://management.core.windows.net/').Token
$HEADERS = @{
    "Authorization" = "Bearer $TOKEN"
    "X-Databricks-Azure-SP-Management-Token" = "$AZ_TOKEN"
    "X-Databricks-Azure-Workspace-Resource-Id" = "$ACTUAL_WORKSPACE_ID"
}
$BODY = @'
{ "lifetime_seconds": 1200, "comment": "ARM deployment" }
'@
$DB_PAT = ((Invoke-RestMethod -Method POST -Uri "https://$REGION.azuredatabricks.net/api/2.0/token/create" -Headers $HEADERS -Body $BODY).token_value)


Write-Output "Task: Creating cluster"
$HEADERS = @{
    "Authorization" = "Bearer $DB_PAT"
    "Content-Type" = "application/json"
}
$BODY = @"
{"cluster_name": "dbcluster", "spark_version": "11.3.x-scala2.12", "autotermination_minutes": 30, "num_workers": "2", "node_type_id": "Standard_DS3_v2", "driver_node_type_id": "Standard_DS3_v2" }
"@
$CLUSTER_ID = ((Invoke-RestMethod -Method POST -Uri "https://$REGION.azuredatabricks.net/api/2.0/clusters/create" -Headers $HEADERS -Body $BODY).cluster_id)
if ( $CLUSTER_ID -ne "null" ) {
    Write-Output "[INFO] CLUSTER_ID: $CLUSTER_ID"
} else {
    Write-Output "[ERROR] cluster was not created"
    exit 1
}

Write-Output "Task: Checking cluster"
$RETRY_LIMIT = 15
$RETRY_TIME = 60
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
