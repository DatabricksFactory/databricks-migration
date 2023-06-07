param(
$SUBSCRIPTION_ID,
$RG_NAME,
$WORKSPACE_NAME
)

Write-Output "Task: Generating Databricks Workspace URL"

try {
    $token = (Get-AzAccessToken).Token
    
    # https url for getting workspace details
    $url = "https://management.azure.com/subscriptions/" + $SUBSCRIPTION_ID + "/resourceGroups/" + $RG_NAME + "/providers/Microsoft.Databricks/workspaces/" + $WORKSPACE_NAME + "?api-version=2023-02-01"
    
    # Set the headers
    $headerstkn = @{ Authorization = "Bearer $token"; 'ContentType' = "application/json" }
    
    #call http method to get workspace url
    $resurl = Invoke-RestMethod -Method Get -ContentType "application/json" -Uri $url  -Headers $headerstkn
    $WorkspaceUrl = $resurl.properties.workspaceUrl
    Write-Host "Successful: Databricks workspace url is generated"
}
catch {
    Write-Host "Error while getting the Workspace URL"
    $errorMessage = $_.Exception.Message
    Write-Host "Error message: $errorMessage"
}

# Generating Databricks Workspace resource ID

Write-Output "Task: Generating Databricks Workspace resource ID"

try {
    $WORKSPACE_ID = Get-AzResource -ResourceType Microsoft.Databricks/workspaces -ResourceGroupName $RG_NAME -Name $WORKSPACE_NAME
    $ACTUAL_WORKSPACE_ID = $WORKSPACE_ID.ResourceId
    Write-Host "Successful: Databricks workspace resource ID is generated"
}
catch {
    Write-Host "Error while getting workspace ID"
    $errorMessage = $_.Exception.Message
    Write-Host "Error message: $errorMessage"
}

# Generating Databricks resource token

Write-Output "Task: Generating Databricks resource token"

try {
    # unique resource ID for the Azure Databricks service
    [string] $TOKEN = (Get-AzAccessToken -Resource '2ff814a6-3304-4ab8-85cb-cd0e6f879c1d').Token
    Write-Host "Successful: Resource Token generated"
}
catch {
    Write-Host "Error while getting the resource token"
    $errorMessage = $_.Exception.Message
    Write-Host "Error message: $errorMessage"    
}

# Generating Databricks management token

Write-Output "Task: Generating management token"

try {
    [string] $AZ_TOKEN = (Get-AzAccessToken -ResourceUrl 'https://management.core.windows.net/').Token   
    Write-Host "Successful: Management token generated"
}
catch {
    Write-Host "Error while getting the management token"
    $errorMessage = $_.Exception.Message
    Write-Host "Error message: $errorMessage"    
}

# Generating Databricks Personal access token

Write-Output "Task: Generating Databricks Personal access token"
# Set the headers
$HEADERS = @{
    "Authorization"                            = "Bearer $TOKEN"
    "X-Databricks-Azure-SP-Management-Token"   = "$AZ_TOKEN"
    "X-Databricks-Azure-Workspace-Resource-Id" = "$ACTUAL_WORKSPACE_ID"
}
# Set the request body
$BODY = @"
    { "lifetime_seconds": 1200, "comment": "Job Automation Script" }
"@
    
try {
    #https request for generating token
    Write-Host "Attempt 1 : Generating Personal Access Token"
    $DB_PAT = ((Invoke-RestMethod -Method POST -Uri "https://$WorkspaceUrl/api/2.0/token/create" -Headers $HEADERS -Body $BODY).token_value)
    Write-Output "Successful: Personal Access Token generated"
}
catch {
    Write-Host "Attempt 1 : Error while calling the Databricks API for generating Personal Access Token"
    $errorMessage = $_.Exception.Message
    Write-Host "Error message: $errorMessage" 
    try {
        Write-Host "Attempt 2 : generating Personal Access Token"
        $DB_PAT = ((Invoke-RestMethod -Method POST -Uri "https://$WorkspaceUrl/api/2.0/token/create" -Headers $HEADERS -Body $BODY).token_value)
        Write-Output "Successful: Personal Access Token generated"
    }
    catch {
        Write-Host "Attempt 2 : Error while calling the Databricks API for generating Personal Access Token"
        $errorMessage = $_.Exception.Message
        Write-Host "Error message: $errorMessage" 
    }
}

$HEADERS = @{
    "Authorization" = "Bearer $DB_PAT"
    "Content-Type"  = "application/json"
}

# Create the pipeline for the batch processing of the retail org dataset
Write-Host "[INFO] Create the pipeline for the batch processing of the retail org dataset"

$pipelineCreateUrl = "https://$WorkspaceUrl/api/2.0/pipelines"

$pipelineDefinitionJSON = @"
{
    "name": "retail_org_batch_dlt(API)",
    "edition": "ADVANCED",
    "target": "retail_org_batch_api",
    "clusters": [
        {
            "label": "default",
            "num_workers": 1
        }
    ],
    "development": true,
    "continuous": false,
    "channel": "CURRENT",
    "photon": false,
    "libraries": [
        {
            "notebook": {
                "path": "/Shared/Example/bronze-layer-notebook"
            }
        },
        {
            "notebook": {
                "path": "/Shared/Example/silver-layer-notebook"
            }
        },
        {
            "notebook": {
                "path": "/Shared/Example/gold-layer-notebook"
            }
        }
    ]
}
"@

try {
    $batchPipelineId = (Invoke-RestMethod -Method POST -Uri $pipelineCreateUrl -Headers $HEADERS -Body $pipelineDefinitionJSON).pipeline_id
    Write-Host "[SUCCESS] Batch pipeline created successfully with Pipeline ID: $batchPipelineId"
}
catch {
    Write-Host "[ERROR] Error while calling the Databricks API for creating the batch pipeline"
    $errorMessage = $_.Exception.Message
    Write-Host "Error message: $errorMessage" 
}

# Create jobs and run for the retail org dataset
Write-Host '[INFO] Create jobs and run them for the retail org dataset'

$jobCreateUrl = "https://$WorkspaceUrl/api/2.1/jobs/create"

$jobRunUrl = "https://$WorkspaceUrl/api/2.1/jobs/run-now"

# Create and run the batch job

$batchJobDefinition = @"
        {
            "name": "retail_org_batch(API)",
            "max_concurrent_runs": 1,
            "tasks": [
                {
                    "task_key": "batch_processing_dlt",
                    "pipeline_task": {
                        "pipeline_id": "$batchPipelineId"
                    }
                }
            ],
            "format": "MULTI_TASK"
        }
"@

if ($null -ne $batchPipelineId) {
    try {
        $batchJobId = (Invoke-RestMethod -Method POST -Uri $jobCreateUrl -Headers $HEADERS -Body $batchJobDefinition).job_id
        $batchJobCreated = $true
        Write-Host "[SUCCESS] Job successfully created for batch processing with Job ID: $batchJobId"
    }
    catch {
        $batchJobCreated = $false
        Write-Host "[ERROR] Error while calling the Databricks API for creating job for batch processing"
        $errorMessage = $_.Exception.Message
        Write-Host "Error message: $errorMessage" 
    }
}

if ($batchJobCreated) {
    $batchRunDefinition = @"
        {
            "job_id": $batchJobId
        }
"@
    try {
        $batchRunId = (Invoke-RestMethod -Method POST -Uri $jobRunUrl -Headers $HEADERS -Body $batchRunDefinition).run_id
        Write-Host "[SUCCESS] Running the job for batch processing with Run ID: $batchRunId"
    }
    catch {
        Write-Host "[ERROR] Error while calling the Databricks API for running the batch processing job"
        $errorMessage = $_.Exception.Message
        Write-Host "Error message: $errorMessage" 
    }
}

# Create and run the stream job

$streamJobDefinition = @"
        {
            "name": "retail_org_stream(API)",
            "max_concurrent_runs": 1,
            "tasks": [
                {
                    "task_key": "publish_events",
                    "notebook_task": {
                        "notebook_path": "/Shared/Example/publish_events-eventhub",
                        "source": "WORKSPACE"
                    },
                    "job_cluster_key": "Job_cluster"
                },
                {
                    "task_key": "stream_processing",
                    "depends_on": [
                        {
                            "task_key": "publish_events"
                        }
                    ],
                    "notebook_task": {
                        "notebook_path": "/Shared/Example/bronze_silver_gold_stream",
                        "source": "WORKSPACE"
                    },
                    "job_cluster_key": "Job_cluster",
                    "libraries": [
                        {
                            "maven": {
                                "coordinates": "com.microsoft.azure:azure-eventhubs-spark_2.12:2.3.22"
                            }
                        }
                    ]
                }
            ],
            "job_clusters": [
                {
                    "job_cluster_key": "Job_cluster",
                    "new_cluster": {
                        "cluster_name": "",
                        "spark_version": "12.2.x-scala2.12",
                        "spark_conf": {
                            "spark.databricks.delta.preview.enabled": "true",
                            "spark.master": "local[*, 4]",
                            "spark.databricks.cluster.profile": "singleNode"
                        },
                        "azure_attributes": {
                            "first_on_demand": 1,
                            "availability": "ON_DEMAND_AZURE",
                            "spot_bid_max_price": -1
                        },
                        "node_type_id": "Standard_DS3_v2",
                        "custom_tags": {
                            "ResourceClass": "SingleNode"
                        },
                        "spark_env_vars": {
                            "PYSPARK_PYTHON": "/databricks/python3/bin/python3"
                        },
                        "enable_elastic_disk": true,
                        "data_security_mode": "LEGACY_SINGLE_USER_STANDARD",
                        "runtime_engine": "STANDARD",
                        "num_workers": 0
                    }
                }
            ],
            "format": "MULTI_TASK"
        }
"@
    
try {
    $streamJobId = (Invoke-RestMethod -Method POST -Uri $jobCreateUrl -Headers $HEADERS -Body $streamJobDefinition).job_id
    $streamJobCreated = $true
    Write-Host "[SUCCESS] Job successfully created for stream processing with Job ID: $streamJobId"
}
catch {
    $streamJobCreated = $false
    Write-Host "[ERROR] Error while calling the Databricks API for creating job for stream processing"
    $errorMessage = $_.Exception.Message
    Write-Host "Error message: $errorMessage" 
}


if ($streamJobCreated) {
    $streamRunDefinition = @"
    {
        "job_id": $streamJobId
    }
"@
    try {
        $streamRunId = (Invoke-RestMethod -Method POST -Uri $jobRunUrl -Headers $HEADERS -Body $streamRunDefinition).run_id
        Write-Host "[SUCCESS] Running the job for stream processing with Run ID: $streamRunId"
    }
    catch {
        Write-Host "[ERROR] Error while calling the Databricks API for running the stream processing job"
        $errorMessage = $_.Exception.Message
        Write-Host "Error message: $errorMessage" 
    }
}
