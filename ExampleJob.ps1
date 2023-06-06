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
    { "lifetime_seconds": 1200, "comment": "Script Test" }
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

try {
    $pipelineCreateUrl = "https://$WorkspaceUrl/api/2.0/pipelines"

    $HEADERS = @{
        "Authorization" = "Bearer $DB_PAT"
        "Content-Type"  = "application/json"
    }

    $pipelineDefinitionJSON = @"
    {
        "name": "batch_processing_pipeline_api",
        "clusters": [
            {
                "label": "default",
                "num_workers": 0
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
        ],
        "edition": "ADVANCED",
        "target": "batch_example_api"
    }
"@

    $pipelineId = (Invoke-RestMethod -Method POST -Uri $pipelineCreateUrl -Headers $HEADERS -Body $pipelineDefinitionJSON).pipeline_id
}
catch {
    Write-Host "Error while calling the Databricks API for creating pipeline"
    $errorMessage = $_.Exception.Message
    Write-Host "Error message: $errorMessage" 
}

if ($null -ne $pipelineId) {
    Write-Host $pipelineId
    try {
        $jobCreateUrl = "https://$WorkspaceUrl/api/2.1/jobs/create"
    
        $HEADERS = @{
            "Authorization" = "Bearer $DB_PAT"
            "Content-Type"  = "application/json"
        }
    
        $jobDefinitionJSON = @"
        {
            "name": "example_job_API",
            "max_concurrent_runs": 1,
            "tasks": [
                {
                    "task_key": "batch_processing_pipeline",
                    "pipeline_task": {
                        "pipeline_id": "$pipelineId",
                        "full_refresh": false
                    }
                },
                {
                    "task_key": "publish_events",
                    "depends_on": [
                        {
                            "task_key": "batch_processing_pipeline"
                        }
                    ],
                    "notebook_task": {
                        "notebook_path": "/Shared/Example/publish_events-eventhub",
                        "source": "WORKSPACE"
                    },
                    "job_cluster_key": "Job_cluster_API"
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
                    "job_cluster_key": "Job_cluster_API",
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
                    "job_cluster_key": "Job_cluster_API",
                    "new_cluster": {
                        "cluster_name": "",
                        "spark_version": "12.2.x-scala2.12",
                        "spark_conf": {
                            "spark.databricks.delta.preview.enabled": "true",
                            "spark.master": "local[*, 4]",
                            "spark.databricks.cluster.profile": "singleNode"
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
    
        $jobId = (Invoke-RestMethod -Method POST -Uri $jobCreateUrl -Headers $HEADERS -Body $jobDefinitionJSON).job_id
        $jobCreated = $true
    }
    catch {
        $jobCreated = $false
        Write-Host "Error while calling the Databricks API for creating Job"
        $errorMessage = $_.Exception.Message
        Write-Host "Error message: $errorMessage" 
    }
    
}

if ($jobCreated) {
    Write-Host $jobId
    try {
        $jobRunUrl = "https://$WorkspaceUrl/api/2.1/jobs/run-now"

        $HEADERS = @{
            "Authorization" = "Bearer $DB_PAT"
            "Content-Type"  = "application/json"
        }
            
        $runDefinitionJSON = @"
        {
            "idempotency_token": "101",
            "job_id": $jobId
        }
"@

        $runId = (Invoke-RestMethod -Method POST -Uri $jobRunUrl -Headers $HEADERS -Body $runDefinitionJSON).run_id
        
        if ($null -ne $runId) {
            Write-Host "Job running with Run ID: $runId"
        } else {
            Write-Host "Job not running"
        }
    }
    catch {
        Write-Host "Error while calling the Databricks API for running job"
        $errorMessage = $_.Exception.Message
        Write-Host "Error message: $errorMessage" 
    }
}
