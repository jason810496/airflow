<!--
 Licensed to the Apache Software Foundation (ASF) under one
 or more contributor license agreements.  See the NOTICE file
 distributed with this work for additional information
 regarding copyright ownership.  The ASF licenses this file
 to you under the Apache License, Version 2.0 (the
 "License"); you may not use this file except in compliance
 with the License.  You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing,
 software distributed under the License is distributed on an
 "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 KIND, either express or implied.  See the License for the
 specific language governing permissions and limitations
 under the License.
 -->

# github.com/apache/airflow/go-sdk/providers/amazon

The operators and sensors of `apache-airflow-providers-amazon` 9.40.0, for a Dag written with the Go SDK.

A wrapper has no task body. It records the Python operator class and its arguments in the
Dag, and a Python worker that has the provider installed runs the operator. Such a task takes
the Dag's queue like any other task: give it a `Queue` in its `TaskSpec` when the Dag's queue
is not served by Python workers.

```go
import "github.com/apache/airflow/go-sdk/providers/amazon/aws/operators/appflow"

dag.Task(
    appflow.AppflowBaseOperator(appflow.AppflowBaseOperatorArgs{ /* ... */ }),
    airflow.TaskSpec{TaskID: "my_task", Queue: "default"},
)
```

A package mirrors the Python module of the same path, without the underscores, and an
argument is the Python argument in Go case. An argument that is not required is a pointer
or a slice, a map or `any`, so that `airflow.Ptr` sets it and nil leaves it out. A templated
argument is an `airflow.Templated`: it takes a Jinja template, or the result of another task
with `airflow.ResultOf`. A duration is a `time.Duration`.

This package is generated. To change it, regenerate the operator specs and the packages:

```bash
python3 scripts/ci/lang_sdk_providers/generate_operator_specs.py --allow-list amazon
```

```bash
just --justfile go-sdk/Justfile generate-providers
```

## Operators

| Operator | Package |
| --- | --- |
| `AppflowBaseOperator` | `github.com/apache/airflow/go-sdk/providers/amazon/aws/operators/appflow` |
| `AppflowRecordsShortCircuitOperator` | `github.com/apache/airflow/go-sdk/providers/amazon/aws/operators/appflow` |
| `AppflowRunAfterOperator` | `github.com/apache/airflow/go-sdk/providers/amazon/aws/operators/appflow` |
| `AppflowRunBeforeOperator` | `github.com/apache/airflow/go-sdk/providers/amazon/aws/operators/appflow` |
| `AppflowRunDailyOperator` | `github.com/apache/airflow/go-sdk/providers/amazon/aws/operators/appflow` |
| `AppflowRunFullOperator` | `github.com/apache/airflow/go-sdk/providers/amazon/aws/operators/appflow` |
| `AppflowRunOperator` | `github.com/apache/airflow/go-sdk/providers/amazon/aws/operators/appflow` |
| `AthenaOperator` | `github.com/apache/airflow/go-sdk/providers/amazon/aws/operators/athena` |
| `AthenaSparkOperator` | `github.com/apache/airflow/go-sdk/providers/amazon/aws/operators/athenaspark` |
| `BatchCreateComputeEnvironmentOperator` | `github.com/apache/airflow/go-sdk/providers/amazon/aws/operators/batch` |
| `BatchOperator` | `github.com/apache/airflow/go-sdk/providers/amazon/aws/operators/batch` |
| `BedrockBatchInferenceOperator` | `github.com/apache/airflow/go-sdk/providers/amazon/aws/operators/bedrock` |
| `BedrockCreateAgentRuntimeOperator` | `github.com/apache/airflow/go-sdk/providers/amazon/aws/operators/bedrock` |
| `BedrockCreateDataSourceOperator` | `github.com/apache/airflow/go-sdk/providers/amazon/aws/operators/bedrock` |
| `BedrockCreateEvaluationJobOperator` | `github.com/apache/airflow/go-sdk/providers/amazon/aws/operators/bedrock` |
| `BedrockCreateGuardrailOperator` | `github.com/apache/airflow/go-sdk/providers/amazon/aws/operators/bedrock` |
| `BedrockCreateGuardrailVersionOperator` | `github.com/apache/airflow/go-sdk/providers/amazon/aws/operators/bedrock` |
| `BedrockCreateKnowledgeBaseOperator` | `github.com/apache/airflow/go-sdk/providers/amazon/aws/operators/bedrock` |
| `BedrockCreateProvisionedModelThroughputOperator` | `github.com/apache/airflow/go-sdk/providers/amazon/aws/operators/bedrock` |
| `BedrockCustomizeModelOperator` | `github.com/apache/airflow/go-sdk/providers/amazon/aws/operators/bedrock` |
| `BedrockDeleteAgentRuntimeOperator` | `github.com/apache/airflow/go-sdk/providers/amazon/aws/operators/bedrock` |
| `BedrockDeleteGuardrailOperator` | `github.com/apache/airflow/go-sdk/providers/amazon/aws/operators/bedrock` |
| `BedrockIngestDataOperator` | `github.com/apache/airflow/go-sdk/providers/amazon/aws/operators/bedrock` |
| `BedrockInvokeAgentRuntimeOperator` | `github.com/apache/airflow/go-sdk/providers/amazon/aws/operators/bedrock` |
| `BedrockInvokeModelOperator` | `github.com/apache/airflow/go-sdk/providers/amazon/aws/operators/bedrock` |
| `BedrockRaGOperator` | `github.com/apache/airflow/go-sdk/providers/amazon/aws/operators/bedrock` |
| `BedrockRetrieveOperator` | `github.com/apache/airflow/go-sdk/providers/amazon/aws/operators/bedrock` |
| `BedrockUpdateGuardrailOperator` | `github.com/apache/airflow/go-sdk/providers/amazon/aws/operators/bedrock` |
| `CloudFormationCreateStackOperator` | `github.com/apache/airflow/go-sdk/providers/amazon/aws/operators/cloudformation` |
| `CloudFormationDeleteStackOperator` | `github.com/apache/airflow/go-sdk/providers/amazon/aws/operators/cloudformation` |
| `ComprehendBaseOperator` | `github.com/apache/airflow/go-sdk/providers/amazon/aws/operators/comprehend` |
| `ComprehendCreateDocumentClassifierOperator` | `github.com/apache/airflow/go-sdk/providers/amazon/aws/operators/comprehend` |
| `ComprehendStartPiiEntitiesDetectionJobOperator` | `github.com/apache/airflow/go-sdk/providers/amazon/aws/operators/comprehend` |
| `DataSyncOperator` | `github.com/apache/airflow/go-sdk/providers/amazon/aws/operators/datasync` |
| `DmsCreateReplicationConfigOperator` | `github.com/apache/airflow/go-sdk/providers/amazon/aws/operators/dms` |
| `DmsCreateTaskOperator` | `github.com/apache/airflow/go-sdk/providers/amazon/aws/operators/dms` |
| `DmsDeleteReplicationConfigOperator` | `github.com/apache/airflow/go-sdk/providers/amazon/aws/operators/dms` |
| `DmsDeleteTaskOperator` | `github.com/apache/airflow/go-sdk/providers/amazon/aws/operators/dms` |
| `DmsDescribeReplicationConfigsOperator` | `github.com/apache/airflow/go-sdk/providers/amazon/aws/operators/dms` |
| `DmsDescribeReplicationsOperator` | `github.com/apache/airflow/go-sdk/providers/amazon/aws/operators/dms` |
| `DmsDescribeTasksOperator` | `github.com/apache/airflow/go-sdk/providers/amazon/aws/operators/dms` |
| `DmsModifyTaskOperator` | `github.com/apache/airflow/go-sdk/providers/amazon/aws/operators/dms` |
| `DmsReloadTablesOperator` | `github.com/apache/airflow/go-sdk/providers/amazon/aws/operators/dms` |
| `DmsStartReplicationOperator` | `github.com/apache/airflow/go-sdk/providers/amazon/aws/operators/dms` |
| `DmsStartTaskOperator` | `github.com/apache/airflow/go-sdk/providers/amazon/aws/operators/dms` |
| `DmsStopReplicationOperator` | `github.com/apache/airflow/go-sdk/providers/amazon/aws/operators/dms` |
| `DmsStopTaskOperator` | `github.com/apache/airflow/go-sdk/providers/amazon/aws/operators/dms` |
| `EC2CreateInstanceOperator` | `github.com/apache/airflow/go-sdk/providers/amazon/aws/operators/ec2` |
| `EC2HibernateInstanceOperator` | `github.com/apache/airflow/go-sdk/providers/amazon/aws/operators/ec2` |
| `EC2RebootInstanceOperator` | `github.com/apache/airflow/go-sdk/providers/amazon/aws/operators/ec2` |
| `EC2StartInstanceOperator` | `github.com/apache/airflow/go-sdk/providers/amazon/aws/operators/ec2` |
| `EC2StopInstanceOperator` | `github.com/apache/airflow/go-sdk/providers/amazon/aws/operators/ec2` |
| `EC2TerminateInstanceOperator` | `github.com/apache/airflow/go-sdk/providers/amazon/aws/operators/ec2` |
| `EcrCreateRepositoryOperator` | `github.com/apache/airflow/go-sdk/providers/amazon/aws/operators/ecr` |
| `EcrDeleteRepositoryOperator` | `github.com/apache/airflow/go-sdk/providers/amazon/aws/operators/ecr` |
| `EcrSetRepositoryPolicyOperator` | `github.com/apache/airflow/go-sdk/providers/amazon/aws/operators/ecr` |
| `EcsBaseOperator` | `github.com/apache/airflow/go-sdk/providers/amazon/aws/operators/ecs` |
| `EcsCreateClusterOperator` | `github.com/apache/airflow/go-sdk/providers/amazon/aws/operators/ecs` |
| `EcsDeleteClusterOperator` | `github.com/apache/airflow/go-sdk/providers/amazon/aws/operators/ecs` |
| `EcsDeregisterTaskDefinitionOperator` | `github.com/apache/airflow/go-sdk/providers/amazon/aws/operators/ecs` |
| `EcsRegisterTaskDefinitionOperator` | `github.com/apache/airflow/go-sdk/providers/amazon/aws/operators/ecs` |
| `EcsRunTaskOperator` | `github.com/apache/airflow/go-sdk/providers/amazon/aws/operators/ecs` |
| `EksCreateClusterOperator` | `github.com/apache/airflow/go-sdk/providers/amazon/aws/operators/eks` |
| `EksCreateFargateProfileOperator` | `github.com/apache/airflow/go-sdk/providers/amazon/aws/operators/eks` |
| `EksCreateNodegroupOperator` | `github.com/apache/airflow/go-sdk/providers/amazon/aws/operators/eks` |
| `EksDeleteClusterOperator` | `github.com/apache/airflow/go-sdk/providers/amazon/aws/operators/eks` |
| `EksDeleteFargateProfileOperator` | `github.com/apache/airflow/go-sdk/providers/amazon/aws/operators/eks` |
| `EksDeleteNodegroupOperator` | `github.com/apache/airflow/go-sdk/providers/amazon/aws/operators/eks` |
| `EksPodExecOperator` | `github.com/apache/airflow/go-sdk/providers/amazon/aws/operators/eks` |
| `EksPodOperator` | `github.com/apache/airflow/go-sdk/providers/amazon/aws/operators/eks` |
| `EmrAddStepsOperator` | `github.com/apache/airflow/go-sdk/providers/amazon/aws/operators/emr` |
| `EmrContainerOperator` | `github.com/apache/airflow/go-sdk/providers/amazon/aws/operators/emr` |
| `EmrCreateJobFlowOperator` | `github.com/apache/airflow/go-sdk/providers/amazon/aws/operators/emr` |
| `EmrEksCreateClusterOperator` | `github.com/apache/airflow/go-sdk/providers/amazon/aws/operators/emr` |
| `EmrModifyClusterOperator` | `github.com/apache/airflow/go-sdk/providers/amazon/aws/operators/emr` |
| `EmrServerlessCreateApplicationOperator` | `github.com/apache/airflow/go-sdk/providers/amazon/aws/operators/emr` |
| `EmrServerlessDeleteApplicationOperator` | `github.com/apache/airflow/go-sdk/providers/amazon/aws/operators/emr` |
| `EmrServerlessGetSessionEndpointOperator` | `github.com/apache/airflow/go-sdk/providers/amazon/aws/operators/emr` |
| `EmrServerlessStartJobOperator` | `github.com/apache/airflow/go-sdk/providers/amazon/aws/operators/emr` |
| `EmrServerlessStartSessionOperator` | `github.com/apache/airflow/go-sdk/providers/amazon/aws/operators/emr` |
| `EmrServerlessStopApplicationOperator` | `github.com/apache/airflow/go-sdk/providers/amazon/aws/operators/emr` |
| `EmrServerlessStopSessionOperator` | `github.com/apache/airflow/go-sdk/providers/amazon/aws/operators/emr` |
| `EmrStartNotebookExecutionOperator` | `github.com/apache/airflow/go-sdk/providers/amazon/aws/operators/emr` |
| `EmrStopNotebookExecutionOperator` | `github.com/apache/airflow/go-sdk/providers/amazon/aws/operators/emr` |
| `EmrTerminateJobFlowOperator` | `github.com/apache/airflow/go-sdk/providers/amazon/aws/operators/emr` |
| `EventBridgeDisableRuleOperator` | `github.com/apache/airflow/go-sdk/providers/amazon/aws/operators/eventbridge` |
| `EventBridgeEnableRuleOperator` | `github.com/apache/airflow/go-sdk/providers/amazon/aws/operators/eventbridge` |
| `EventBridgePutEventsOperator` | `github.com/apache/airflow/go-sdk/providers/amazon/aws/operators/eventbridge` |
| `EventBridgePutRuleOperator` | `github.com/apache/airflow/go-sdk/providers/amazon/aws/operators/eventbridge` |
| `GlacierCreateJobOperator` | `github.com/apache/airflow/go-sdk/providers/amazon/aws/operators/glacier` |
| `GlacierUploadArchiveOperator` | `github.com/apache/airflow/go-sdk/providers/amazon/aws/operators/glacier` |
| `GlueDataQualityOperator` | `github.com/apache/airflow/go-sdk/providers/amazon/aws/operators/glue` |
| `GlueDataQualityRuleRecommendationRunOperator` | `github.com/apache/airflow/go-sdk/providers/amazon/aws/operators/glue` |
| `GlueDataQualityRuleSetEvaluationRunOperator` | `github.com/apache/airflow/go-sdk/providers/amazon/aws/operators/glue` |
| `GlueJobOperator` | `github.com/apache/airflow/go-sdk/providers/amazon/aws/operators/glue` |
| `GlueCatalogBatchDeletePartitionOperator` | `github.com/apache/airflow/go-sdk/providers/amazon/aws/operators/gluecatalog` |
| `GlueCatalogCreateDatabaseOperator` | `github.com/apache/airflow/go-sdk/providers/amazon/aws/operators/gluecatalog` |
| `GlueCatalogCreatePartitionOperator` | `github.com/apache/airflow/go-sdk/providers/amazon/aws/operators/gluecatalog` |
| `GlueCatalogCreateTableOperator` | `github.com/apache/airflow/go-sdk/providers/amazon/aws/operators/gluecatalog` |
| `GlueCatalogDeleteDatabaseOperator` | `github.com/apache/airflow/go-sdk/providers/amazon/aws/operators/gluecatalog` |
| `GlueCatalogDeleteTableOperator` | `github.com/apache/airflow/go-sdk/providers/amazon/aws/operators/gluecatalog` |
| `GlueCrawlerCreateOperator` | `github.com/apache/airflow/go-sdk/providers/amazon/aws/operators/gluecrawler` |
| `GlueCrawlerDeleteOperator` | `github.com/apache/airflow/go-sdk/providers/amazon/aws/operators/gluecrawler` |
| `GlueCrawlerOperator` | `github.com/apache/airflow/go-sdk/providers/amazon/aws/operators/gluecrawler` |
| `GlueCrawlerRunOperator` | `github.com/apache/airflow/go-sdk/providers/amazon/aws/operators/gluecrawler` |
| `GlueCrawlerUpdateOperator` | `github.com/apache/airflow/go-sdk/providers/amazon/aws/operators/gluecrawler` |
| `GlueDataBrewStartJobOperator` | `github.com/apache/airflow/go-sdk/providers/amazon/aws/operators/gluedatabrew` |
| `KinesisAnalyticsV2CreateApplicationOperator` | `github.com/apache/airflow/go-sdk/providers/amazon/aws/operators/kinesisanalytics` |
| `KinesisAnalyticsV2StartApplicationOperator` | `github.com/apache/airflow/go-sdk/providers/amazon/aws/operators/kinesisanalytics` |
| `KinesisAnalyticsV2StopApplicationOperator` | `github.com/apache/airflow/go-sdk/providers/amazon/aws/operators/kinesisanalytics` |
| `LambdaCreateFunctionOperator` | `github.com/apache/airflow/go-sdk/providers/amazon/aws/operators/lambdafunction` |
| `LambdaInvokeFunctionOperator` | `github.com/apache/airflow/go-sdk/providers/amazon/aws/operators/lambdafunction` |
| `MwaaTriggerDagRunOperator` | `github.com/apache/airflow/go-sdk/providers/amazon/aws/operators/mwaa` |
| `MwaaServerlessCreateWorkflowOperator` | `github.com/apache/airflow/go-sdk/providers/amazon/aws/operators/mwaaserverless` |
| `MwaaServerlessDeleteWorkflowOperator` | `github.com/apache/airflow/go-sdk/providers/amazon/aws/operators/mwaaserverless` |
| `MwaaServerlessStartWorkflowRunOperator` | `github.com/apache/airflow/go-sdk/providers/amazon/aws/operators/mwaaserverless` |
| `MwaaServerlessStopWorkflowRunOperator` | `github.com/apache/airflow/go-sdk/providers/amazon/aws/operators/mwaaserverless` |
| `MwaaServerlessUpdateWorkflowOperator` | `github.com/apache/airflow/go-sdk/providers/amazon/aws/operators/mwaaserverless` |
| `NeptuneStartDbClusterOperator` | `github.com/apache/airflow/go-sdk/providers/amazon/aws/operators/neptune` |
| `NeptuneStopDbClusterOperator` | `github.com/apache/airflow/go-sdk/providers/amazon/aws/operators/neptune` |
| `NeptuneCancelImportTaskOperator` | `github.com/apache/airflow/go-sdk/providers/amazon/aws/operators/neptuneanalytics` |
| `NeptuneCreateGraphOperator` | `github.com/apache/airflow/go-sdk/providers/amazon/aws/operators/neptuneanalytics` |
| `NeptuneCreateGraphWithImportOperator` | `github.com/apache/airflow/go-sdk/providers/amazon/aws/operators/neptuneanalytics` |
| `NeptuneCreatePrivateGraphEndpointOperator` | `github.com/apache/airflow/go-sdk/providers/amazon/aws/operators/neptuneanalytics` |
| `NeptuneDeleteGraphOperator` | `github.com/apache/airflow/go-sdk/providers/amazon/aws/operators/neptuneanalytics` |
| `NeptuneDeletePrivateGraphEndpointOperator` | `github.com/apache/airflow/go-sdk/providers/amazon/aws/operators/neptuneanalytics` |
| `NeptuneStartImportTaskOperator` | `github.com/apache/airflow/go-sdk/providers/amazon/aws/operators/neptuneanalytics` |
| `OpenSearchServerlessCreateCollectionOperator` | `github.com/apache/airflow/go-sdk/providers/amazon/aws/operators/opensearchserverless` |
| `QuickSightCreateIngestionOperator` | `github.com/apache/airflow/go-sdk/providers/amazon/aws/operators/quicksight` |
| `RdsBaseOperator` | `github.com/apache/airflow/go-sdk/providers/amazon/aws/operators/rds` |
| `RdsCancelExportTaskOperator` | `github.com/apache/airflow/go-sdk/providers/amazon/aws/operators/rds` |
| `RdsCopyDbSnapshotOperator` | `github.com/apache/airflow/go-sdk/providers/amazon/aws/operators/rds` |
| `RdsCreateDbInstanceOperator` | `github.com/apache/airflow/go-sdk/providers/amazon/aws/operators/rds` |
| `RdsCreateDbSnapshotOperator` | `github.com/apache/airflow/go-sdk/providers/amazon/aws/operators/rds` |
| `RdsCreateEventSubscriptionOperator` | `github.com/apache/airflow/go-sdk/providers/amazon/aws/operators/rds` |
| `RdsDeleteDbInstanceOperator` | `github.com/apache/airflow/go-sdk/providers/amazon/aws/operators/rds` |
| `RdsDeleteDbSnapshotOperator` | `github.com/apache/airflow/go-sdk/providers/amazon/aws/operators/rds` |
| `RdsDeleteEventSubscriptionOperator` | `github.com/apache/airflow/go-sdk/providers/amazon/aws/operators/rds` |
| `RdsStartDbOperator` | `github.com/apache/airflow/go-sdk/providers/amazon/aws/operators/rds` |
| `RdsStartExportTaskOperator` | `github.com/apache/airflow/go-sdk/providers/amazon/aws/operators/rds` |
| `RdsStopDbOperator` | `github.com/apache/airflow/go-sdk/providers/amazon/aws/operators/rds` |
| `RedshiftCreateClusterOperator` | `github.com/apache/airflow/go-sdk/providers/amazon/aws/operators/redshiftcluster` |
| `RedshiftCreateClusterSnapshotOperator` | `github.com/apache/airflow/go-sdk/providers/amazon/aws/operators/redshiftcluster` |
| `RedshiftDeleteClusterOperator` | `github.com/apache/airflow/go-sdk/providers/amazon/aws/operators/redshiftcluster` |
| `RedshiftDeleteClusterSnapshotOperator` | `github.com/apache/airflow/go-sdk/providers/amazon/aws/operators/redshiftcluster` |
| `RedshiftPauseClusterOperator` | `github.com/apache/airflow/go-sdk/providers/amazon/aws/operators/redshiftcluster` |
| `RedshiftResumeClusterOperator` | `github.com/apache/airflow/go-sdk/providers/amazon/aws/operators/redshiftcluster` |
| `RedshiftDataOperator` | `github.com/apache/airflow/go-sdk/providers/amazon/aws/operators/redshiftdata` |
| `S3CopyObjectOperator` | `github.com/apache/airflow/go-sdk/providers/amazon/aws/operators/s3` |
| `S3CopyPrefixOperator` | `github.com/apache/airflow/go-sdk/providers/amazon/aws/operators/s3` |
| `S3CreateBucketOperator` | `github.com/apache/airflow/go-sdk/providers/amazon/aws/operators/s3` |
| `S3CreateObjectOperator` | `github.com/apache/airflow/go-sdk/providers/amazon/aws/operators/s3` |
| `S3DeleteBucketOperator` | `github.com/apache/airflow/go-sdk/providers/amazon/aws/operators/s3` |
| `S3DeleteBucketTaggingOperator` | `github.com/apache/airflow/go-sdk/providers/amazon/aws/operators/s3` |
| `S3DeleteObjectsOperator` | `github.com/apache/airflow/go-sdk/providers/amazon/aws/operators/s3` |
| `S3FileTransformOperator` | `github.com/apache/airflow/go-sdk/providers/amazon/aws/operators/s3` |
| `S3GetBucketTaggingOperator` | `github.com/apache/airflow/go-sdk/providers/amazon/aws/operators/s3` |
| `S3ListOperator` | `github.com/apache/airflow/go-sdk/providers/amazon/aws/operators/s3` |
| `S3ListPrefixesOperator` | `github.com/apache/airflow/go-sdk/providers/amazon/aws/operators/s3` |
| `S3PutBucketTaggingOperator` | `github.com/apache/airflow/go-sdk/providers/amazon/aws/operators/s3` |
| `S3ReadObjectOperator` | `github.com/apache/airflow/go-sdk/providers/amazon/aws/operators/s3` |
| `S3TablesCreateNamespaceOperator` | `github.com/apache/airflow/go-sdk/providers/amazon/aws/operators/s3tables` |
| `S3TablesCreateTableBucketOperator` | `github.com/apache/airflow/go-sdk/providers/amazon/aws/operators/s3tables` |
| `S3TablesCreateTableOperator` | `github.com/apache/airflow/go-sdk/providers/amazon/aws/operators/s3tables` |
| `S3TablesDeleteNamespaceOperator` | `github.com/apache/airflow/go-sdk/providers/amazon/aws/operators/s3tables` |
| `S3TablesDeleteTableBucketOperator` | `github.com/apache/airflow/go-sdk/providers/amazon/aws/operators/s3tables` |
| `S3TablesDeleteTableBucketPolicyOperator` | `github.com/apache/airflow/go-sdk/providers/amazon/aws/operators/s3tables` |
| `S3TablesDeleteTableOperator` | `github.com/apache/airflow/go-sdk/providers/amazon/aws/operators/s3tables` |
| `S3TablesPutTableBucketPolicyOperator` | `github.com/apache/airflow/go-sdk/providers/amazon/aws/operators/s3tables` |
| `S3TablesRenameTableOperator` | `github.com/apache/airflow/go-sdk/providers/amazon/aws/operators/s3tables` |
| `S3VectorsCreateIndexOperator` | `github.com/apache/airflow/go-sdk/providers/amazon/aws/operators/s3vectors` |
| `S3VectorsCreateVectorBucketOperator` | `github.com/apache/airflow/go-sdk/providers/amazon/aws/operators/s3vectors` |
| `S3VectorsDeleteIndexOperator` | `github.com/apache/airflow/go-sdk/providers/amazon/aws/operators/s3vectors` |
| `S3VectorsDeleteVectorBucketOperator` | `github.com/apache/airflow/go-sdk/providers/amazon/aws/operators/s3vectors` |
| `S3VectorsPutVectorsOperator` | `github.com/apache/airflow/go-sdk/providers/amazon/aws/operators/s3vectors` |
| `S3VectorsQueryVectorsOperator` | `github.com/apache/airflow/go-sdk/providers/amazon/aws/operators/s3vectors` |
| `SageMakerAutoMLOperator` | `github.com/apache/airflow/go-sdk/providers/amazon/aws/operators/sagemaker` |
| `SageMakerBaseOperator` | `github.com/apache/airflow/go-sdk/providers/amazon/aws/operators/sagemaker` |
| `SageMakerConditionOperator` | `github.com/apache/airflow/go-sdk/providers/amazon/aws/operators/sagemaker` |
| `SageMakerCreateExperimentOperator` | `github.com/apache/airflow/go-sdk/providers/amazon/aws/operators/sagemaker` |
| `SageMakerCreateNotebookOperator` | `github.com/apache/airflow/go-sdk/providers/amazon/aws/operators/sagemaker` |
| `SageMakerDeleteModelOperator` | `github.com/apache/airflow/go-sdk/providers/amazon/aws/operators/sagemaker` |
| `SageMakerDeleteNotebookOperator` | `github.com/apache/airflow/go-sdk/providers/amazon/aws/operators/sagemaker` |
| `SageMakerEndpointConfigOperator` | `github.com/apache/airflow/go-sdk/providers/amazon/aws/operators/sagemaker` |
| `SageMakerEndpointOperator` | `github.com/apache/airflow/go-sdk/providers/amazon/aws/operators/sagemaker` |
| `SageMakerModelOperator` | `github.com/apache/airflow/go-sdk/providers/amazon/aws/operators/sagemaker` |
| `SageMakerProcessingOperator` | `github.com/apache/airflow/go-sdk/providers/amazon/aws/operators/sagemaker` |
| `SageMakerRegisterModelVersionOperator` | `github.com/apache/airflow/go-sdk/providers/amazon/aws/operators/sagemaker` |
| `SageMakerStartNoteBookOperator` | `github.com/apache/airflow/go-sdk/providers/amazon/aws/operators/sagemaker` |
| `SageMakerStartPipelineOperator` | `github.com/apache/airflow/go-sdk/providers/amazon/aws/operators/sagemaker` |
| `SageMakerStopNotebookOperator` | `github.com/apache/airflow/go-sdk/providers/amazon/aws/operators/sagemaker` |
| `SageMakerStopPipelineOperator` | `github.com/apache/airflow/go-sdk/providers/amazon/aws/operators/sagemaker` |
| `SageMakerTrainingOperator` | `github.com/apache/airflow/go-sdk/providers/amazon/aws/operators/sagemaker` |
| `SageMakerTransformOperator` | `github.com/apache/airflow/go-sdk/providers/amazon/aws/operators/sagemaker` |
| `SageMakerTuningOperator` | `github.com/apache/airflow/go-sdk/providers/amazon/aws/operators/sagemaker` |
| `SageMakerNotebookOperator` | `github.com/apache/airflow/go-sdk/providers/amazon/aws/operators/sagemakerunifiedstudio` |
| `SageMakerUnifiedStudioNotebookOperator` | `github.com/apache/airflow/go-sdk/providers/amazon/aws/operators/sagemakerunifiedstudionotebook` |
| `SesEmailOperator` | `github.com/apache/airflow/go-sdk/providers/amazon/aws/operators/ses` |
| `SnsPublishOperator` | `github.com/apache/airflow/go-sdk/providers/amazon/aws/operators/sns` |
| `SqsPublishOperator` | `github.com/apache/airflow/go-sdk/providers/amazon/aws/operators/sqs` |
| `SsmGetCommandInvocationOperator` | `github.com/apache/airflow/go-sdk/providers/amazon/aws/operators/ssm` |
| `SsmRunCommandOperator` | `github.com/apache/airflow/go-sdk/providers/amazon/aws/operators/ssm` |
| `StepFunctionGetExecutionOutputOperator` | `github.com/apache/airflow/go-sdk/providers/amazon/aws/operators/stepfunction` |
| `StepFunctionStartExecutionOperator` | `github.com/apache/airflow/go-sdk/providers/amazon/aws/operators/stepfunction` |
| `AthenaSensor` | `github.com/apache/airflow/go-sdk/providers/amazon/aws/sensors/athena` |
| `AthenaSparkSensor` | `github.com/apache/airflow/go-sdk/providers/amazon/aws/sensors/athenaspark` |
| `BatchComputeEnvironmentSensor` | `github.com/apache/airflow/go-sdk/providers/amazon/aws/sensors/batch` |
| `BatchJobQueueSensor` | `github.com/apache/airflow/go-sdk/providers/amazon/aws/sensors/batch` |
| `BatchSensor` | `github.com/apache/airflow/go-sdk/providers/amazon/aws/sensors/batch` |
| `BedrockBatchInferenceSensor` | `github.com/apache/airflow/go-sdk/providers/amazon/aws/sensors/bedrock` |
| `BedrockCustomizeModelCompletedSensor` | `github.com/apache/airflow/go-sdk/providers/amazon/aws/sensors/bedrock` |
| `BedrockIngestionJobSensor` | `github.com/apache/airflow/go-sdk/providers/amazon/aws/sensors/bedrock` |
| `BedrockKnowledgeBaseActiveSensor` | `github.com/apache/airflow/go-sdk/providers/amazon/aws/sensors/bedrock` |
| `BedrockProvisionModelThroughputCompletedSensor` | `github.com/apache/airflow/go-sdk/providers/amazon/aws/sensors/bedrock` |
| `CloudFormationCreateStackSensor` | `github.com/apache/airflow/go-sdk/providers/amazon/aws/sensors/cloudformation` |
| `CloudFormationDeleteStackSensor` | `github.com/apache/airflow/go-sdk/providers/amazon/aws/sensors/cloudformation` |
| `ComprehendCreateDocumentClassifierCompletedSensor` | `github.com/apache/airflow/go-sdk/providers/amazon/aws/sensors/comprehend` |
| `ComprehendStartPiiEntitiesDetectionJobCompletedSensor` | `github.com/apache/airflow/go-sdk/providers/amazon/aws/sensors/comprehend` |
| `DmsTaskBaseSensor` | `github.com/apache/airflow/go-sdk/providers/amazon/aws/sensors/dms` |
| `DmsTaskCompletedSensor` | `github.com/apache/airflow/go-sdk/providers/amazon/aws/sensors/dms` |
| `DynamoDBValueSensor` | `github.com/apache/airflow/go-sdk/providers/amazon/aws/sensors/dynamodb` |
| `EC2InstanceStateSensor` | `github.com/apache/airflow/go-sdk/providers/amazon/aws/sensors/ec2` |
| `EcsClusterStateSensor` | `github.com/apache/airflow/go-sdk/providers/amazon/aws/sensors/ecs` |
| `EcsTaskDefinitionStateSensor` | `github.com/apache/airflow/go-sdk/providers/amazon/aws/sensors/ecs` |
| `EcsTaskStateSensor` | `github.com/apache/airflow/go-sdk/providers/amazon/aws/sensors/ecs` |
| `EksClusterStateSensor` | `github.com/apache/airflow/go-sdk/providers/amazon/aws/sensors/eks` |
| `EksFargateProfileStateSensor` | `github.com/apache/airflow/go-sdk/providers/amazon/aws/sensors/eks` |
| `EksNodegroupStateSensor` | `github.com/apache/airflow/go-sdk/providers/amazon/aws/sensors/eks` |
| `EmrBaseSensor` | `github.com/apache/airflow/go-sdk/providers/amazon/aws/sensors/emr` |
| `EmrContainerSensor` | `github.com/apache/airflow/go-sdk/providers/amazon/aws/sensors/emr` |
| `EmrJobFlowSensor` | `github.com/apache/airflow/go-sdk/providers/amazon/aws/sensors/emr` |
| `EmrNotebookExecutionSensor` | `github.com/apache/airflow/go-sdk/providers/amazon/aws/sensors/emr` |
| `EmrServerlessApplicationSensor` | `github.com/apache/airflow/go-sdk/providers/amazon/aws/sensors/emr` |
| `EmrServerlessJobSensor` | `github.com/apache/airflow/go-sdk/providers/amazon/aws/sensors/emr` |
| `EmrStepSensor` | `github.com/apache/airflow/go-sdk/providers/amazon/aws/sensors/emr` |
| `GlacierJobOperationSensor` | `github.com/apache/airflow/go-sdk/providers/amazon/aws/sensors/glacier` |
| `GlueDataQualityRuleRecommendationRunSensor` | `github.com/apache/airflow/go-sdk/providers/amazon/aws/sensors/glue` |
| `GlueDataQualityRuleSetEvaluationRunSensor` | `github.com/apache/airflow/go-sdk/providers/amazon/aws/sensors/glue` |
| `GlueJobSensor` | `github.com/apache/airflow/go-sdk/providers/amazon/aws/sensors/glue` |
| `GlueCatalogPartitionSensor` | `github.com/apache/airflow/go-sdk/providers/amazon/aws/sensors/gluecatalogpartition` |
| `GlueCrawlerSensor` | `github.com/apache/airflow/go-sdk/providers/amazon/aws/sensors/gluecrawler` |
| `KinesisAnalyticsV2BaseSensor` | `github.com/apache/airflow/go-sdk/providers/amazon/aws/sensors/kinesisanalytics` |
| `KinesisAnalyticsV2StartApplicationCompletedSensor` | `github.com/apache/airflow/go-sdk/providers/amazon/aws/sensors/kinesisanalytics` |
| `KinesisAnalyticsV2StopApplicationCompletedSensor` | `github.com/apache/airflow/go-sdk/providers/amazon/aws/sensors/kinesisanalytics` |
| `LambdaFunctionStateSensor` | `github.com/apache/airflow/go-sdk/providers/amazon/aws/sensors/lambdafunction` |
| `MwaaDagRunSensor` | `github.com/apache/airflow/go-sdk/providers/amazon/aws/sensors/mwaa` |
| `MwaaTaskSensor` | `github.com/apache/airflow/go-sdk/providers/amazon/aws/sensors/mwaa` |
| `MwaaServerlessWorkflowRunSensor` | `github.com/apache/airflow/go-sdk/providers/amazon/aws/sensors/mwaaserverless` |
| `OpenSearchServerlessCollectionActiveSensor` | `github.com/apache/airflow/go-sdk/providers/amazon/aws/sensors/opensearchserverless` |
| `QuickSightSensor` | `github.com/apache/airflow/go-sdk/providers/amazon/aws/sensors/quicksight` |
| `RdsDbSensor` | `github.com/apache/airflow/go-sdk/providers/amazon/aws/sensors/rds` |
| `RdsExportTaskExistenceSensor` | `github.com/apache/airflow/go-sdk/providers/amazon/aws/sensors/rds` |
| `RdsSnapshotExistenceSensor` | `github.com/apache/airflow/go-sdk/providers/amazon/aws/sensors/rds` |
| `RedshiftClusterSensor` | `github.com/apache/airflow/go-sdk/providers/amazon/aws/sensors/redshiftcluster` |
| `S3KeySensor` | `github.com/apache/airflow/go-sdk/providers/amazon/aws/sensors/s3` |
| `S3KeysUnchangedSensor` | `github.com/apache/airflow/go-sdk/providers/amazon/aws/sensors/s3` |
| `SageMakerAutoMLSensor` | `github.com/apache/airflow/go-sdk/providers/amazon/aws/sensors/sagemaker` |
| `SageMakerBaseSensor` | `github.com/apache/airflow/go-sdk/providers/amazon/aws/sensors/sagemaker` |
| `SageMakerEndpointSensor` | `github.com/apache/airflow/go-sdk/providers/amazon/aws/sensors/sagemaker` |
| `SageMakerPipelineSensor` | `github.com/apache/airflow/go-sdk/providers/amazon/aws/sensors/sagemaker` |
| `SageMakerProcessingSensor` | `github.com/apache/airflow/go-sdk/providers/amazon/aws/sensors/sagemaker` |
| `SageMakerTrainingSensor` | `github.com/apache/airflow/go-sdk/providers/amazon/aws/sensors/sagemaker` |
| `SageMakerTransformSensor` | `github.com/apache/airflow/go-sdk/providers/amazon/aws/sensors/sagemaker` |
| `SageMakerTuningSensor` | `github.com/apache/airflow/go-sdk/providers/amazon/aws/sensors/sagemaker` |
| `SageMakerNotebookSensor` | `github.com/apache/airflow/go-sdk/providers/amazon/aws/sensors/sagemakerunifiedstudio` |
| `SageMakerUnifiedStudioNotebookSensor` | `github.com/apache/airflow/go-sdk/providers/amazon/aws/sensors/sagemakerunifiedstudionotebook` |
| `SqsSensor` | `github.com/apache/airflow/go-sdk/providers/amazon/aws/sensors/sqs` |
| `SsmRunCommandCompletedSensor` | `github.com/apache/airflow/go-sdk/providers/amazon/aws/sensors/ssm` |
| `StepFunctionExecutionSensor` | `github.com/apache/airflow/go-sdk/providers/amazon/aws/sensors/stepfunction` |
