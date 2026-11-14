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

# @apache-airflow/providers-amazon

The operators and sensors of `apache-airflow-providers-amazon` 9.40.0, for a Dag written with
[`@apache-airflow/ts-sdk`](https://www.npmjs.com/package/@apache-airflow/ts-sdk).

A binding has no task body. It records the Python operator class and its arguments in the
Dag, and a Python worker that has the provider installed runs the operator. Such a task does
not take the Dag's queue: give it one in its task options when the default queue is not
served by Python workers.

```ts
import { Dag } from "@apache-airflow/ts-sdk";
import { AppflowBaseOperator } from "@apache-airflow/providers-amazon/aws/operators/appflow";

const dag = new Dag("example");
dag.task("my_task", AppflowBaseOperator({ /* ... */ }))();
```

A module mirrors the Python module of the same path, and an argument is the Python
argument in camel case. A templated argument takes a Jinja template, or the result of
another task. A duration is a number of seconds.

This package is generated. To change it, regenerate the operator specs and the packages:

```bash
python3 scripts/ci/lang_sdk_providers/generate_operator_specs.py --allow-list amazon
```

```bash
pnpm --dir ts-sdk run generate:providers
```

## Operators

| Operator | Module |
| --- | --- |
| `AppflowBaseOperator` | `@apache-airflow/providers-amazon/aws/operators/appflow` |
| `AppflowRecordsShortCircuitOperator` | `@apache-airflow/providers-amazon/aws/operators/appflow` |
| `AppflowRunAfterOperator` | `@apache-airflow/providers-amazon/aws/operators/appflow` |
| `AppflowRunBeforeOperator` | `@apache-airflow/providers-amazon/aws/operators/appflow` |
| `AppflowRunDailyOperator` | `@apache-airflow/providers-amazon/aws/operators/appflow` |
| `AppflowRunFullOperator` | `@apache-airflow/providers-amazon/aws/operators/appflow` |
| `AppflowRunOperator` | `@apache-airflow/providers-amazon/aws/operators/appflow` |
| `AthenaOperator` | `@apache-airflow/providers-amazon/aws/operators/athena` |
| `AthenaSparkOperator` | `@apache-airflow/providers-amazon/aws/operators/athena_spark` |
| `BatchCreateComputeEnvironmentOperator` | `@apache-airflow/providers-amazon/aws/operators/batch` |
| `BatchOperator` | `@apache-airflow/providers-amazon/aws/operators/batch` |
| `BedrockBatchInferenceOperator` | `@apache-airflow/providers-amazon/aws/operators/bedrock` |
| `BedrockCreateAgentRuntimeOperator` | `@apache-airflow/providers-amazon/aws/operators/bedrock` |
| `BedrockCreateDataSourceOperator` | `@apache-airflow/providers-amazon/aws/operators/bedrock` |
| `BedrockCreateEvaluationJobOperator` | `@apache-airflow/providers-amazon/aws/operators/bedrock` |
| `BedrockCreateGuardrailOperator` | `@apache-airflow/providers-amazon/aws/operators/bedrock` |
| `BedrockCreateGuardrailVersionOperator` | `@apache-airflow/providers-amazon/aws/operators/bedrock` |
| `BedrockCreateKnowledgeBaseOperator` | `@apache-airflow/providers-amazon/aws/operators/bedrock` |
| `BedrockCreateProvisionedModelThroughputOperator` | `@apache-airflow/providers-amazon/aws/operators/bedrock` |
| `BedrockCustomizeModelOperator` | `@apache-airflow/providers-amazon/aws/operators/bedrock` |
| `BedrockDeleteAgentRuntimeOperator` | `@apache-airflow/providers-amazon/aws/operators/bedrock` |
| `BedrockDeleteGuardrailOperator` | `@apache-airflow/providers-amazon/aws/operators/bedrock` |
| `BedrockIngestDataOperator` | `@apache-airflow/providers-amazon/aws/operators/bedrock` |
| `BedrockInvokeAgentRuntimeOperator` | `@apache-airflow/providers-amazon/aws/operators/bedrock` |
| `BedrockInvokeModelOperator` | `@apache-airflow/providers-amazon/aws/operators/bedrock` |
| `BedrockRaGOperator` | `@apache-airflow/providers-amazon/aws/operators/bedrock` |
| `BedrockRetrieveOperator` | `@apache-airflow/providers-amazon/aws/operators/bedrock` |
| `BedrockUpdateGuardrailOperator` | `@apache-airflow/providers-amazon/aws/operators/bedrock` |
| `CloudFormationCreateStackOperator` | `@apache-airflow/providers-amazon/aws/operators/cloud_formation` |
| `CloudFormationDeleteStackOperator` | `@apache-airflow/providers-amazon/aws/operators/cloud_formation` |
| `ComprehendBaseOperator` | `@apache-airflow/providers-amazon/aws/operators/comprehend` |
| `ComprehendCreateDocumentClassifierOperator` | `@apache-airflow/providers-amazon/aws/operators/comprehend` |
| `ComprehendStartPiiEntitiesDetectionJobOperator` | `@apache-airflow/providers-amazon/aws/operators/comprehend` |
| `DataSyncOperator` | `@apache-airflow/providers-amazon/aws/operators/datasync` |
| `DmsCreateReplicationConfigOperator` | `@apache-airflow/providers-amazon/aws/operators/dms` |
| `DmsCreateTaskOperator` | `@apache-airflow/providers-amazon/aws/operators/dms` |
| `DmsDeleteReplicationConfigOperator` | `@apache-airflow/providers-amazon/aws/operators/dms` |
| `DmsDeleteTaskOperator` | `@apache-airflow/providers-amazon/aws/operators/dms` |
| `DmsDescribeReplicationConfigsOperator` | `@apache-airflow/providers-amazon/aws/operators/dms` |
| `DmsDescribeReplicationsOperator` | `@apache-airflow/providers-amazon/aws/operators/dms` |
| `DmsDescribeTasksOperator` | `@apache-airflow/providers-amazon/aws/operators/dms` |
| `DmsModifyTaskOperator` | `@apache-airflow/providers-amazon/aws/operators/dms` |
| `DmsReloadTablesOperator` | `@apache-airflow/providers-amazon/aws/operators/dms` |
| `DmsStartReplicationOperator` | `@apache-airflow/providers-amazon/aws/operators/dms` |
| `DmsStartTaskOperator` | `@apache-airflow/providers-amazon/aws/operators/dms` |
| `DmsStopReplicationOperator` | `@apache-airflow/providers-amazon/aws/operators/dms` |
| `DmsStopTaskOperator` | `@apache-airflow/providers-amazon/aws/operators/dms` |
| `EC2CreateInstanceOperator` | `@apache-airflow/providers-amazon/aws/operators/ec2` |
| `EC2HibernateInstanceOperator` | `@apache-airflow/providers-amazon/aws/operators/ec2` |
| `EC2RebootInstanceOperator` | `@apache-airflow/providers-amazon/aws/operators/ec2` |
| `EC2StartInstanceOperator` | `@apache-airflow/providers-amazon/aws/operators/ec2` |
| `EC2StopInstanceOperator` | `@apache-airflow/providers-amazon/aws/operators/ec2` |
| `EC2TerminateInstanceOperator` | `@apache-airflow/providers-amazon/aws/operators/ec2` |
| `EcrCreateRepositoryOperator` | `@apache-airflow/providers-amazon/aws/operators/ecr` |
| `EcrDeleteRepositoryOperator` | `@apache-airflow/providers-amazon/aws/operators/ecr` |
| `EcrSetRepositoryPolicyOperator` | `@apache-airflow/providers-amazon/aws/operators/ecr` |
| `EcsBaseOperator` | `@apache-airflow/providers-amazon/aws/operators/ecs` |
| `EcsCreateClusterOperator` | `@apache-airflow/providers-amazon/aws/operators/ecs` |
| `EcsDeleteClusterOperator` | `@apache-airflow/providers-amazon/aws/operators/ecs` |
| `EcsDeregisterTaskDefinitionOperator` | `@apache-airflow/providers-amazon/aws/operators/ecs` |
| `EcsRegisterTaskDefinitionOperator` | `@apache-airflow/providers-amazon/aws/operators/ecs` |
| `EcsRunTaskOperator` | `@apache-airflow/providers-amazon/aws/operators/ecs` |
| `EksCreateClusterOperator` | `@apache-airflow/providers-amazon/aws/operators/eks` |
| `EksCreateFargateProfileOperator` | `@apache-airflow/providers-amazon/aws/operators/eks` |
| `EksCreateNodegroupOperator` | `@apache-airflow/providers-amazon/aws/operators/eks` |
| `EksDeleteClusterOperator` | `@apache-airflow/providers-amazon/aws/operators/eks` |
| `EksDeleteFargateProfileOperator` | `@apache-airflow/providers-amazon/aws/operators/eks` |
| `EksDeleteNodegroupOperator` | `@apache-airflow/providers-amazon/aws/operators/eks` |
| `EksPodExecOperator` | `@apache-airflow/providers-amazon/aws/operators/eks` |
| `EksPodOperator` | `@apache-airflow/providers-amazon/aws/operators/eks` |
| `EmrAddStepsOperator` | `@apache-airflow/providers-amazon/aws/operators/emr` |
| `EmrContainerOperator` | `@apache-airflow/providers-amazon/aws/operators/emr` |
| `EmrCreateJobFlowOperator` | `@apache-airflow/providers-amazon/aws/operators/emr` |
| `EmrEksCreateClusterOperator` | `@apache-airflow/providers-amazon/aws/operators/emr` |
| `EmrModifyClusterOperator` | `@apache-airflow/providers-amazon/aws/operators/emr` |
| `EmrServerlessCreateApplicationOperator` | `@apache-airflow/providers-amazon/aws/operators/emr` |
| `EmrServerlessDeleteApplicationOperator` | `@apache-airflow/providers-amazon/aws/operators/emr` |
| `EmrServerlessGetSessionEndpointOperator` | `@apache-airflow/providers-amazon/aws/operators/emr` |
| `EmrServerlessStartJobOperator` | `@apache-airflow/providers-amazon/aws/operators/emr` |
| `EmrServerlessStartSessionOperator` | `@apache-airflow/providers-amazon/aws/operators/emr` |
| `EmrServerlessStopApplicationOperator` | `@apache-airflow/providers-amazon/aws/operators/emr` |
| `EmrServerlessStopSessionOperator` | `@apache-airflow/providers-amazon/aws/operators/emr` |
| `EmrStartNotebookExecutionOperator` | `@apache-airflow/providers-amazon/aws/operators/emr` |
| `EmrStopNotebookExecutionOperator` | `@apache-airflow/providers-amazon/aws/operators/emr` |
| `EmrTerminateJobFlowOperator` | `@apache-airflow/providers-amazon/aws/operators/emr` |
| `EventBridgeDisableRuleOperator` | `@apache-airflow/providers-amazon/aws/operators/eventbridge` |
| `EventBridgeEnableRuleOperator` | `@apache-airflow/providers-amazon/aws/operators/eventbridge` |
| `EventBridgePutEventsOperator` | `@apache-airflow/providers-amazon/aws/operators/eventbridge` |
| `EventBridgePutRuleOperator` | `@apache-airflow/providers-amazon/aws/operators/eventbridge` |
| `GlacierCreateJobOperator` | `@apache-airflow/providers-amazon/aws/operators/glacier` |
| `GlacierUploadArchiveOperator` | `@apache-airflow/providers-amazon/aws/operators/glacier` |
| `GlueDataQualityOperator` | `@apache-airflow/providers-amazon/aws/operators/glue` |
| `GlueDataQualityRuleRecommendationRunOperator` | `@apache-airflow/providers-amazon/aws/operators/glue` |
| `GlueDataQualityRuleSetEvaluationRunOperator` | `@apache-airflow/providers-amazon/aws/operators/glue` |
| `GlueJobOperator` | `@apache-airflow/providers-amazon/aws/operators/glue` |
| `GlueCatalogBatchDeletePartitionOperator` | `@apache-airflow/providers-amazon/aws/operators/glue_catalog` |
| `GlueCatalogCreateDatabaseOperator` | `@apache-airflow/providers-amazon/aws/operators/glue_catalog` |
| `GlueCatalogCreatePartitionOperator` | `@apache-airflow/providers-amazon/aws/operators/glue_catalog` |
| `GlueCatalogCreateTableOperator` | `@apache-airflow/providers-amazon/aws/operators/glue_catalog` |
| `GlueCatalogDeleteDatabaseOperator` | `@apache-airflow/providers-amazon/aws/operators/glue_catalog` |
| `GlueCatalogDeleteTableOperator` | `@apache-airflow/providers-amazon/aws/operators/glue_catalog` |
| `GlueCrawlerCreateOperator` | `@apache-airflow/providers-amazon/aws/operators/glue_crawler` |
| `GlueCrawlerDeleteOperator` | `@apache-airflow/providers-amazon/aws/operators/glue_crawler` |
| `GlueCrawlerOperator` | `@apache-airflow/providers-amazon/aws/operators/glue_crawler` |
| `GlueCrawlerRunOperator` | `@apache-airflow/providers-amazon/aws/operators/glue_crawler` |
| `GlueCrawlerUpdateOperator` | `@apache-airflow/providers-amazon/aws/operators/glue_crawler` |
| `GlueDataBrewStartJobOperator` | `@apache-airflow/providers-amazon/aws/operators/glue_databrew` |
| `KinesisAnalyticsV2CreateApplicationOperator` | `@apache-airflow/providers-amazon/aws/operators/kinesis_analytics` |
| `KinesisAnalyticsV2StartApplicationOperator` | `@apache-airflow/providers-amazon/aws/operators/kinesis_analytics` |
| `KinesisAnalyticsV2StopApplicationOperator` | `@apache-airflow/providers-amazon/aws/operators/kinesis_analytics` |
| `LambdaCreateFunctionOperator` | `@apache-airflow/providers-amazon/aws/operators/lambda_function` |
| `LambdaInvokeFunctionOperator` | `@apache-airflow/providers-amazon/aws/operators/lambda_function` |
| `MwaaTriggerDagRunOperator` | `@apache-airflow/providers-amazon/aws/operators/mwaa` |
| `MwaaServerlessCreateWorkflowOperator` | `@apache-airflow/providers-amazon/aws/operators/mwaa_serverless` |
| `MwaaServerlessDeleteWorkflowOperator` | `@apache-airflow/providers-amazon/aws/operators/mwaa_serverless` |
| `MwaaServerlessStartWorkflowRunOperator` | `@apache-airflow/providers-amazon/aws/operators/mwaa_serverless` |
| `MwaaServerlessStopWorkflowRunOperator` | `@apache-airflow/providers-amazon/aws/operators/mwaa_serverless` |
| `MwaaServerlessUpdateWorkflowOperator` | `@apache-airflow/providers-amazon/aws/operators/mwaa_serverless` |
| `NeptuneStartDbClusterOperator` | `@apache-airflow/providers-amazon/aws/operators/neptune` |
| `NeptuneStopDbClusterOperator` | `@apache-airflow/providers-amazon/aws/operators/neptune` |
| `NeptuneCancelImportTaskOperator` | `@apache-airflow/providers-amazon/aws/operators/neptune_analytics` |
| `NeptuneCreateGraphOperator` | `@apache-airflow/providers-amazon/aws/operators/neptune_analytics` |
| `NeptuneCreateGraphWithImportOperator` | `@apache-airflow/providers-amazon/aws/operators/neptune_analytics` |
| `NeptuneCreatePrivateGraphEndpointOperator` | `@apache-airflow/providers-amazon/aws/operators/neptune_analytics` |
| `NeptuneDeleteGraphOperator` | `@apache-airflow/providers-amazon/aws/operators/neptune_analytics` |
| `NeptuneDeletePrivateGraphEndpointOperator` | `@apache-airflow/providers-amazon/aws/operators/neptune_analytics` |
| `NeptuneStartImportTaskOperator` | `@apache-airflow/providers-amazon/aws/operators/neptune_analytics` |
| `OpenSearchServerlessCreateCollectionOperator` | `@apache-airflow/providers-amazon/aws/operators/opensearch_serverless` |
| `QuickSightCreateIngestionOperator` | `@apache-airflow/providers-amazon/aws/operators/quicksight` |
| `RdsBaseOperator` | `@apache-airflow/providers-amazon/aws/operators/rds` |
| `RdsCancelExportTaskOperator` | `@apache-airflow/providers-amazon/aws/operators/rds` |
| `RdsCopyDbSnapshotOperator` | `@apache-airflow/providers-amazon/aws/operators/rds` |
| `RdsCreateDbInstanceOperator` | `@apache-airflow/providers-amazon/aws/operators/rds` |
| `RdsCreateDbSnapshotOperator` | `@apache-airflow/providers-amazon/aws/operators/rds` |
| `RdsCreateEventSubscriptionOperator` | `@apache-airflow/providers-amazon/aws/operators/rds` |
| `RdsDeleteDbInstanceOperator` | `@apache-airflow/providers-amazon/aws/operators/rds` |
| `RdsDeleteDbSnapshotOperator` | `@apache-airflow/providers-amazon/aws/operators/rds` |
| `RdsDeleteEventSubscriptionOperator` | `@apache-airflow/providers-amazon/aws/operators/rds` |
| `RdsStartDbOperator` | `@apache-airflow/providers-amazon/aws/operators/rds` |
| `RdsStartExportTaskOperator` | `@apache-airflow/providers-amazon/aws/operators/rds` |
| `RdsStopDbOperator` | `@apache-airflow/providers-amazon/aws/operators/rds` |
| `RedshiftCreateClusterOperator` | `@apache-airflow/providers-amazon/aws/operators/redshift_cluster` |
| `RedshiftCreateClusterSnapshotOperator` | `@apache-airflow/providers-amazon/aws/operators/redshift_cluster` |
| `RedshiftDeleteClusterOperator` | `@apache-airflow/providers-amazon/aws/operators/redshift_cluster` |
| `RedshiftDeleteClusterSnapshotOperator` | `@apache-airflow/providers-amazon/aws/operators/redshift_cluster` |
| `RedshiftPauseClusterOperator` | `@apache-airflow/providers-amazon/aws/operators/redshift_cluster` |
| `RedshiftResumeClusterOperator` | `@apache-airflow/providers-amazon/aws/operators/redshift_cluster` |
| `RedshiftDataOperator` | `@apache-airflow/providers-amazon/aws/operators/redshift_data` |
| `S3CopyObjectOperator` | `@apache-airflow/providers-amazon/aws/operators/s3` |
| `S3CopyPrefixOperator` | `@apache-airflow/providers-amazon/aws/operators/s3` |
| `S3CreateBucketOperator` | `@apache-airflow/providers-amazon/aws/operators/s3` |
| `S3CreateObjectOperator` | `@apache-airflow/providers-amazon/aws/operators/s3` |
| `S3DeleteBucketOperator` | `@apache-airflow/providers-amazon/aws/operators/s3` |
| `S3DeleteBucketTaggingOperator` | `@apache-airflow/providers-amazon/aws/operators/s3` |
| `S3DeleteObjectsOperator` | `@apache-airflow/providers-amazon/aws/operators/s3` |
| `S3FileTransformOperator` | `@apache-airflow/providers-amazon/aws/operators/s3` |
| `S3GetBucketTaggingOperator` | `@apache-airflow/providers-amazon/aws/operators/s3` |
| `S3ListOperator` | `@apache-airflow/providers-amazon/aws/operators/s3` |
| `S3ListPrefixesOperator` | `@apache-airflow/providers-amazon/aws/operators/s3` |
| `S3PutBucketTaggingOperator` | `@apache-airflow/providers-amazon/aws/operators/s3` |
| `S3ReadObjectOperator` | `@apache-airflow/providers-amazon/aws/operators/s3` |
| `S3TablesCreateNamespaceOperator` | `@apache-airflow/providers-amazon/aws/operators/s3_tables` |
| `S3TablesCreateTableBucketOperator` | `@apache-airflow/providers-amazon/aws/operators/s3_tables` |
| `S3TablesCreateTableOperator` | `@apache-airflow/providers-amazon/aws/operators/s3_tables` |
| `S3TablesDeleteNamespaceOperator` | `@apache-airflow/providers-amazon/aws/operators/s3_tables` |
| `S3TablesDeleteTableBucketOperator` | `@apache-airflow/providers-amazon/aws/operators/s3_tables` |
| `S3TablesDeleteTableBucketPolicyOperator` | `@apache-airflow/providers-amazon/aws/operators/s3_tables` |
| `S3TablesDeleteTableOperator` | `@apache-airflow/providers-amazon/aws/operators/s3_tables` |
| `S3TablesPutTableBucketPolicyOperator` | `@apache-airflow/providers-amazon/aws/operators/s3_tables` |
| `S3TablesRenameTableOperator` | `@apache-airflow/providers-amazon/aws/operators/s3_tables` |
| `S3VectorsCreateIndexOperator` | `@apache-airflow/providers-amazon/aws/operators/s3_vectors` |
| `S3VectorsCreateVectorBucketOperator` | `@apache-airflow/providers-amazon/aws/operators/s3_vectors` |
| `S3VectorsDeleteIndexOperator` | `@apache-airflow/providers-amazon/aws/operators/s3_vectors` |
| `S3VectorsDeleteVectorBucketOperator` | `@apache-airflow/providers-amazon/aws/operators/s3_vectors` |
| `S3VectorsPutVectorsOperator` | `@apache-airflow/providers-amazon/aws/operators/s3_vectors` |
| `S3VectorsQueryVectorsOperator` | `@apache-airflow/providers-amazon/aws/operators/s3_vectors` |
| `SageMakerAutoMLOperator` | `@apache-airflow/providers-amazon/aws/operators/sagemaker` |
| `SageMakerBaseOperator` | `@apache-airflow/providers-amazon/aws/operators/sagemaker` |
| `SageMakerConditionOperator` | `@apache-airflow/providers-amazon/aws/operators/sagemaker` |
| `SageMakerCreateExperimentOperator` | `@apache-airflow/providers-amazon/aws/operators/sagemaker` |
| `SageMakerCreateNotebookOperator` | `@apache-airflow/providers-amazon/aws/operators/sagemaker` |
| `SageMakerDeleteModelOperator` | `@apache-airflow/providers-amazon/aws/operators/sagemaker` |
| `SageMakerDeleteNotebookOperator` | `@apache-airflow/providers-amazon/aws/operators/sagemaker` |
| `SageMakerEndpointConfigOperator` | `@apache-airflow/providers-amazon/aws/operators/sagemaker` |
| `SageMakerEndpointOperator` | `@apache-airflow/providers-amazon/aws/operators/sagemaker` |
| `SageMakerModelOperator` | `@apache-airflow/providers-amazon/aws/operators/sagemaker` |
| `SageMakerProcessingOperator` | `@apache-airflow/providers-amazon/aws/operators/sagemaker` |
| `SageMakerRegisterModelVersionOperator` | `@apache-airflow/providers-amazon/aws/operators/sagemaker` |
| `SageMakerStartNoteBookOperator` | `@apache-airflow/providers-amazon/aws/operators/sagemaker` |
| `SageMakerStartPipelineOperator` | `@apache-airflow/providers-amazon/aws/operators/sagemaker` |
| `SageMakerStopNotebookOperator` | `@apache-airflow/providers-amazon/aws/operators/sagemaker` |
| `SageMakerStopPipelineOperator` | `@apache-airflow/providers-amazon/aws/operators/sagemaker` |
| `SageMakerTrainingOperator` | `@apache-airflow/providers-amazon/aws/operators/sagemaker` |
| `SageMakerTransformOperator` | `@apache-airflow/providers-amazon/aws/operators/sagemaker` |
| `SageMakerTuningOperator` | `@apache-airflow/providers-amazon/aws/operators/sagemaker` |
| `SageMakerNotebookOperator` | `@apache-airflow/providers-amazon/aws/operators/sagemaker_unified_studio` |
| `SageMakerUnifiedStudioNotebookOperator` | `@apache-airflow/providers-amazon/aws/operators/sagemaker_unified_studio_notebook` |
| `SesEmailOperator` | `@apache-airflow/providers-amazon/aws/operators/ses` |
| `SnsPublishOperator` | `@apache-airflow/providers-amazon/aws/operators/sns` |
| `SqsPublishOperator` | `@apache-airflow/providers-amazon/aws/operators/sqs` |
| `SsmGetCommandInvocationOperator` | `@apache-airflow/providers-amazon/aws/operators/ssm` |
| `SsmRunCommandOperator` | `@apache-airflow/providers-amazon/aws/operators/ssm` |
| `StepFunctionGetExecutionOutputOperator` | `@apache-airflow/providers-amazon/aws/operators/step_function` |
| `StepFunctionStartExecutionOperator` | `@apache-airflow/providers-amazon/aws/operators/step_function` |
| `AthenaSensor` | `@apache-airflow/providers-amazon/aws/sensors/athena` |
| `AthenaSparkSensor` | `@apache-airflow/providers-amazon/aws/sensors/athena_spark` |
| `BatchComputeEnvironmentSensor` | `@apache-airflow/providers-amazon/aws/sensors/batch` |
| `BatchJobQueueSensor` | `@apache-airflow/providers-amazon/aws/sensors/batch` |
| `BatchSensor` | `@apache-airflow/providers-amazon/aws/sensors/batch` |
| `BedrockBatchInferenceSensor` | `@apache-airflow/providers-amazon/aws/sensors/bedrock` |
| `BedrockCustomizeModelCompletedSensor` | `@apache-airflow/providers-amazon/aws/sensors/bedrock` |
| `BedrockIngestionJobSensor` | `@apache-airflow/providers-amazon/aws/sensors/bedrock` |
| `BedrockKnowledgeBaseActiveSensor` | `@apache-airflow/providers-amazon/aws/sensors/bedrock` |
| `BedrockProvisionModelThroughputCompletedSensor` | `@apache-airflow/providers-amazon/aws/sensors/bedrock` |
| `CloudFormationCreateStackSensor` | `@apache-airflow/providers-amazon/aws/sensors/cloud_formation` |
| `CloudFormationDeleteStackSensor` | `@apache-airflow/providers-amazon/aws/sensors/cloud_formation` |
| `ComprehendCreateDocumentClassifierCompletedSensor` | `@apache-airflow/providers-amazon/aws/sensors/comprehend` |
| `ComprehendStartPiiEntitiesDetectionJobCompletedSensor` | `@apache-airflow/providers-amazon/aws/sensors/comprehend` |
| `DmsTaskBaseSensor` | `@apache-airflow/providers-amazon/aws/sensors/dms` |
| `DmsTaskCompletedSensor` | `@apache-airflow/providers-amazon/aws/sensors/dms` |
| `DynamoDBValueSensor` | `@apache-airflow/providers-amazon/aws/sensors/dynamodb` |
| `EC2InstanceStateSensor` | `@apache-airflow/providers-amazon/aws/sensors/ec2` |
| `EcsClusterStateSensor` | `@apache-airflow/providers-amazon/aws/sensors/ecs` |
| `EcsTaskDefinitionStateSensor` | `@apache-airflow/providers-amazon/aws/sensors/ecs` |
| `EcsTaskStateSensor` | `@apache-airflow/providers-amazon/aws/sensors/ecs` |
| `EksClusterStateSensor` | `@apache-airflow/providers-amazon/aws/sensors/eks` |
| `EksFargateProfileStateSensor` | `@apache-airflow/providers-amazon/aws/sensors/eks` |
| `EksNodegroupStateSensor` | `@apache-airflow/providers-amazon/aws/sensors/eks` |
| `EmrBaseSensor` | `@apache-airflow/providers-amazon/aws/sensors/emr` |
| `EmrContainerSensor` | `@apache-airflow/providers-amazon/aws/sensors/emr` |
| `EmrJobFlowSensor` | `@apache-airflow/providers-amazon/aws/sensors/emr` |
| `EmrNotebookExecutionSensor` | `@apache-airflow/providers-amazon/aws/sensors/emr` |
| `EmrServerlessApplicationSensor` | `@apache-airflow/providers-amazon/aws/sensors/emr` |
| `EmrServerlessJobSensor` | `@apache-airflow/providers-amazon/aws/sensors/emr` |
| `EmrStepSensor` | `@apache-airflow/providers-amazon/aws/sensors/emr` |
| `GlacierJobOperationSensor` | `@apache-airflow/providers-amazon/aws/sensors/glacier` |
| `GlueDataQualityRuleRecommendationRunSensor` | `@apache-airflow/providers-amazon/aws/sensors/glue` |
| `GlueDataQualityRuleSetEvaluationRunSensor` | `@apache-airflow/providers-amazon/aws/sensors/glue` |
| `GlueJobSensor` | `@apache-airflow/providers-amazon/aws/sensors/glue` |
| `GlueCatalogPartitionSensor` | `@apache-airflow/providers-amazon/aws/sensors/glue_catalog_partition` |
| `GlueCrawlerSensor` | `@apache-airflow/providers-amazon/aws/sensors/glue_crawler` |
| `KinesisAnalyticsV2BaseSensor` | `@apache-airflow/providers-amazon/aws/sensors/kinesis_analytics` |
| `KinesisAnalyticsV2StartApplicationCompletedSensor` | `@apache-airflow/providers-amazon/aws/sensors/kinesis_analytics` |
| `KinesisAnalyticsV2StopApplicationCompletedSensor` | `@apache-airflow/providers-amazon/aws/sensors/kinesis_analytics` |
| `LambdaFunctionStateSensor` | `@apache-airflow/providers-amazon/aws/sensors/lambda_function` |
| `MwaaDagRunSensor` | `@apache-airflow/providers-amazon/aws/sensors/mwaa` |
| `MwaaTaskSensor` | `@apache-airflow/providers-amazon/aws/sensors/mwaa` |
| `MwaaServerlessWorkflowRunSensor` | `@apache-airflow/providers-amazon/aws/sensors/mwaa_serverless` |
| `OpenSearchServerlessCollectionActiveSensor` | `@apache-airflow/providers-amazon/aws/sensors/opensearch_serverless` |
| `QuickSightSensor` | `@apache-airflow/providers-amazon/aws/sensors/quicksight` |
| `RdsDbSensor` | `@apache-airflow/providers-amazon/aws/sensors/rds` |
| `RdsExportTaskExistenceSensor` | `@apache-airflow/providers-amazon/aws/sensors/rds` |
| `RdsSnapshotExistenceSensor` | `@apache-airflow/providers-amazon/aws/sensors/rds` |
| `RedshiftClusterSensor` | `@apache-airflow/providers-amazon/aws/sensors/redshift_cluster` |
| `S3KeySensor` | `@apache-airflow/providers-amazon/aws/sensors/s3` |
| `S3KeysUnchangedSensor` | `@apache-airflow/providers-amazon/aws/sensors/s3` |
| `SageMakerAutoMLSensor` | `@apache-airflow/providers-amazon/aws/sensors/sagemaker` |
| `SageMakerBaseSensor` | `@apache-airflow/providers-amazon/aws/sensors/sagemaker` |
| `SageMakerEndpointSensor` | `@apache-airflow/providers-amazon/aws/sensors/sagemaker` |
| `SageMakerPipelineSensor` | `@apache-airflow/providers-amazon/aws/sensors/sagemaker` |
| `SageMakerProcessingSensor` | `@apache-airflow/providers-amazon/aws/sensors/sagemaker` |
| `SageMakerTrainingSensor` | `@apache-airflow/providers-amazon/aws/sensors/sagemaker` |
| `SageMakerTransformSensor` | `@apache-airflow/providers-amazon/aws/sensors/sagemaker` |
| `SageMakerTuningSensor` | `@apache-airflow/providers-amazon/aws/sensors/sagemaker` |
| `SageMakerNotebookSensor` | `@apache-airflow/providers-amazon/aws/sensors/sagemaker_unified_studio` |
| `SageMakerUnifiedStudioNotebookSensor` | `@apache-airflow/providers-amazon/aws/sensors/sagemaker_unified_studio_notebook` |
| `SqsSensor` | `@apache-airflow/providers-amazon/aws/sensors/sqs` |
| `SsmRunCommandCompletedSensor` | `@apache-airflow/providers-amazon/aws/sensors/ssm` |
| `StepFunctionExecutionSensor` | `@apache-airflow/providers-amazon/aws/sensors/step_function` |
