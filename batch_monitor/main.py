import os
from aws_cdk import (
    Stack,
    Size,
    aws_batch as batch,
    aws_ec2 as ec2,
    aws_ecs as ecs,
    aws_stepfunctions as sfn,
    aws_stepfunctions_tasks as tasks,
    aws_sns as sns
)
from constructs import Construct


class MyStack(Stack):
  def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
    super().__init__(scope, construct_id, **kwargs)

    # Create a VPC
    vpc = ec2.Vpc(self, "MyVpc", max_azs=2)

    # Create an AWS Batch Compute Environment with ECS Fargate
    compute_env=self.create_batch_compute_env("MyBatchComputeEnv", vpc)
    # Create an AWS Batch Job Queue with ECS Fargate
    job_queue = self.create_batch_job_queue("MyBatchJobQueue", compute_env, 1)
    # Create an AWS Batch Job Definition with ECS Fargate
    job_definition = self.create_batch_job_definition("MyBatchJobDefinition", "MyBatchContainerDefinition")
    # Create an SNS Topic to send notifications
    topic = sns.Topic(self, "MySnsTopic")
    # Create a Step Function State Machine that starts an AWS Batch Job,
    # then monitors the job status and after a period of time triggers an SNS notification if the job is still running. 
    # Once the job is complete it outputs the job result.
    self.create_batch_state_machine("MyBatchStateMachine", job_definition, job_queue, topic)
    
  def create_batch_compute_env(self, name, vpc):
    return batch.FargateComputeEnvironment(self, name,
      vpc=vpc
    )
  
  # Create an AWS Batch Job Queue with ECS Fargate
  def create_batch_job_queue(self, name, compute_env, priority):
    return batch.JobQueue(self, name,
      compute_environments=[batch.OrderedComputeEnvironment(
          compute_environment=compute_env,
          order=1
      )],
      priority=priority
    )
  
  # Create an AWS Batch Job Definition with ECS Fargate
  def create_batch_job_definition(self, name, container_name):
    return batch.EcsJobDefinition(self, name,
      container=batch.EcsFargateContainerDefinition(self, container_name, 
        image=ecs.ContainerImage.from_registry("public.ecr.aws/amazonlinux/amazonlinux:latest"),
        command=["/bin/bash", "-c", "echo \"sleeping\" && sleep 300 && echo \"Hello world\""],
        memory=Size.mebibytes(1024),
        cpu=0.5,
        fargate_cpu_architecture=ecs.CpuArchitecture.ARM64
      )
    )
  
  # Create a Step Functions State Machine that starts an AWS Batch Job,
  # then monitors the job status and after a period of time triggers an SNS notification if the job is still running.
  # Once the job is complete it outputs the job result.
  def create_batch_state_machine(self, name, job_definition, job_queue, topic):
    # Step Functions task to submit an AWS Batch Job
    submit_job = tasks.BatchSubmitJob(self, "MyBatchSubmitJobTask",
      job_definition_arn=job_definition.job_definition_arn ,
      # job_name="MyBatchJob",
      job_name=sfn.JsonPath.string_at("$.jobName"),
      job_queue_arn=job_queue.job_queue_arn,
      result_path="$.guid",
      result_selector={
        "jobId.$": "$.JobId"
      },
      integration_pattern=sfn.IntegrationPattern.REQUEST_RESPONSE
    )

    # Step Functions step to wait X seconds
    wait_x = sfn.Wait(self, "Wait X Seconds",
      time=sfn.WaitTime.seconds_path("$.wait_time")
    )

    # Step Functions task to describe an AWS Batch Job
    describe_job = tasks.CallAwsService(self, "GetJobStatus",
      service="batch",
      action="describeJobs",
      parameters={
        "Jobs": sfn.JsonPath.array(sfn.JsonPath.string_at("$.jobId"))
      },
      input_path="$.guid",
      result_path="$.jobStatus",
      result_selector={
        "status.$": "$.Jobs[0].Status",
        "startedAt.$": "$.Jobs[0].StartedAt"
      },
      iam_resources=["*"]
    )

    # If the job failed, error. If the job succeded, success. Otherwise notify the job is still running
    job_failed = sfn.Fail(self, "Job Failed",
      cause="AWS Batch Job Failed",
      error="DescribeJob returned FAILED"
    )
    job_succeeded = sfn.Succeed(self, "Job Succeeded",
      comment="AWS Batch Job Succeeded"
    )
    notify_job_running = tasks.SnsPublish(self, "Notify Job Still Running",
      topic=topic,
      integration_pattern=sfn.IntegrationPattern.REQUEST_RESPONSE,
      message=sfn.TaskInput.from_object({
        "guid.$": "$.guid",
        "jobName.$": "$.jobName",
        "startedAt.$": "$.jobStatus.startedAt"
      }),
      result_path=sfn.JsonPath.DISCARD
    )
    choice = sfn.Choice(self, "Job Complete?")
    choice.when(sfn.Condition.string_equals("$.jobStatus.status", "FAILED"), job_failed)
    choice.when(sfn.Condition.string_equals("$.jobStatus.status", "SUCCEEDED"), job_succeeded)
    choice.otherwise(notify_job_running)
    choice.afterwards().next(wait_x)
    chain = sfn.Chain.start(submit_job).next(wait_x).next(describe_job).next(choice)
    
    return sfn.StateMachine(self, name,
      definition_body=sfn.DefinitionBody.from_chainable(chain),
      comment="An example of the Amazon States Language that runs an AWS Batch job and monitors the job until it completes."
    )
