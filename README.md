# spring-batch-remote-k8s-paritition-example (ATTEMPT #2 - PartitionerHandler + TaskLauncher with `@StepScope`)

Detail issue can be found in 

https://stackoverflow.com/posts/68647761


 1. Define PartitionerHandler + TaskLauncher with @StepScope

Result:
```
java.lang.NullPointerException: null
	at org.springframework.cloud.task.batch.partition.DeployerPartitionHandler.launchWorker(DeployerPartitionHandler.java:347) ~[spring-cloud-task-batch-2.3.1-SNAPSHOT.jar!/:2.3.1-SNAPSHOT]
	at org.springframework.cloud.task.batch.partition.DeployerPartitionHandler.launchWorkers(DeployerPartitionHandler.java:313) ~[spring-cloud-task-batch-2.3.1-SNAPSHOT.jar!/:2.3.1-SNAPSHOT]
	at org.springframework.cloud.task.batch.partition.DeployerPartitionHandler.handle(DeployerPartitionHandler.java:302) ~[spring-cloud-task-batch-2.3.1-SNAPSHOT.jar!/:2.3.1-SNAPSHOT]
	at org.springframework.batch.core.partition.support.PartitionStep.doExecute(PartitionStep.java:106) ~[spring-batch-core-4.3.3.jar!/:4.3.3]
	at org.springframework.batch.core.step.AbstractStep.execute(AbstractStep.java:208) ~[spring-batch-core-4.3.3.jar!/:4.3.3]
	at org.springframework.batch.core.job.SimpleStepHandler.handleStep(SimpleStepHandler.java:152) ~[spring-batch-core-4.3.3.jar!/:4.3.3]
	at org.springframework.batch.core.job.AbstractJob.handleStep(AbstractJob.java:413) ~[spring-batch-core-4.3.3.jar!/:4.3.3]
	at org.springframework.batch.core.job.SimpleJob.doExecute(SimpleJob.java:136) ~[spring-batch-core-4.3.3.jar!/:4.3.3]
	at org.springframework.batch.core.job.AbstractJob.execute(AbstractJob.java:320) ~[spring-batch-core-4.3.3.jar!/:4.3.3]
	at org.springframework.batch.core.launch.support.SimpleJobLauncher$1.run(SimpleJobLauncher.java:149) ~[spring-batch-core-4.3.3.jar!/:4.3.3]
	at org.springframework.core.task.SyncTaskExecutor.execute(SyncTaskExecutor.java:50) ~[spring-core-5.3.7.jar!/:5.3.7]
	at org.springframework.batch.core.launch.support.SimpleJobLauncher.run(SimpleJobLauncher.java:140) ~[spring-batch-core-4.3.3.jar!/:4.3.3]
	at java.base/jdk.internal.reflect.NativeMethodAccessorImpl.invoke0(Native Method) ~[na:na]
	at java.base/jdk.internal.reflect.NativeMethodAccessorImpl.invoke(NativeMethodAccessorImpl.java:62) ~[na:na]
	at java.base/jdk.internal.reflect.DelegatingMethodAccessorImpl.invoke(DelegatingMethodAccessorImpl.java:43) ~[na:na]
	at java.base/java.lang.reflect.Method.invoke(Method.java:566) ~[na:na]
	at org.springframework.aop.support.AopUtils.invokeJoinpointUsingReflection(AopUtils.java:344) ~[spring-aop-5.3.7.jar!/:5.3.7]
	at org.springframework.aop.framework.ReflectiveMethodInvocation.invokeJoinpoint(ReflectiveMethodInvocation.java:198) ~[spring-aop-5.3.7.jar!/:5.3.7]
	at org.springframework.aop.framework.ReflectiveMethodInvocation.proceed(ReflectiveMethodInvocation.java:163) ~[spring-aop-5.3.7.jar!/:5.3.7]
	at org.springframework.batch.core.configuration.annotation.SimpleBatchConfiguration$PassthruAdvice.invoke(SimpleBatchConfiguration.java:128) ~[spring-batch-core-4.3.3.jar!/:4.3.3]
	at org.springframework.aop.framework.ReflectiveMethodInvocation.proceed(ReflectiveMethodInvocation.java:186) ~[spring-aop-5.3.7.jar!/:5.3.7]
	at org.springframework.aop.framework.JdkDynamicAopProxy.invoke(JdkDynamicAopProxy.java:215) ~[spring-aop-5.3.7.jar!/:5.3.7]
	at com.sun.proxy.$Proxy51.run(Unknown Source) ~[na:na]
	at org.springframework.boot.autoconfigure.batch.JobLauncherApplicationRunner.execute(JobLauncherApplicationRunner.java:199) ~[spring-boot-autoconfigure-2.4.6.jar!/:2.4.6]
	at org.springframework.boot.autoconfigure.batch.JobLauncherApplicationRunner.executeLocalJobs(JobLauncherApplicationRunner.java:173) ~[spring-boot-autoconfigure-2.4.6.jar!/:2.4.6]
	at org.springframework.boot.autoconfigure.batch.JobLauncherApplicationRunner.launchJobFromProperties(JobLauncherApplicationRunner.java:160) ~[spring-boot-autoconfigure-2.4.6.jar!/:2.4.6]
	at org.springframework.boot.autoconfigure.batch.JobLauncherApplicationRunner.run(JobLauncherApplicationRunner.java:155) ~[spring-boot-autoconfigure-2.4.6.jar!/:2.4.6]
	at org.springframework.boot.autoconfigure.batch.JobLauncherApplicationRunner.run(JobLauncherApplicationRunner.java:150) ~[spring-boot-autoconfigure-2.4.6.jar!/:2.4.6]
	at org.springframework.boot.SpringApplication.callRunner(SpringApplication.java:799) ~[spring-boot-2.4.6.jar!/:2.4.6]
	at org.springframework.boot.SpringApplication.callRunners(SpringApplication.java:789) ~[spring-boot-2.4.6.jar!/:2.4.6]
	at org.springframework.boot.SpringApplication.run(SpringApplication.java:346) ~[spring-boot-2.4.6.jar!/:2.4.6]
	at org.springframework.boot.SpringApplication.run(SpringApplication.java:1329) ~[spring-boot-2.4.6.jar!/:2.4.6]
	at org.springframework.boot.SpringApplication.run(SpringApplication.java:1318) ~[spring-boot-2.4.6.jar!/:2.4.6]
	at com.example.batchprocessing.BatchProcessingApplication.main(BatchProcessingApplication.java:10) ~[classes!/:0.0.1-SNAPSHOT]
	at java.base/jdk.internal.reflect.NativeMethodAccessorImpl.invoke0(Native Method) ~[na:na]
	at java.base/jdk.internal.reflect.NativeMethodAccessorImpl.invoke(NativeMethodAccessorImpl.java:62) ~[na:na]
	at java.base/jdk.internal.reflect.DelegatingMethodAccessorImpl.invoke(DelegatingMethodAccessorImpl.java:43) ~[na:na]
	at java.base/java.lang.reflect.Method.invoke(Method.java:566) ~[na:na]
	at org.springframework.boot.loader.MainMethodRunner.run(MainMethodRunner.java:49) ~[batchprocessing-0.0.1-SNAPSHOT.jar:0.0.1-SNAPSHOT]
	at org.springframework.boot.loader.Launcher.launch(Launcher.java:108) ~[batchprocessing-0.0.1-SNAPSHOT.jar:0.0.1-SNAPSHOT]
	at org.springframework.boot.loader.Launcher.launch(Launcher.java:58) ~[batchprocessing-0.0.1-SNAPSHOT.jar:0.0.1-SNAPSHOT]
	at org.springframework.boot.loader.JarLauncher.main(JarLauncher.java:88) ~[batchprocessing-0.0.1-SNAPSHOT.jar:0.0.1-SNAPSHOT]

```
Example to setup Spring Batch with Remote Partition Execution through Spring Cloud Deployer Kuberentes 

This certainly is not the most accurate/most efficent approach. But just one approach to the problem, as thought there is no complete doc online, so thought to collect my underatnding and share with others

Derived from Spring IO default local deployer, and update to make to deploy on K8S (also make it compile and work on my local laptop, as original sample seem not work out of box for me)

Reference

- [Spring Cloud Deployer Kuberentes, discover method through test code] https://github.com/spring-cloud/spring-cloud-deployer-kubernetes/
- [Spring Cloud Tutorial on Remote Deployment, though the default code seem not able to compile for me] https://dataflow.spring.io/docs/feature-guides/batch/partitioning/

Assume have local kuberentes and controller will be run on local with worker deploy to kuberentes

Build the image
```aidl 
mvn clean package
docker build . -t worker
```

Setup a MariaDB
```
kubectl apply -f mariadb.deployment.yaml
```

Expose the DB for local controller
```
kubectl port-forward deployment/mariadb 3306:3306
```

On different terminal, start the jar
```
export SPRING_DATASOURCE_PASSWORD="password"
export SPRING_DATASOURCE_USERNAME="spring"
export SPRING_DATASOURCE_URL="jdbc:mysql://localhost:3306/spring"
export SPRING_DATASOURCE_DRIVERCLASSNAME="org.mariadb.jdbc.Driver"
export SPRING_PROFILES_ACTIVE="controller"
export SPRING_BATCH_INITIALIZE_SCHEMA=always

java -jar target/batchprocessing-0.0.1-SNAPSHOT.jar
```

If quick enough, will able to see following, where the pod is spawned up as part of remote partition
```
kubectl get pods
NAME                                 READY   STATUS      RESTARTS   AGE
mariadb-6b48f78bbf-bmxsm             1/1     Running     0          4h7m
partitionedbatchjobtask-6zjr0mg7ny   1/1     Running     0          2s         11m
partitionedbatchjobtask-kdgp9w0exk   1/1     Running     0          2s
```

Each pod will be carry its own partition context (partition0, partition1)
Can be seen inside the pod definition on args field
```
    Args:
      --spring.profiles.active=worker
      --spring.cloud.task.initialize.enable=false
      --spring.batch.initializer.enabled=false
      --spring.cloud.task.job-execution-id=34
      --spring.cloud.task.step-execution-id=104
      --spring.cloud.task.step-name=workerStep1
      --spring.cloud.task.name=application_partitionedJob1532999942_workerStep1:partition1
      --spring.cloud.task.parentExecutionId=69
      --spring.cloud.task.executionid=71

```

The pod current clean up is done through brute force, as this one example is not using full spring cloud method

If using job [current code implementation]
```
        kubernetesDeployerProperties.setCreateJob(true);
```
In after job, force clean up success/failed job , if need, can comment out the deletion line to let pod remain

Job and its associated pod will be rmovoed through the `DeletionPropagation` policy

Job is removed if there are success or fail attempt at least once. Combine with restart policy of 1 in 
```

            // Clean up jobs
            Map<String, String> labels = new HashMap<String, String>();
            labels.put("task-name",taskName);

            List<io.fabric8.kubernetes.api.model.batch.Job> joblist = kuberentesClient().batch().jobs().inNamespace("default").withLabels(labels).list().getItems();

            for(int index = 0; index < joblist.size(); index++)
            {
                io.fabric8.kubernetes.api.model.batch.Job job = joblist.get(index);
                JobStatus jobStatus = job.getStatus();

                System.out.println(jobStatus.getConditions().get(0).getType()  + " CHECK JOB STATUS " + job.getMetadata().getName());
                // Clean up job that is in Complete (Success)/Failed state
                if(jobStatus.getConditions().get(0).getType().contains("Complete") ||
                        jobStatus.getConditions().get(0).getType().contains("Failed"))
                {
                    kuberentesClient().batch().jobs().inNamespace("default")
                            .withName(job.getMetadata().getName())
                            .withPropagationPolicy(DeletionPropagation.BACKGROUND)
                            .delete();
                }

            }
```

If using Pod (default) without set job to true

In after job, force clean up success/failed pod , if need, can comment out the deletion line to let pod remain

Then can use

```
    @Bean
    public JobExecutionListener jobExecutionListener() {
    JobExecutionListener listener = new JobExecutionListener(){
        @Override
        public void beforeJob(JobExecution JobExecution)
        {
            // Auto generated method
        }

        @Override
        public void afterJob(JobExecution jobExecution) {

            // Clean up jobs
            Map<String, String> labels = new HashMap<String, String>();
            labels.put("task-name",taskName);


            List<io.fabric8.kubernetes.api.model.batch.Job> joblist = kuberentesClient().batch().jobs().inNamespace("default").withLabels(labels).list().getItems();

            for(int index = 0; index < joblist.size(); index++)
            {
                io.fabric8.kubernetes.api.model.batch.Job job = joblist.get(index);
                JobStatus jobStatus = job.getStatus();

                System.out.println(jobStatus.getConditions().get(0).getType()  + " CHECK JOB STATUS " + job.getMetadata().getName());
                // Clean up job that is in Complete (Success)/Failed state
                if(jobStatus.getConditions().get(0).getType().contains("Complete") ||
                        jobStatus.getConditions().get(0).getType().contains("Failed"))
                {
                    kuberentesClient().batch().jobs().inNamespace("default")
                            .withName(job.getMetadata().getName())
                            .withPropagationPolicy(DeletionPropagation.BACKGROUND)
                            .delete();
                }

            }
        }
    };
```

Also demo how to use DockerResource and TaskLauncher to inject custom remote docker image with pod computing

Custom K8S pod setting for CPU/MEMORY can be found in
```
    @Bean
    public TaskLauncher taskLauncher()
...

Custom Docker Image
```aidl
    @Bean("partitionHandler")
    public PartitionHandler partitionHandler(TaskLauncher taskLauncher,
                                             JobExplorer jobExplorer) throws Exception {

        // Use local build image
        DockerResource resource = new DockerResource("worker:latest");
...

```
