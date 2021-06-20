# spring-batch-remote-k8s-paritition-example
Example to setup Spring Batch with Remote Partition Execution through Spring Cloud Deployer Kuberentes 
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
```aidl
kubectl get pods
NAME                                 READY   STATUS      RESTARTS   AGE
mariadb-6b48f78bbf-bmxsm             1/1     Running     0          4h7m
partitionedbatchjobtask-6zjr0mg7ny   1/1     Running     0          2s         11m
partitionedbatchjobtask-kdgp9w0exk   1/1     Running     0          2s
```

Each pod will be carry its own partition context (partition0, partition1)
Can be seen inside the pod definition on args field
```aidl
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
In after job, force clean up success/failed pod, if need, can comment out the deletion line to let pod remain
```aidl
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
            Map<String, String> fields = new HashMap<String, String>();
            fields.put("status.phase","Succeeded");

            Map<String, String> labels = new HashMap<String, String>();
            labels.put("role","spring-app");

            kuberentesClient().pods().inNamespace("default").withLabels(labels).withFields(fields).delete();

            fields.put("status.phase","Failed");
            kuberentesClient().pods().inNamespace("default").withLabels(labels).withFields(fields).delete();

        }
    };
```

Also demo how to use DockerResource and TaskLauncher to inject custom remote docker image with pod computing

Custom K8S pod setting for CPU/MEMORY can be found in
```aidl
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