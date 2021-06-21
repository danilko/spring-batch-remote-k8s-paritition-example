package com.example.batchprocessing;

import io.fabric8.kubernetes.api.model.DeletionPropagation;
import io.fabric8.kubernetes.api.model.batch.JobList;
import io.fabric8.kubernetes.api.model.batch.JobSpec;
import io.fabric8.kubernetes.api.model.batch.JobStatus;
import io.fabric8.kubernetes.client.KubernetesClient;
import org.springframework.batch.core.*;
import org.springframework.batch.core.configuration.JobRegistry;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.core.explore.JobExplorer;
import org.springframework.batch.core.partition.PartitionHandler;
import org.springframework.batch.core.partition.support.Partitioner;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.cloud.deployer.resource.docker.DockerResource;
import org.springframework.cloud.deployer.resource.support.DelegatingResourceLoader;
import org.springframework.cloud.deployer.spi.kubernetes.*;
import org.springframework.cloud.deployer.spi.task.TaskLauncher;
import org.springframework.cloud.task.batch.partition.*;
import org.springframework.cloud.task.configuration.EnableTask;
import org.springframework.cloud.task.repository.TaskRepository;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;
import org.springframework.core.env.Environment;
import org.springframework.core.io.Resource;
import org.springframework.core.task.TaskExecutor;
import org.springframework.core.task.TaskRejectedException;
import org.springframework.util.StringUtils;

import java.util.*;


@Configuration
@EnableBatchProcessing
@EnableTask
public class BatchConfiguration {

    // Set the grid (also controller worker)
    private static final int  GRID_SIZE = 2;

    private static int BACK_OFF_LIMIT = 6;

    // Set the kuberentes job name
    private String taskName="partitionedbatchjobtask";

    @Autowired
    public JobBuilderFactory jobBuilderFactory;

    @Autowired
    public StepBuilderFactory stepBuilderFactory;

    @Autowired
    public JobExplorer jobExplorer;

    @Autowired
    public JobRepository jobRepository;

    @Autowired
    public TaskExecutor taskExecutor;

    @Autowired
    public TaskRepository taskRepository;

    @Autowired
    private ConfigurableApplicationContext context;

    @Autowired
    private DelegatingResourceLoader resourceLoader;

    @Autowired
    private Environment environment;

    @Bean
    public Partitioner partitioner() {
        return new Partitioner() {
            @Override
            public Map<String, ExecutionContext> partition(int gridSize) {

                Map<String, ExecutionContext> partitions = new HashMap<>(gridSize);

                for (int i = 0; i < GRID_SIZE; i++) {
                    ExecutionContext context1 = new ExecutionContext();
                    context1.put("partitionNumber", i);

                    partitions.put("partition" + i, context1);
                }

                return partitions;
            }
        };
    }

    @Bean
    public KubernetesClient kuberentesClient()
    {
        KubernetesDeployerProperties kubernetesDeployerProperties = new KubernetesDeployerProperties();

        return KubernetesClientFactory.getKubernetesClient(kubernetesDeployerProperties);
    }

    @Bean
    public TaskLauncher taskLauncher()
    {
        KubernetesDeployerProperties kubernetesDeployerProperties = new KubernetesDeployerProperties();
        kubernetesDeployerProperties.setNamespace("default");

        kubernetesDeployerProperties.setCreateJob(true);

        // Database setup to reference configmap for database info
        List<KubernetesDeployerProperties.ConfigMapKeyRef> configMapKeyRefList = new ArrayList<KubernetesDeployerProperties.ConfigMapKeyRef>();
        KubernetesDeployerProperties.ConfigMapKeyRef configMapKeyRef = new KubernetesDeployerProperties.ConfigMapKeyRef();
        configMapKeyRef.setConfigMapName("mariadb");
        configMapKeyRef.setDataKey("SPRING_DATASOURCE_URL");
        configMapKeyRef.setEnvVarName("SPRING_DATASOURCE_URL");
        configMapKeyRefList.add(configMapKeyRef);

        configMapKeyRef = new KubernetesDeployerProperties.ConfigMapKeyRef();
        configMapKeyRef.setConfigMapName("mariadb");
        configMapKeyRef.setDataKey("SPRING_DATASOURCE_USERNAME");
        configMapKeyRef.setEnvVarName("SPRING_DATASOURCE_USERNAME");
        configMapKeyRefList.add(configMapKeyRef);

        configMapKeyRef = new KubernetesDeployerProperties.ConfigMapKeyRef();
        configMapKeyRef.setConfigMapName("mariadb");
        configMapKeyRef.setDataKey("SPRING_DATASOURCE_PASSWORD");
        configMapKeyRef.setEnvVarName("SPRING_DATASOURCE_PASSWORD");
        configMapKeyRefList.add(configMapKeyRef);

        configMapKeyRef = new KubernetesDeployerProperties.ConfigMapKeyRef();
        configMapKeyRef.setConfigMapName("mariadb");
        configMapKeyRef.setDataKey("SPRING_DATASOURCE_DRIVERCLASSNAME");
        configMapKeyRef.setEnvVarName("SPRING_DATASOURCE_DRIVERCLASSNAME");
        configMapKeyRefList.add(configMapKeyRef);

        configMapKeyRef = new KubernetesDeployerProperties.ConfigMapKeyRef();
        configMapKeyRef.setConfigMapName("mariadb");
        configMapKeyRef.setDataKey("SPRING_PROFILES_ACTIVE");
        configMapKeyRef.setEnvVarName("SPRING_PROFILES_ACTIVE");
        configMapKeyRefList.add(configMapKeyRef);

        kubernetesDeployerProperties.setConfigMapKeyRefs(configMapKeyRefList);

        // Set request resource
        KubernetesDeployerProperties.RequestsResources request = new KubernetesDeployerProperties.RequestsResources();
        request.setCpu("2");
        request.setMemory("2000Mi");

        KubernetesDeployerProperties.LimitsResources limit = new KubernetesDeployerProperties.LimitsResources();
        limit.setCpu("3");
        limit.setMemory("3000Mi");

        kubernetesDeployerProperties.setRequests(request);
        kubernetesDeployerProperties.setLimits(limit);

        // as build on local image, so need to use local
        kubernetesDeployerProperties.setImagePullPolicy(ImagePullPolicy.IfNotPresent);

        // Set task launcher properties to not repeat and not restart
        KubernetesTaskLauncherProperties kubernetesTaskLauncherProperties = new KubernetesTaskLauncherProperties();

        // https://kubernetes.io/docs/concepts/workloads/controllers/job/
        // Set to never to create new pod on restart
        kubernetesTaskLauncherProperties.setBackoffLimit(BACK_OFF_LIMIT);
        kubernetesTaskLauncherProperties.setRestartPolicy(RestartPolicy.Never);
        KubernetesTaskLauncher kubernetesTaskLauncher = new KubernetesTaskLauncher(kubernetesDeployerProperties,
                kubernetesTaskLauncherProperties, kuberentesClient());



        return kubernetesTaskLauncher;
    }

    @Bean(name = "partitionedJob")
    @Profile("!worker")
    public Job partitionedJob(PartitionHandler partitionHandler)throws Exception {
        Random random = new Random();
        return jobBuilderFactory.get("partitionedJob" + random.nextInt())
                .start(partitionStep1(partitionHandler))
                .listener(jobExecutionListener())
                .build();
    }

    @Bean(name = "partitionStep1")
    public Step partitionStep1(PartitionHandler partitionHandler) throws Exception {
        return stepBuilderFactory.get("partitionStep1")
                .partitioner(workerStep1().getName(), partitioner())
                .step(workerStep1())
                .partitionHandler(partitionHandler)
                .build();
    }

    @Bean("partitionHandler")
    public PartitionHandler partitionHandler(TaskLauncher taskLauncher,
                                             JobExplorer jobExplorer) throws Exception {

        // Use local build image
        DockerResource resource = new DockerResource("worker:latest");

        DeployerPartitionHandler partitionHandler =
                new DeployerPartitionHandler(taskLauncher, jobExplorer, resource, "workerStep1", taskRepository);

        List<String> commandLineArgs = new ArrayList<>(3);
        commandLineArgs.add("--spring.profiles.active=worker");
        commandLineArgs.add("--spring.cloud.task.initialize.enable=false");
        commandLineArgs.add("--spring.batch.initializer.enabled=false");
        partitionHandler
                .setCommandLineArgsProvider(new PassThroughCommandLineArgsProvider(commandLineArgs));
        partitionHandler.setEnvironmentVariablesProvider(new NoOpEnvironmentVariablesProvider());
        partitionHandler.setMaxWorkers(GRID_SIZE);

        partitionHandler.setApplicationName(taskName);

        return partitionHandler;
    }

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

    return listener;
    }

    @Bean
    @Profile("worker")
    public DeployerStepExecutionHandler stepExecutionHandler(JobExplorer jobExplorer) {
        return new DeployerStepExecutionHandler(this.context, jobExplorer, this.jobRepository);
    }

    @Bean(name = "workerStep1")
    public Step workerStep1() {
        return this.stepBuilderFactory.get("workerStep1")
                .tasklet(workerTasklet(null))
                .build();
    }


    @Bean
    @StepScope
    public Tasklet workerTasklet(
            final @Value("#{stepExecutionContext['partitionNumber']}") Integer partitionNumber) {

        return new Tasklet() {
            @Override
            public RepeatStatus execute(StepContribution contribution, ChunkContext chunkContext) throws Exception {
                System.out.println("This tasklet ran partition: " + partitionNumber);

                return RepeatStatus.FINISHED;
            }
        };
    }
}
