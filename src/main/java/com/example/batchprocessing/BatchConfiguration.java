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
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.cloud.deployer.resource.docker.DockerResource;
import org.springframework.cloud.deployer.resource.support.DelegatingResourceLoader;
import org.springframework.cloud.deployer.spi.kubernetes.*;
import org.springframework.cloud.deployer.spi.task.TaskLauncher;
import org.springframework.cloud.task.batch.partition.*;
import org.springframework.cloud.task.configuration.EnableTask;
import org.springframework.cloud.task.repository.TaskExecution;
import org.springframework.cloud.task.repository.TaskExplorer;
import org.springframework.cloud.task.repository.TaskRepository;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;
import org.springframework.core.env.Environment;
import org.springframework.core.env.SystemEnvironmentPropertySource;
import org.springframework.core.io.Resource;
import org.springframework.core.task.TaskExecutor;
import org.springframework.core.task.TaskRejectedException;
import org.springframework.util.StringUtils;

import java.util.*;


@Configuration
@EnableBatchProcessing
@EnableTask
public class BatchConfiguration {

    private static int BACK_OFF_LIMIT = 6;

    // Set the kuberentes job name
    private String taskName_prefix="partitionedbatchjob";

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
    public TaskExplorer taskExplorer;

    @Autowired
    private ConfigurableApplicationContext context;

    @Autowired
    private DelegatingResourceLoader resourceLoader;

    @Autowired
    private Environment environment;

    @Bean
    @StepScope
    public Partitioner partitioner( @Value("#{stepExecution}") StepExecution stepExecution) {
        return new Partitioner() {
            @Override
            public Map<String, ExecutionContext> partition(int gridSize) {

                Map<String, ExecutionContext> partitions = new HashMap<>(gridSize);

                int targetGridSize = 0;
                String step = "";
                if(stepExecution.getStepName().equalsIgnoreCase("partitionReaderStep"))
                {
                    step = "reader";
                }
                else
                {
                    step = "processor";
                }

                targetGridSize = Integer.parseInt(stepExecution.getJobExecution().getExecutionContext().getString(step + "WorkerGridSize"));

                for (int i = 0; i < targetGridSize; i++) {
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
    @StepScope
    public TaskLauncher taskLauncher( @Value("#{stepExecution}") StepExecution stepExecution)
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
        KubernetesDeployerProperties.LimitsResources limit = new KubernetesDeployerProperties.LimitsResources();

        String step = "";

        if(stepExecution.getStepName().equalsIgnoreCase("partitionReaderStep"))
        {
            step="reader";
        }
        else
        {
            step="processor";
        }

        request.setCpu(stepExecution.getJobExecution().getExecutionContext().getString(step + "CPURequest"));
        request.setMemory("2000Mi");


        limit.setCpu(stepExecution.getJobExecution().getExecutionContext().getString(step +"CPULimit"));
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
    public Job partitionedJob()throws Exception {
        Random random = new Random();
        return jobBuilderFactory.get("partitionedJob" + random.nextInt())
                .start(partitionReaderStep())
                .listener(jobExecutionListener())
                .next(partitionProcessorStep())
                .build();
    }

    @Bean(name = "partitionReaderStep")
    public Step partitionReaderStep() throws Exception {

        return stepBuilderFactory.get("partitionReaderStep")
                .partitioner(workerStepReader().getName(),  partitioner( null))
                .step(workerStepReader())
                .partitionHandler(partitionHandler(
                        taskLauncher( null),
                        jobExplorer, null))
                .build();
    }

    @Bean(name = "partitionProcessorStep")
    public Step partitionProcessorStep() throws Exception {

        return stepBuilderFactory.get("partitionProcessorStep")
                .partitioner(workerStepProcessor().getName(), partitioner( null))
                .step(workerStepProcessor())
                .partitionHandler(partitionHandler(
                        taskLauncher( null),
                        jobExplorer, null))
                .build();
    }


    @Bean
    @StepScope
    public PartitionHandler partitionHandler(TaskLauncher taskLauncher,
                                                   JobExplorer jobExplorer,
                                             @Value("#{stepExecution}") StepExecution stepExecution) throws Exception {

        String step ="processor";

        if(stepExecution.getStepName().equalsIgnoreCase("partitionReaderStep")) {
            step = "reader";
        }

        // Use local build image
        DockerResource resource = new DockerResource(stepExecution.getJobExecution().getExecutionContext().getString(step + "WorkerImage"));


        DeployerPartitionHandler partitionHandler =
                new DeployerPartitionHandler(taskLauncher, jobExplorer, resource,
                        stepExecution.getJobExecution().getExecutionContext().getString(step + "WorkerStep")
                        , taskRepository);

        // Issue https://github.com/spring-cloud/spring-cloud-task/issues/793
        // Perform the setting of execution as this partitioner now not created at task level so @beforetask is no longer vaild
        // The problem is DeployerPartitionHandler utilize annoation @BeforeTask to force task to pass in TaskExecution object as part of Task setup
        // But as this partionerHandler is now at @StepScope (instead of directly at @Bean level with @Enable Task), that setup is no longer triggered
        // Resulted created DeployerHandler faced a null

        // Below is essentially a workaround to use the current job execution id to retrieve the associated task execution id
        // From there, got that task execution and passed to deploy handler to fulfill its need of taskExecution reference
        // It seem to work, but still not clear if there is other side effect (so far during test not found any)
        long executionId = taskExplorer.getTaskExecutionIdByJobExecutionId(stepExecution.getJobExecutionId());

        System.out.println("Current execution job to task execution id " + executionId);
        TaskExecution taskExecution = taskExplorer.getTaskExecution(taskExplorer.getTaskExecutionIdByJobExecutionId(stepExecution.getJobExecutionId()));
        System.out.println("Current execution job to task execution is not null: " + (taskExecution != null));
        partitionHandler.beforeTask(taskExecution);

        List<String> commandLineArgs = new ArrayList<>(3);
        commandLineArgs.add("--spring.profiles.active=worker");
        commandLineArgs.add("--spring.cloud.task.initialize.enable=false");
        commandLineArgs.add("--spring.batch.initializer.enabled=false");
        partitionHandler
                .setCommandLineArgsProvider(new PassThroughCommandLineArgsProvider(commandLineArgs));
        partitionHandler.setEnvironmentVariablesProvider(new NoOpEnvironmentVariablesProvider());
        partitionHandler.setMaxWorkers(Integer.parseInt(stepExecution.getJobExecution().getExecutionContext().getString(step + "WorkerGridSize")));

            partitionHandler.setApplicationName(taskName_prefix + step);

        return partitionHandler;
    }

    @Bean
    public JobExecutionListener jobExecutionListener() {
    JobExecutionListener listener = new JobExecutionListener(){
        @Override
        public void beforeJob(JobExecution jobExecution)
        {
            jobExecution.getExecutionContext().putString("readerCPURequest", "1");
            jobExecution.getExecutionContext().putString("readerCPULimit", "2");

            jobExecution.getExecutionContext().putString("readerWorkerGridSize", "1");

            // For now using same image for reader/processor, but if it work, can split them
            jobExecution.getExecutionContext().putString("readerWorkerImage", "worker:latest");
            jobExecution.getExecutionContext().putString("readerWorkerStep", "workerStepReader");

            jobExecution.getExecutionContext().putString("processorCPURequest", "3");
            jobExecution.getExecutionContext().putString("processorCPULimit", "4");

            jobExecution.getExecutionContext().putString("processorWorkerGridSize", "2");

            // For now using same image for reader/processor, but if it work, will split them
            jobExecution.getExecutionContext().putString("processorWorkerImage", "worker:latest");
            jobExecution.getExecutionContext().putString("processorWorkerStep", "workerStepProcessor");

            System.out.println("Set readerGridSize == " + jobExecution.getExecutionContext().getString("readerGridSize", "IT IS NULL WHICH IS INCORRECT"));

        }

        @Override
        public void afterJob(JobExecution jobExecution) {
        }
    };

    return listener;
    }

    @Bean
    @Profile("worker")
    public DeployerStepExecutionHandler stepExecutionHandler(JobExplorer jobExplorer) {
        return new DeployerStepExecutionHandler(this.context, jobExplorer, this.jobRepository);
    }

    @Bean(name = "workerStepReader")
    public Step workerStepReader() {
        return this.stepBuilderFactory.get("workerStepReader")
                .tasklet(workerTaskletReader(null))
                .build();
    }

    @Bean(name = "workerStepProcessor")
    public Step workerStepProcessor() {
        return this.stepBuilderFactory.get("workerStepProcessor")
                .tasklet(workerTaskletProcessor(null))
                .build();
    }



    @Bean
    @StepScope
    public Tasklet workerTaskletReader(
            final @Value("#{stepExecution}") StepExecution stepExecution) {

        return new Tasklet() {
            @Override
            public RepeatStatus execute(StepContribution contribution, ChunkContext chunkContext) throws Exception {
                Integer partitionNumber = stepExecution.getExecutionContext().getInt("partitionNumber");
                System.out.println("This workerTaskletReader ran partition: " + partitionNumber);

                return RepeatStatus.FINISHED;
            }
        };
    }

    @Bean
    @StepScope
    public Tasklet workerTaskletProcessor(
            final @Value("#{stepExecution}") StepExecution stepExecution) {

        return new Tasklet() {
            @Override
            public RepeatStatus execute(StepContribution contribution, ChunkContext chunkContext) throws Exception {
                Integer partitionNumber = stepExecution.getExecutionContext().getInt("partitionNumber");
                System.out.println("This workerTaskletProcessor ran partition: " + partitionNumber);

                return RepeatStatus.FINISHED;
            }
        };
    }
}
