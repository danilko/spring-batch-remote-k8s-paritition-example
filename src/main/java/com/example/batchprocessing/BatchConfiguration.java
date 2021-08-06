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

    // Set the grid (also controller worker)
    private static final int  GRID_SIZE = 2;

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
    private ConfigurableApplicationContext context;

    @Autowired
    private DelegatingResourceLoader resourceLoader;

    @Autowired
    private Environment environment;

    @Bean
    @StepScope
    public Partitioner readerPartitioner() {
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
    @StepScope
    public Partitioner processorPartitioner() {
        return new Partitioner() {
            @Override
            public Map<String, ExecutionContext> partition(int gridSize) {

                Map<String, ExecutionContext> partitions = new HashMap<>(gridSize);

                for (int i = 0; i < 1; i++) {
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
    public TaskLauncher processorTaskLauncher()
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
        request.setCpu("3");
        request.setMemory("2000Mi");

        KubernetesDeployerProperties.LimitsResources limit = new KubernetesDeployerProperties.LimitsResources();
        limit.setCpu("4");
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

    @Bean
    @StepScope
    public TaskLauncher readerTaskLauncher()
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
        request.setCpu("1");
        request.setMemory("2000Mi");

        KubernetesDeployerProperties.LimitsResources limit = new KubernetesDeployerProperties.LimitsResources();
        limit.setCpu("2");
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
    public Job partitionedJob(@Qualifier("partitionHandlerReader") PartitionHandler partitionHandlerReader,
                              @Qualifier("partitionHandlerReader") PartitionHandler partitionHandlerProcessor)throws Exception {
        Random random = new Random();
        return jobBuilderFactory.get("partitionedJob" + random.nextInt())
                .start(partitionReaderStep(partitionHandlerReader))
               // .listener(jobExecutionListener())
                .next(partitionProcessorStep(partitionHandlerProcessor))
                .build();
    }

    @Bean(name = "partitionReaderStep")
    public Step partitionReaderStep(PartitionHandler partitionHandlerReader) throws Exception {
        Partitioner partitioner = readerPartitioner();

        return stepBuilderFactory.get("partitionReaderStep")
                .partitioner(workerStepReader().getName(), partitioner)
                .step(workerStepReader())
                .partitionHandler(partitionHandlerReader)
                .build();
    }

    @Bean(name = "partitionProcessorStep")
    public Step partitionProcessorStep(PartitionHandler partitionHandlerProcessor) throws Exception {
        Partitioner partitioner = processorPartitioner();

        return stepBuilderFactory.get("partitionProcessorStep")
                .partitioner(workerStepProcessor().getName(), partitioner)
                .step(workerStepProcessor())
                .partitionHandler(partitionHandlerProcessor)
                .build();
    }

    @Bean
    public PartitionHandler partitionHandlerReader(TaskLauncher readerTaskLauncher,
                                             JobExplorer jobExplorer) throws Exception {

        // Use local build image
        DockerResource resource = new DockerResource("worker:latest");

        DeployerPartitionHandler partitionHandler =
                new DeployerPartitionHandler(readerTaskLauncher, jobExplorer, resource, "workerStepReader", taskRepository);

        List<String> commandLineArgs = new ArrayList<>(3);
        commandLineArgs.add("--spring.profiles.active=worker");
        commandLineArgs.add("--spring.cloud.task.initialize.enable=false");
        commandLineArgs.add("--spring.batch.initializer.enabled=false");
        partitionHandler
                .setCommandLineArgsProvider(new PassThroughCommandLineArgsProvider(commandLineArgs));
        partitionHandler.setEnvironmentVariablesProvider(new NoOpEnvironmentVariablesProvider());
        partitionHandler.setMaxWorkers(1);

        partitionHandler.setApplicationName(taskName_prefix + "reader");

        return partitionHandler;
    }

    @Bean
    public PartitionHandler partitionHandlerProcessor(TaskLauncher processorTaskLauncher,
                                                   JobExplorer jobExplorer) throws Exception {

        // Use local build image
        DockerResource resource = new DockerResource("worker:latest");

        DeployerPartitionHandler partitionHandler =
                new DeployerPartitionHandler(processorTaskLauncher, jobExplorer, resource, "workerStepProcessor", taskRepository);

        List<String> commandLineArgs = new ArrayList<>(3);
        commandLineArgs.add("--spring.profiles.active=worker");
        commandLineArgs.add("--spring.cloud.task.initialize.enable=false");
        commandLineArgs.add("--spring.batch.initializer.enabled=false");
        partitionHandler
                .setCommandLineArgsProvider(new PassThroughCommandLineArgsProvider(commandLineArgs));
        partitionHandler.setEnvironmentVariablesProvider(new NoOpEnvironmentVariablesProvider());
        partitionHandler.setMaxWorkers(GRID_SIZE);

        partitionHandler.setApplicationName(taskName_prefix + "processor");

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
            labels.put("task-name",taskName_prefix);


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
            final @Value("#{stepExecutionContext['partitionNumber']}") Integer partitionNumber) {

        return new Tasklet() {
            @Override
            public RepeatStatus execute(StepContribution contribution, ChunkContext chunkContext) throws Exception {
                System.out.println("This workerTaskletReader ran partition: " + partitionNumber);

                return RepeatStatus.FINISHED;
            }
        };
    }

    @Bean
    @StepScope
    public Tasklet workerTaskletProcessor(
            final @Value("#{stepExecutionContext['partitionNumber']}") Integer partitionNumber) {

        return new Tasklet() {
            @Override
            public RepeatStatus execute(StepContribution contribution, ChunkContext chunkContext) throws Exception {
                System.out.println("This workerTaskletProcessor ran partition: " + partitionNumber);

                return RepeatStatus.FINISHED;
            }
        };
    }
}
