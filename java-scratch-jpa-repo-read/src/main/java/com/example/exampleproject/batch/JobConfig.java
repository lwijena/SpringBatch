package com.example.exampleproject.batch;

import java.util.List;

import javax.sql.DataSource;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.support.DefaultBatchConfiguration;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.core.launch.support.TaskExecutorJobLauncher;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.batch.BatchDataSourceScriptDatabaseInitializer;
import org.springframework.boot.autoconfigure.batch.BatchProperties;
import org.springframework.boot.sql.init.DatabaseInitializationMode;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Lazy;
import org.springframework.context.annotation.PropertySource;
import org.springframework.transaction.PlatformTransactionManager;

import com.example.exampleproject.DbReader;
import com.example.exampleproject.DbWriter;
import com.example.exampleproject.entity.Student;
import com.example.exampleproject.repository.StudentRepository;

@Configuration
@EnableBatchProcessing
public class JobConfig {
    @Autowired
    @Lazy
    private StudentRepository repository;

   
    @Autowired
    private DbReader dbReader;

    @Autowired
    private DbWriter dbWriter;
    
    @Bean
    public JobLauncher jobLauncher(JobRepository jobRepository) throws Exception {
        TaskExecutorJobLauncher jobLauncher = new TaskExecutorJobLauncher();
        jobLauncher.setJobRepository(jobRepository);
        jobLauncher.afterPropertiesSet();
        return jobLauncher;
    }


@Bean
public Job job(JobRepository jobRepository, PlatformTransactionManager transactionManager) {
	return new JobBuilder("dailyJob", jobRepository).incrementer(new RunIdIncrementer())
			.start(step1(jobRepository, transactionManager)).next(step2(jobRepository, transactionManager))
			.build();
}


@Bean
public Step step1(JobRepository jobRepository, PlatformTransactionManager transactionManager) {
    return new StepBuilder("fileToSftpStep", jobRepository).tasklet(dataPrepareTasklet(), transactionManager).build();
}



	@Bean
	public Step step2(JobRepository jobRepository, PlatformTransactionManager transactionManager) {
		return new StepBuilder("databaseToFileStep", jobRepository)
				.<List<Student>, List<Student>>chunk(5, transactionManager).reader(dbReader).writer(dbWriter).build();

	}

    @Bean
    public Tasklet dataPrepareTasklet(){
        return (contribution, chunkContext) -> {
            for(int i = 0; i<43; i++){
                Student student = repository.save(new Student(i, "Student"+i, false));
            }
            for(int i = 43; i<100; i++){
                Student student = repository.save(new Student(i, "Student"+i, true));
            }
            return RepeatStatus.FINISHED;
        };
    }
    
    
    /**
     * Due to usage of {@link DefaultBatchConfiguration}, db initializer need to defined in order for Spring Batch
     * to consider initializing the schema on the first usage. In case of
     * {@link org.springframework.batch.core.configuration.annotation.EnableBatchProcessing} usage, it would have
     * been resolved with 'spring.batch.initialize-schema' property
     */
    @Bean
    public BatchDataSourceScriptDatabaseInitializer batchDataSourceInitializer(DataSource dataSource,
                                                                               BatchProperties properties) {
        return new BatchDataSourceScriptDatabaseInitializer(dataSource, properties.getJdbc());
    }
    
   

    /**
     * Due to usage of {@link DefaultBatchConfiguration}, we need to explicitly (programmatically) set initializeSchema
     * mode, and we are taking this parameter from the configuration wile, defined at {@link PropertySource} on class level;
     * In case we'd use {@link org.springframework.batch.core.configuration.annotation.EnableBatchProcessing}, having
     * 'spring.batch.initialize-schema' property would be enough
     */
    @Bean
    public BatchProperties batchProperties(@Value("${spring.batch.jdbc.initialize-schema}") DatabaseInitializationMode initializationMode) {
        BatchProperties properties = new BatchProperties();
        properties.getJdbc().setInitializeSchema(initializationMode);
        return properties;
    }
    

}
