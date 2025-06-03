package com.example.batch.config;

import com.example.batch.entity.Customer;
import com.example.batch.repo.CustomerRepository;
import lombok.AllArgsConstructor;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.item.data.RepositoryItemReader;
import org.springframework.batch.item.data.RepositoryItemWriter;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.LineMapper;
import org.springframework.batch.item.file.mapping.BeanWrapperFieldSetMapper;
import org.springframework.batch.item.file.mapping.DefaultLineMapper;
import org.springframework.batch.item.file.transform.BeanWrapperFieldExtractor;
import org.springframework.batch.item.file.transform.DelimitedLineTokenizer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.FileSystemResource;
import org.springframework.core.task.SimpleAsyncTaskExecutor;
import org.springframework.core.task.TaskExecutor;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.PlatformTransactionManager;

@Configuration
@EnableBatchProcessing
@AllArgsConstructor
public class SpringBatchConfiguration {

    @Autowired
    private CustomerRepository customerRepository;

    @Bean
    public FlatFileItemReader<Customer> reader1(){
        FlatFileItemReader<Customer> itemReader = new FlatFileItemReader();
        itemReader.setResource(new FileSystemResource("src/main/resources/customers.csv"));
        itemReader.setName("CSV Reader 1");
        itemReader.setLinesToSkip(1);
        itemReader.setLineMapper(linemapper());
        return itemReader;
    }

/*    @Bean
    public FlatFileItemReader<Customer> reader2(){
        FlatFileItemReader<Customer> itemReader = new FlatFileItemReader();
        itemReader.setResource(new FileSystemResource("src/main/resources/customers_modified.csv"));
        itemReader.setName("CSV Reader 2");
        itemReader.setLinesToSkip(1);
        itemReader.setLineMapper(linemapper());
        return itemReader;
    }*/

    private LineMapper<Customer> linemapper() {
        DefaultLineMapper<Customer> lineMapper=new DefaultLineMapper<>();//composite pattern- composite+templace design pattern

        DelimitedLineTokenizer lineTokenizer = new DelimitedLineTokenizer();   //strategy design- allows us to change the behaviour at runtime
        lineTokenizer.setDelimiter(",");
        lineTokenizer.setStrict(false);   ///missing dields will be set to null, not need every line has all values
        lineTokenizer.setNames("id", "firstName", "lastName", "email", "gender", "contactNo", "country", "dob");

        BeanWrapperFieldSetMapper<Customer> fieldSetMapper = new BeanWrapperFieldSetMapper<>();   //strategy design- allows us to change the behaviour at runtime
        fieldSetMapper.setTargetType(Customer.class);

        lineMapper.setLineTokenizer(lineTokenizer);
        lineMapper.setFieldSetMapper(fieldSetMapper);

        return lineMapper;
    }

    @Bean
    public CustomerProcessor processor(){
        return new CustomerProcessor();
    }
    @Bean
    public RepositoryItemWriter<Customer> writer(){//there are many other writers in itemwroter inside this classs inside itemwriter like FLatFileItemWriter,Resids,JDBC
        RepositoryItemWriter<Customer> writer = new RepositoryItemWriter<>();
        writer.setRepository(customerRepository);
        writer.setMethodName("save");
        return writer;
    }

    @Bean
    public Step step1(JobRepository jobRepository, PlatformTransactionManager transactionManager){
        return new StepBuilder("csv-step-1", jobRepository)
                .<Customer, Customer>chunk(10, transactionManager)
                .reader(reader1())
                .processor(processor())
                .writer(writer())
                //.taskExecutor(taskExecutor())//task executer
                .build();
    }

/*    @Bean
    public Step step2(JobRepository jobRepository, PlatformTransactionManager transactionManager){
        return new StepBuilder("csv-step-2", jobRepository)
                .<Customer, Customer>chunk(20, transactionManager)
                .reader(reader2())
                .processor(processor())
                .writer(writer())
                //.taskExecutor(taskExecutor())//task executer
                .build();
    }*/
    @Bean
    public Job runJob(JobRepository jobRepository, Step step1){  //(JobRepository jobRepository, Step step1,Step step2)
        return new JobBuilder("importCustomers", jobRepository)
                .start(step1)
                //.next(step2)   //add multiple steps like this
                .build();
    }

    @Bean
    public TaskExecutor taskExecutor(){
        SimpleAsyncTaskExecutor taskExecutor = new SimpleAsyncTaskExecutor();
        taskExecutor.setConcurrencyLimit(10);//10 thread to execute parallel
        return taskExecutor;
    }
}
