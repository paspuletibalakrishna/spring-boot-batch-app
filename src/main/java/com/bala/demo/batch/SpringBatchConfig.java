package com.bala.demo.batch;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.FlatFileItemWriter;
import org.springframework.batch.item.file.mapping.PassThroughLineMapper;
import org.springframework.batch.item.file.transform.PassThroughLineAggregator;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.FileSystemResource;



@Configuration
@EnableBatchProcessing
public class SpringBatchConfig {
	
	@Autowired
    JobBuilderFactory jobBuilderFactory;
	
	@Autowired
    StepBuilderFactory stepBuilderFactory;
	
	Logger logger = LoggerFactory.getLogger(SpringBatchConfig.class);
	
	@Bean
    @StepScope
    public FlatFileItemReader flatFileItemReader(@Value("#{jobParameters['file']}") String file) {
        FlatFileItemReader reader = new FlatFileItemReader<>();
        reader.setResource(new ClassPathResource(file));
        reader.setLineMapper(new PassThroughLineMapper());
        return reader;
    }
	
	@Bean
	@StepScope
	public FlatFileItemWriter flatFileItemWriter() {
	 FlatFileItemWriter flatFileItemWriter = new FlatFileItemWriter<>();
	 flatFileItemWriter.setShouldDeleteIfExists(true);
	 flatFileItemWriter.setResource(new FileSystemResource("/Users/paspuletibalakrishna/Desktop/Balu/modified_sample.txt"));
	 flatFileItemWriter.setLineAggregator(new PassThroughLineAggregator());
	 return flatFileItemWriter;
	}
	
	@Bean
    public Job job() {
        return jobBuilderFactory.get("flowJob")
                .incrementer(new RunIdIncrementer())
                .start(staxFileWriterStep())
                .build();
    }
	
	private Step staxFileWriterStep() {
        return stepBuilderFactory.get("staxFileWriterStep")
                .chunk(5)
                .reader(flatFileItemReader(null))
                .writer(flatFileItemWriter())
                .build();
    }

}
