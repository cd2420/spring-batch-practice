package com.example.springbatchpractice.job.lawd;

import com.example.springbatchpractice.core.entity.Lawd;
import com.example.springbatchpractice.core.service.LawdService;
import com.example.springbatchpractice.job.validator.FilePathParameterValidator;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.JobScope;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.builder.FlatFileItemReaderBuilder;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.ClassPathResource;

import static com.example.springbatchpractice.job.lawd.LawdFiledSetMapper.*;

@Configuration
@RequiredArgsConstructor
@Slf4j
public class LawdInsertJobConfig {

    private final JobBuilderFactory jobBuilderFactory;
    private final StepBuilderFactory stepBuilderFactory;
    private final LawdService lawdService;

    @Bean
    public Job lawdInsertJob(Step lawdInsertStep) {
        return jobBuilderFactory.get("lawdInsertJob")
                .incrementer(new RunIdIncrementer())
                .validator(new FilePathParameterValidator())
                .start(lawdInsertStep)
                .build();
    }

    @JobScope
    @Bean
    public Step lawdInsertStep(FlatFileItemReader<Lawd> lawdFileItemReader, ItemWriter<Lawd> lawdItemWriter) {
        return stepBuilderFactory.get("lawdInsertStep")
                .<Lawd, Lawd>chunk(1000)
                .reader(lawdFileItemReader)
                .writer(lawdItemWriter)
                .build();
    }

    @Bean
    @StepScope
    public FlatFileItemReader<Lawd> lawdFileItemReader(@Value("#{jobParameters['filePath']}") String filePath) {
        return new FlatFileItemReaderBuilder<Lawd>()
                .name("lawdFileItemReader")
                .delimited()     // 구분자로 나눠져 있어서 delimited
                .delimiter("\t")
                .names(LAWD_CD, LAWD_DONG, EXIST)        // field set mapper 에서 객체에 mapping 해줄 name
                .linesToSkip(1)                              // 1번째 라인은 skip
                .fieldSetMapper(new LawdFiledSetMapper())
                .resource(new ClassPathResource(filePath))
                .build();

    }

    @Bean
    @StepScope
    public ItemWriter<Lawd> lawdItemWriter() {
        return items -> items.forEach(lawdService::upsert);
    }
}
