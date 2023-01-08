package com.example.springbatchpractice.job.apt;

import com.example.springbatchpractice.adapter.ApartmentApiResource;
import com.example.springbatchpractice.core.dto.AptDealDto;
import com.example.springbatchpractice.core.repository.LawdRepository;
import com.example.springbatchpractice.job.validator.LawdCdValidator;
import com.example.springbatchpractice.job.validator.YearMonthParameterValidator;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.*;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.JobScope;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.core.job.CompositeJobParametersValidator;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.xml.StaxEventItemReader;
import org.springframework.batch.item.xml.builder.StaxEventItemReaderBuilder;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.oxm.jaxb.Jaxb2Marshaller;

import java.time.YearMonth;
import java.util.Arrays;
import java.util.List;

@Configuration
@RequiredArgsConstructor
@Slf4j
public class AptDealInsertJobConfig {

    private final JobBuilderFactory jobBuilderFactory;
    private final StepBuilderFactory stepBuilderFactory;

    private final ApartmentApiResource apartmentApiResource;
    private final LawdRepository lawdRepository;

    @Bean
    public Job aptDealInsertJob(
            Step guLawdCdStep
            , Step contextPrintStep
//            , Step aptDealInsertStep
    ) {
        return jobBuilderFactory.get("aptDealInsertJob")
                .incrementer(new RunIdIncrementer())
                .validator(aptDealJobParameterValidator())
                .start(guLawdCdStep)
                .on("CONTINUABLE").to(contextPrintStep).next(guLawdCdStep)// exitCode에 CONTINUABLE 이라는 패턴이 있으면  contextPrintStep를 실행해주고 다시 guLawdCdStep 실행한다. 만약 또 CONTINUABLE이 있으면 contextPrintStep를 실행 이 반복됨.
                .from(guLawdCdStep)
                .on("*").end() // exitCode가 CONTINUABLE이 아니라면 종료해라
                .end()
                .build()
                ;
    }

    private JobParametersValidator aptDealJobParameterValidator() {
        CompositeJobParametersValidator validator = new CompositeJobParametersValidator();
        validator.setValidators(Arrays.asList(new YearMonthParameterValidator()));
        return validator;
    }

    @Bean
    @JobScope
    public Step aptDealInsertStep(
            StaxEventItemReader<AptDealDto> aptDealResourceReader
            , ItemWriter<AptDealDto> aptDealWriter
    ) {
        return stepBuilderFactory.get("aptDealInsertStep")
                .<AptDealDto, AptDealDto>chunk(10) // 1번째 제너릭 : 데이터를 읽을 타입, 2번재 제너릭 : 다음 프로세스로(여기서는 writer 부분) 데이터를 넘길 타입
                .reader(aptDealResourceReader)
                .writer(aptDealWriter)
                .build();
    }

    @Bean
    @JobScope
    public Step guLawdCdStep(Tasklet guLawdCdTasklet) {
        return stepBuilderFactory.get("guLawdCdStep")
                .tasklet(guLawdCdTasklet)
                .build();
    }

    /**
     * ExcutionContext에 저장할 데이터
     * 1. guLawdCdList - 구 코드 리스트
     * 2. guLawdCd - 구 코드 -> 다음 스탭에서 활용할 값
     * 3. itemCount - 남아있는 구 코드와 갯수
     * @return
     */
    @Bean
    @StepScope
    public Tasklet guLawdCdTasklet() {
        return (stepContribution, chunkContext) -> {

            StepExecution stepExecution = chunkContext.getStepContext().getStepExecution();
            ExecutionContext executionContext = stepExecution.getJobExecution().getExecutionContext();

            // 데이터가 있으면 다음 스탭을 실행하도록 하고, 데이터가 없으면 종료되도록 한다.
            // 데이터가 있으면 -> CONTINUABLE
            List<String> guLawdCdList;
            if (!executionContext.containsKey("guLawdCdList")) {
                guLawdCdList = lawdRepository.selectDistinctGuLawdCd();
                executionContext.put("guLawdCdList", guLawdCdList);
                executionContext.putInt("itemCount", guLawdCdList.size());
            } else {
                guLawdCdList = (List<String>) executionContext.get("guLawdCdList");
            }

            Integer itemCount = executionContext.getInt("itemCount");

            if (itemCount == 0) {
                stepContribution.setExitStatus(ExitStatus.COMPLETED);
                return RepeatStatus.FINISHED;
            }

            itemCount--;
            executionContext.putString("guLawdCd", guLawdCdList.get(itemCount));
            executionContext.putInt("itemCount", itemCount);
            stepContribution.setExitStatus(new ExitStatus("CONTINUABLE"));

            return RepeatStatus.FINISHED;
        };
    }

    @JobScope
    @Bean
    public Step contextPrintStep(Tasklet contextPrintTasklet) {
        return stepBuilderFactory.get("contextPrintStep")
                .tasklet(contextPrintTasklet)
                .build();
    }

    @StepScope
    @Bean
    public Tasklet contextPrintTasklet(
            @Value("#{jobExecutionContext['guLawdCd']}") String guLawdCd
    ) {
        return (stepContribution, chunkContext) -> {
            System.out.println("[contextPrintTasklet] guLawdCd " + guLawdCd);
            return RepeatStatus.FINISHED;
        };
    }

    @Bean
    @StepScope
    public StaxEventItemReader<AptDealDto> aptDealResourceReader(
            @Value("#{jobParameters['yearMonth']}") String yearMonth,
//            @Value("#{jobParameters['lawdCd']}") String lawdCd,
            @Value("#{jobExecutionContext['guLawdCd']}") String guLawdCd,
             Jaxb2Marshaller aptDealDtoMarshaller
            ) {
        return new StaxEventItemReaderBuilder<AptDealDto>()
                .name("aptDealResourceReader")
                .resource(apartmentApiResource.getResource(guLawdCd, YearMonth.parse(yearMonth)))
                .addFragmentRootElements("item")            // 각 데이터들의 root를 적음
                .unmarshaller(aptDealDtoMarshaller)
                .build()
                ;
    }

    @Bean
    @StepScope
    public Jaxb2Marshaller aptDealDtoMarshaller() {
        Jaxb2Marshaller jaxb2Marshaller = new Jaxb2Marshaller();
        jaxb2Marshaller.setClassesToBeBound(AptDealDto.class);
        return jaxb2Marshaller;
    }

    @Bean
    @StepScope
    public ItemWriter<AptDealDto> aptDealWriter() {
        return items -> {
            items.forEach(System.out::println);  // chunk size 만큼 items 에 데이터가 들어와 있음
            System.out.println("==============writing Complete ==============");
        };
    }

}
