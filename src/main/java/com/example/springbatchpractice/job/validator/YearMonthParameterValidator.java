package com.example.springbatchpractice.job.validator;

import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.JobParametersInvalidException;
import org.springframework.batch.core.JobParametersValidator;
import org.springframework.util.StringUtils;

import java.time.YearMonth;
import java.time.format.DateTimeParseException;

public class YearMonthParameterValidator implements JobParametersValidator {

    private static final String YEAR_MONTH = "yearMonth";


    @Override
    public void validate(JobParameters jobParameters) throws JobParametersInvalidException {
        String yearMonth = jobParameters.getString(YEAR_MONTH);

        if (!StringUtils.hasText(yearMonth)) {
            throw new JobParametersInvalidException(YEAR_MONTH + "가 빈 문자열이거나 존재하지 않습니다.");
        }

        try {
            YearMonth.parse(yearMonth);
        } catch (DateTimeParseException e) {
            throw new JobParametersInvalidException(YEAR_MONTH + "가 올바른 형식이 아닙니다. yyyy-MM 형식이여야 합니다.");
        }

    }
}
