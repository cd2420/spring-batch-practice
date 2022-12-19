package com.example.springbatchpractice.job.validator;

import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.JobParametersInvalidException;
import org.springframework.batch.core.JobParametersValidator;
import org.springframework.util.StringUtils;

public class LawdCdValidator implements JobParametersValidator {
    private static final String LAWD_CD = "lawdCd";

    @Override
    public void validate(JobParameters jobParameters) throws JobParametersInvalidException {
        String lawdCd = jobParameters.getString(LAWD_CD);

        if(isNotValid(lawdCd)) {
            throw new JobParametersInvalidException(LAWD_CD + "은 5자리 문자열이여야 합니다.");
        }

    }

    private boolean isNotValid(String lawdCd) {
        return !isValid(lawdCd);
    }

    private boolean isValid(String lawdCd) {
        return StringUtils.hasText(lawdCd) && lawdCd.length() == 5;
    }

}
