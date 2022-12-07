package com.example.springbatchpractice.job.lawd;

import com.example.springbatchpractice.core.entity.Lawd;
import org.springframework.batch.item.file.mapping.FieldSetMapper;
import org.springframework.batch.item.file.transform.FieldSet;
import org.springframework.validation.BindException;

public class LawdFiledSetMapper implements FieldSetMapper<Lawd> {

    public static final String LAWD_CD = "lawdCd";
    public static final String LAWD_DONG = "lawdDong";
    public static final String EXIST = "exist";

    public static final String EXIST_TRUE = "존재";
    /**
     * 한 라인에 있는 fieldSet 에서 값을 읽어서 Lawd 와 맵핑시켜줌
     */
    @Override
    public Lawd mapFieldSet(FieldSet fieldSet) throws BindException {
        Lawd lawd = new Lawd();
        lawd.setLawdCd(fieldSet.readString(LAWD_CD)); // FlatFileItemReaderBuilder의 names() 에 파라미터로 넘긴 값들
        lawd.setLawdDong(fieldSet.readString(LAWD_DONG));
        lawd.setExist(fieldSet.readBoolean(EXIST, EXIST_TRUE));
        return lawd;
    }
}
