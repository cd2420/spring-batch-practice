package com.example.springbatchpractice.core.dto;

import lombok.Getter;
import lombok.ToString;

import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;

@ToString
@Getter
@XmlRootElement(name = "item")
public class AptDealDto {

    @XmlElement(name = "거래금액")
    private String dealAmount;

    @XmlElement(name = "건축년도")
    private Integer builtYear;

    @XmlElement(name = "년")
    private Integer year;

    @XmlElement(name = "법정동")
    private String dong;

    @XmlElement(name = "아파트")
    private String aptName;

    @XmlElement(name = "월")
    private Integer month;

    @XmlElement(name = "일")
    private Integer date;

    @XmlElement(name = "전용면적")
    private Double exclusiveArea;

    @XmlElement(name = "지번")
    private String jibun;

    @XmlElement(name = "지역코드")
    private String regionCode;

    @XmlElement(name = "층")
    private Integer floor;

    @XmlElement(name = "해제사유발생일")
    private String dealCanceledDate;

    @XmlElement(name = "해제여부")
    private String dealCanceled;
}
