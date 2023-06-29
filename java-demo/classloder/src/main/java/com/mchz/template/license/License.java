package com.mchz.template.license;


import lombok.Getter;
import lombok.Setter;
import org.apache.commons.lang3.builder.ToStringBuilder;

import java.util.Date;

@Getter
@Setter
public class License {
    /**
     * DM
     */
    private String keyName;

    private Date registDate;

    private Date endDate;

    private String hostId;

    private Integer dbNum;

    private String verifyValue;

    private String md5Value;

    private Date insertTime;

    private Date updateTime;
    /**
     * 识别码
     */
    private String identifier;

    public String toMd5String() {
        StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append(this.getHostId()).append(DateUtils.date2String2(this.getRegistDate())).append(DateUtils.date2String2(this.getEndDate()));
        stringBuilder.append(this.getKeyName()).append("1c2f3f4");
        if (hostId!=null&&hostId.length()>0){
        	 stringBuilder.append("99");
        }
        else{
        	   stringBuilder.append(this.getDbNum());
        }
        System.out.println(stringBuilder.toString());
        return MD5Util.md5(stringBuilder.toString());
    }

    @Override
    public String toString() {
        return ToStringBuilder.reflectionToString(this);
    }

}
