package com.chail.datasupport.tools.model;

import com.chail.Db2ObjectFiled;
import lombok.Data;

/**
 * @author : yangc
 * @date :2022/7/4 14:23
 * @description :
 * @modyified By:
 */
@Data
public class Table {

    @Db2ObjectFiled("id")
    private String id;

    @Db2ObjectFiled("name")
    private String name;

    @Db2ObjectFiled("description")
    private String des;
}
