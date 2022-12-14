/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2012-2019. All rights reserved.
 */

package org.chail;

import org.apache.avro.Schema;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.utilities.schema.SchemaProvider;
import org.apache.spark.api.java.JavaSparkContext;

/**
 * 功能描述
 * 提供sorce和target的schema
 *
 * @since 2021-03-17
 */
public class DataSchemaProviderExample extends SchemaProvider {


    public static final String TRIP_EXAMPLE_SCHEMA =  "{\"type\":\"record\",\"name\":\"hoodie_source\",\"fields\":[{\"name\":\"id\",\"type\":\"string\"},{\"name\":\"name\",\"type\":\"string\"},{\"name\":\"idcard\",\"type\":\"string\"},{\"name\":\"address\",\"type\":\"string\"}]}";


    public static final  Schema avroSchema = new Schema.Parser().parse(
            "{\"type\":\"record\",\"name\":\"mytest_record\",\"namespace\":\"hoodie.mytest\",\"fields\":[{\"name\":\"id\",\"type\":\"string\"},{\"name\":\"name\",\"type\":\"string\"},{\"name\":\"idcard\",\"type\":\"string\"},{\"name\":\"address\",\"type\":\"string\"}]}");


    public DataSchemaProviderExample(TypedProperties props, JavaSparkContext jssc) {
        super(props, jssc);
    }

    /**
     * source schema
     *
     * @return Schema
     */
    @Override
    public Schema getSourceSchema() {
        Schema avroSchema = new Schema.Parser().parse(
                TRIP_EXAMPLE_SCHEMA)  ;
        return avroSchema;
    }

    /**
     * target schema
     *
     * @return Schema
     */
    @Override
    public Schema getTargetSchema() {
        Schema avroSchema = new Schema.Parser().parse(
            "{\"type\":\"record\",\"name\":\"mytest_record\",\"namespace\":\"hoodie.mytest\",\"fields\":[{\"name\":\"id\",\"type\":\"string\"},{\"name\":\"name\",\"type\":\"string\"},{\"name\":\"idcard\",\"type\":\"string\"},{\"name\":\"address\",\"type\":\"string\"}]}");
        return avroSchema;
    }

}
