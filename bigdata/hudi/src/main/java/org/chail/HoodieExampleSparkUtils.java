/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2012-2019. All rights reserved.
 */

package org.chail;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.SparkSession;

import java.util.HashMap;
import java.util.Map;

/**
 * 功能描述
 * spark 工具类
 *
 * @since 2021-03-17
 */
public class HoodieExampleSparkUtils {
    private static Map<String, String> defaultConf() {
        Map<String, String> additionalConfigs = new HashMap<>();
        additionalConfigs.put("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
        additionalConfigs.put("spark.kryoserializer.buffer.max", "512m");
        additionalConfigs.put("spark.hadoop.fs.s3a.access.key", "chail");
        additionalConfigs.put("spark.hadoop.fs.s3a.secret.key", "hzmc321#");
        additionalConfigs.put("spark.hadoop.fs.s3a.endpoint", "http://192.168.51.194:9000");
        additionalConfigs.put("spark.hadoop.fs.s3a.path.style.access", "true");
        additionalConfigs.put("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem");
        additionalConfigs.put("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider");
        return additionalConfigs;
    }

    /**
     * 生成默认SparkConf
     *
     * @param String appName
     * @return SparkConf
     */
    public static SparkConf defaultSparkConf(String appName) {
        return buildSparkConf(appName, defaultConf());
    }

    /**
     * 生成自定义SparkConf
     *
     * @param String appName
     * @param Map<String, String> additionalConfigs
     * @return SparkConf
     */
    public static SparkConf buildSparkConf(String appName, Map<String, String> additionalConfigs) {
        SparkConf sparkConf = new SparkConf().setAppName(appName);
        additionalConfigs.forEach(sparkConf::set);
        return sparkConf;
    }

    /**
     * 生成默认SparkSession
     *
     * @param String appName
     * @return SparkSession
     */
    public static SparkSession defaultSparkSession(String appName) {
        return buildSparkSession(appName, defaultConf());
    }

    /**
     * 生成自定义SparkSession
     *
     * @param String appName
     * @param Map<String, String> additionalConfigs
     * @return SparkSession
     */
    public static SparkSession buildSparkSession(String appName, Map<String, String> additionalConfigs) {
        SparkSession.Builder builder = SparkSession.builder().appName(appName);
        additionalConfigs.forEach(builder::config);
        return builder.getOrCreate();
    }
}
