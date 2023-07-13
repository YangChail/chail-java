package com.chail.kafka.kafka2kafka.parse;

/**
 * @author mayongjie
 * @date 2022/03/23 19:42
 **/
public class BinaryColumnConstant {

    /**
     * ORACLE
     */
    public static final int ORACLE_TYPE_RAW = 23;
    public static final int ORACLE_TYPE_LONG_RAW = 24;
    public static final int ORACLE_TYPE_BLOB = 113;

    /**
     * MYSQL
     */
    public final static int BINARY          =  -2;
    public final static int VARBINARY       =  -3;
    public final static int LONGVARBINARY   =  -4;
    public final static int BLOB            = 2004;

    /**
     * MSSQL
     */
    public final static int TIMESTAMP        =  189;
    public final static int IMAGE            =  34;
}
