package org.chail;

import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.utilities.transform.SqlQueryBasedTransformer;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.UUID;

/**
 * @author : yangc
 * @date :2022/12/16 18:21
 * @description :
 * @modyified By:
 */
public class ChailSqlQueryBasedTransformer extends SqlQueryBasedTransformer {
    private static final Logger LOG = LogManager.getLogger(SqlQueryBasedTransformer.class);

    private static final String SRC_PATTERN = "<SRC>";
    private static final String TMP_TABLE = "HOODIE_SRC_TMP_TABLE_";

    static class Config {

        private static final String TRANSFORMER_SQL = "hoodie.deltastreamer.transformer.sql";
    }

    @Override
    public Dataset<Row> apply(JavaSparkContext jsc, SparkSession sparkSession, Dataset<Row> rowDataset, TypedProperties properties) {
        String transformerSQL = properties.getString(Config.TRANSFORMER_SQL);
        if (null == transformerSQL) {
            throw new IllegalArgumentException("Missing configuration : (" + Config.TRANSFORMER_SQL + ")");
        }
        // tmp table name doesn't like dashes
        String tmpTable = TMP_TABLE.concat(UUID.randomUUID().toString().replace("-", "_"));
        LOG.info("Registering tmp table : " + tmpTable);
        rowDataset.createOrReplaceTempView(tmpTable);
        String[] split = transformerSQL.split(";");
        Dataset<Row> rows =null;
        sparkSession.read().format("org.apache.hudi").load(new String[]{"/hudi/test2"}).("test2");
        for (String sqlStr : split) {
            sqlStr = sqlStr.replaceAll(SRC_PATTERN, tmpTable).trim();
            if (!sqlStr.isEmpty()) {
                LOG.info(sqlStr);
                //spark.read.format("org.apache.hudi").load("/hudi/chail_test").createOrReplaceTempView("chail_test")
                // overwrite the same dataset object until the last statement then return.
                rows = sparkSession.sql(sqlStr);
            }
        }
        return rows;
    }
}
