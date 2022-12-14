package org.chail;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.parser.Feature;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.utilities.deltastreamer.HoodieDeltaStreamerMetrics;
import org.apache.hudi.utilities.schema.SchemaProvider;
import org.apache.hudi.utilities.sources.InputBatch;
import org.apache.hudi.utilities.sources.JsonSource;
import org.apache.hudi.utilities.sources.helpers.KafkaOffsetGen;
import org.apache.hudi.utilities.sources.helpers.KafkaOffsetGen.CheckpointUtils;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;
import org.apache.spark.streaming.kafka010.OffsetRange;
import java.util.Map;

/**
 * Read json kafka data.
 */
public class MyJsonKafkaSource extends JsonSource {

    private static final Logger LOG = LogManager.getLogger(MyJsonKafkaSource.class);

    private final KafkaOffsetGen offsetGen;

    private final HoodieDeltaStreamerMetrics metrics;

    public MyJsonKafkaSource(TypedProperties properties, JavaSparkContext sparkContext, SparkSession sparkSession,
                             SchemaProvider schemaProvider) {
        super(properties, sparkContext, sparkSession, schemaProvider);
        HoodieWriteConfig.Builder builder = HoodieWriteConfig.newBuilder();
        this.metrics = new HoodieDeltaStreamerMetrics(builder.withProperties(properties).build());
        properties.put("key.deserializer", StringDeserializer.class);
        properties.put("value.deserializer", StringDeserializer.class);
        offsetGen = new KafkaOffsetGen(properties);
    }

    @Override
    protected InputBatch<JavaRDD<String>> fetchNewData(Option<String> lastCheckpointStr, long sourceLimit) {
        OffsetRange[] offsetRanges = offsetGen.getNextOffsetRanges(lastCheckpointStr, sourceLimit, metrics);
        long totalNewMsgs = CheckpointUtils.totalNewMessages(offsetRanges);
        LOG.info("About to read " + totalNewMsgs + " from Kafka for topic :" + offsetGen.getTopicName());
        if (totalNewMsgs <= 0) {
            return new InputBatch<>(Option.empty(), CheckpointUtils.offsetsToStr(offsetRanges));
        }
        JavaRDD<String> newDataRDD = toRDD(offsetRanges);
        return new InputBatch<>(Option.of(newDataRDD), CheckpointUtils.offsetsToStr(offsetRanges));
    }

    private JavaRDD<String> toRDD(OffsetRange[] offsetRanges) {
        return KafkaUtils.createRDD(this.sparkContext, this.offsetGen.getKafkaParams(), offsetRanges, LocationStrategies.PreferConsistent()).filter((x)->{
            //过滤空行和脏数据
            String msg = (String)x.value();
            if (msg == null) {
                return false;
            }
            try{
                String op = DebeziumJsonParser.getOP(msg);
            }catch (Exception e){
                return false;
            }
            return true;
        }).map((x) -> {
            //将debezium接进来的数据解析写进map,在返回map的tostring, 这样结构改动最小
            String msg = (String)x.value();
            String op = DebeziumJsonParser.getOP(msg);
            JSONObject json_obj = JSON.parseObject(msg, Feature.OrderedField);
            Boolean is_delete = false;
            String out_str = "";
            Object out_obj = new Object();
            if(op.equals("c")){
                out_obj =  json_obj.getJSONObject("payload").get("after");
            }
            else if(op.equals("u")){
                out_obj =   json_obj.getJSONObject("payload").get("after");
            }
            else {
                is_delete = true;
                out_obj =   json_obj.getJSONObject("payload").get("before");
            }
            Map out_map = (Map)out_obj;
            out_map.put("_hoodie_is_deleted",is_delete);
            out_map.put("op",op);

            return out_map.toString();
        });
    }
}