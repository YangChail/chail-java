package com.chail.flink.api.sink;

import com.chail.flink.api.source.ClinkSource;
import com.chail.flink.model.Event;
import com.mchz.flink.connector.jdbc.JdbcConnectionOptions;
import com.mchz.flink.connector.jdbc.JdbcExecutionOptions;
import com.mchz.flink.connector.jdbc.JdbcSink;
import com.mchz.flink.connector.jdbc.JdbcStatementBuilder;
import com.mchz.mcdatasource.core.DataBaseType;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * @author : yangc
 * @date :2023/6/30 16:18
 * @description : 自定义分区
 * @modyified By:
 */
public class SinkToJdbcTest {


    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);
        DataStreamSource<Event> stream = env.addSource(new ClinkSource());
        JdbcConnectionOptions options =
                new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                        .withUrl("192.168.51.139")
                        .withPort(5432)
                        .withDatabaseName("dm")
                        .withDbType(DataBaseType.PGSQL.id)
                        .withUsername("dm")
                        .withPassword("hzmcdm")
                        .build();
        String sql="insert into public.a_test_chail (\"user\", url, \"time\", age) values (?,?,?,?)";


        JdbcExecutionOptions build = JdbcExecutionOptions.builder()
                .withBatchSize(2)
                .withBatchIntervalMs(3)
                .withMaxRetries(5)
                .build();

        //stream.print();
        stream.addSink(JdbcSink.sink(
                sql, new JdbcStatementBuilder<Event>() {
                    @Override
                    public void accept(PreparedStatement preparedStatement, Event event) throws SQLException {
                        preparedStatement.setString(1,event.getUser());
                        preparedStatement.setString(2,event.getUrl());
                        preparedStatement.setObject(3,event.getTime());
                        preparedStatement.setObject(4,event.getAge());
                    }
                },build,options));
        stream.print();
        env.execute();

    }

}
