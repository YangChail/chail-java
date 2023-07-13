package com.chail.flink.project;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.format.DecodingFormat;
import org.apache.flink.table.connector.format.EncodingFormat;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.factories.DeserializationFormatFactory;
import org.apache.flink.table.factories.DynamicTableFactory;
import org.apache.flink.table.factories.SerializationFormatFactory;
import org.apache.flink.table.types.DataType;
import org.apache.flink.types.RowKind;

import java.io.IOException;
import java.util.Collections;
import java.util.Set;

/**
 * @author : yangc
 * @date :2023/7/12 14:01
 * @description :
 * @modyified By:
 */
public class DsDeserializationFactory implements DeserializationFormatFactory, SerializationFormatFactory {
    @Override
    public DecodingFormat<DeserializationSchema<RowData>> createDecodingFormat(DynamicTableFactory.Context context, ReadableConfig formatOptions) {




        return new DecodingFormat<DeserializationSchema<RowData>>() {
            @Override
            public DeserializationSchema<RowData> createRuntimeDecoder(DynamicTableSource.Context context, DataType physicalDataType) {

                        return new DeserializationSchema<RowData>() {
                            @Override
                            public RowData deserialize(byte[] message) throws IOException {
                                return null;
                            }

                            @Override
                            public boolean isEndOfStream(RowData nextElement) {
                                return false;
                            }

                            @Override
                            public TypeInformation<RowData> getProducedType() {
                                return null;
                            }
                        };

            }

            @Override
            public ChangelogMode getChangelogMode() {
                return ChangelogMode.newBuilder()
                        .addContainedKind(RowKind.INSERT)
                        .addContainedKind(RowKind.UPDATE_BEFORE)
                        .addContainedKind(RowKind.UPDATE_AFTER)
                        .addContainedKind(RowKind.DELETE)
                        .build();
            }
        };
    }

    @Override
    public String factoryIdentifier() {
        return "ds-json";
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        return Collections.emptySet();
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        return Collections.emptySet();
    }

    @Override
    public Set<ConfigOption<?>> forwardOptions() {
        return DeserializationFormatFactory.super.forwardOptions();
    }

    @Override
    public EncodingFormat<SerializationSchema<RowData>> createEncodingFormat(DynamicTableFactory.Context context, ReadableConfig formatOptions) {
        return new EncodingFormat<SerializationSchema<RowData>>() {
            @Override
            public SerializationSchema<RowData> createRuntimeEncoder(DynamicTableSink.Context context, DataType physicalDataType) {
                return null;
            }

            @Override
            public ChangelogMode getChangelogMode() {
                return null;
            }
        };
    }
}
