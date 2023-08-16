/*
 * Copyright 2023 Ververica Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.ververica.cdc.connectors.arangodb.source;

import org.apache.flink.annotation.Experimental;
import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.connector.base.source.reader.RecordEmitter;

import com.ververica.cdc.connectors.base.config.SourceConfig;
import com.ververica.cdc.connectors.base.source.IncrementalSource;
import com.ververica.cdc.connectors.base.source.meta.split.SourceRecords;
import com.ververica.cdc.connectors.base.source.meta.split.SourceSplitState;
import com.ververica.cdc.connectors.base.source.metrics.SourceReaderMetrics;
import com.ververica.cdc.connectors.arangodb.source.config.ArangoDBSourceConfig;
import com.ververica.cdc.connectors.arangodb.source.config.ArangoDBSourceConfigFactory;
import com.ververica.cdc.connectors.arangodb.source.dialect.ArangoDBDialect;
import com.ververica.cdc.connectors.arangodb.source.offset.ChangeStreamOffsetFactory;
import com.ververica.cdc.connectors.arangodb.source.reader.ArangoDBRecordEmitter;
import com.ververica.cdc.debezium.DebeziumDeserializationSchema;

/**
 * The ArangoDB CDC Source based on FLIP-27 which supports parallel reading snapshot of collection
 * and then continue to capture data change from change stream.
 *
 * <pre>
 *     1. The source supports parallel capturing database(s) or collection(s) change.
 *     2. The source supports checkpoint in split level when read snapshot data.
 *     3. The source doesn't need apply any lock of ArangoDB.
 * </pre>
 *
 * <pre>{@code
 * ArangoDBSource
 *     .<String>builder()
 *     .hosts("localhost:27017")
 *     .databaseList("mydb")
 *     .collectionList("mydb.users")
 *     .username(username)
 *     .password(password)
 *     .deserializer(new JsonDebeziumDeserializationSchema())
 *     .build();
 * }</pre>
 *
 * <p>See {@link ArangoDBSourceBuilder} for more details.
 *
 * @param <T> the output type of the source.
 */
@Internal
@Experimental
public class ArangoDBSource<T> extends IncrementalSource<T, ArangoDBSourceConfig> {

    private static final long serialVersionUID = 1L;

    ArangoDBSource(
            ArangoDBSourceConfigFactory configFactory,
            DebeziumDeserializationSchema<T> deserializationSchema) {
        super(
                configFactory,
                deserializationSchema,
                new ChangeStreamOffsetFactory(),
                new ArangoDBDialect());
    }

    /**
     * Get a ArangoDBSourceBuilder to build a {@link ArangoDBSource}.
     *
     * @return a ArangoDB parallel source builder.
     */
    @PublicEvolving
    public static <T> ArangoDBSourceBuilder<T> builder() {
        return new ArangoDBSourceBuilder<>();
    }

    @Override
    protected RecordEmitter<SourceRecords, T, SourceSplitState> createRecordEmitter(
            SourceConfig sourceConfig, SourceReaderMetrics sourceReaderMetrics) {
        return new ArangoDBRecordEmitter<>(
                deserializationSchema, sourceReaderMetrics, offsetFactory);
    }
}
