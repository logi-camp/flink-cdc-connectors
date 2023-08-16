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

package com.ververica.cdc.connectors.arangodb.source.reader.fetch;

import com.mongodb.client.model.changestream.OperationType;
import com.ververica.cdc.connectors.base.source.meta.offset.Offset;
import com.ververica.cdc.connectors.base.source.meta.split.SourceSplitBase;
import com.ververica.cdc.connectors.base.source.reader.external.FetchTask;
import com.ververica.cdc.connectors.arangodb.internal.ArangoDBEnvelope;
import com.ververica.cdc.connectors.arangodb.source.config.ArangoDBSourceConfig;
import com.ververica.cdc.connectors.arangodb.source.dialect.ArangoDBDialect;
import com.ververica.cdc.connectors.arangodb.source.offset.ChangeStreamDescriptor;
import com.ververica.cdc.connectors.arangodb.source.offset.ChangeStreamOffset;
import com.ververica.cdc.connectors.arangodb.source.utils.ArangoDBRecordUtils;
import io.debezium.connector.base.ChangeEventQueue;
import io.debezium.pipeline.DataChangeEvent;
import io.debezium.relational.TableId;
import io.debezium.relational.Tables;
import io.debezium.util.LoggingContext;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.bson.BsonDocument;
import org.bson.BsonType;
import org.bson.BsonValue;

import java.time.Duration;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static com.ververica.cdc.connectors.arangodb.source.utils.BsonUtils.compareBsonValue;
import static com.ververica.cdc.connectors.arangodb.source.utils.ArangoDBRecordUtils.getDocumentKey;
import static com.ververica.cdc.connectors.arangodb.source.utils.ArangoDBRecordUtils.getResumeToken;

/** The context for fetch task that fetching data of snapshot split from MongoDB data source. */
public class ArangoDBFetchTaskContext implements FetchTask.Context {

    private final ArangoDBDialect dialect;
    private final ArangoDBSourceConfig sourceConfig;
    private final ChangeStreamDescriptor changeStreamDescriptor;
    private ChangeEventQueue<DataChangeEvent> changeEventQueue;

    public ArangoDBFetchTaskContext(
            ArangoDBDialect dialect,
            ArangoDBSourceConfig sourceConfig,
            ChangeStreamDescriptor changeStreamDescriptor) {
        this.dialect = dialect;
        this.sourceConfig = sourceConfig;
        this.changeStreamDescriptor = changeStreamDescriptor;
    }

    public void configure(SourceSplitBase sourceSplitBase) {
        // we need to use small batch size instead of INT.MAX as earlier because
        // now under the hood of debezium the ArrayDequeue was used as queue implementation
        // TODO: replace getBatchSize with getSnapshotBatchSize
        //  when SNAPSHOT_BATCH_SIZE option will be added
        final int queueSize = sourceConfig.getBatchSize();

        this.changeEventQueue =
                new ChangeEventQueue.Builder<DataChangeEvent>()
                        .pollInterval(Duration.ofMillis(sourceConfig.getPollAwaitTimeMillis()))
                        .maxBatchSize(sourceConfig.getPollMaxBatchSize())
                        .maxQueueSize(queueSize)
                        .loggingContextSupplier(
                                () ->
                                        LoggingContext.forConnector(
                                                "mongodb-cdc",
                                                "mongodb-cdc-connector",
                                                "mongodb-cdc-connector-task"))
                        // do not buffer any element, we use signal event
                        // .buffering()
                        .build();
    }

    public ArangoDBSourceConfig getSourceConfig() {
        return sourceConfig;
    }

    public ArangoDBDialect getDialect() {
        return dialect;
    }

    public ChangeStreamDescriptor getChangeStreamDescriptor() {
        return changeStreamDescriptor;
    }

    public ChangeEventQueue<DataChangeEvent> getQueue() {
        return changeEventQueue;
    }

    @Override
    public TableId getTableId(SourceRecord record) {
        return ArangoDBRecordUtils.getTableId(record);
    }

    @Override
    public Tables.TableFilter getTableFilter() {
        // We have pushed down the filters to server side.
        return Tables.TableFilter.includeAll();
    }

    @Override
    public Offset getStreamOffset(SourceRecord record) {
        return new ChangeStreamOffset(getResumeToken(record));
    }

    @Override
    public boolean isDataChangeRecord(SourceRecord record) {
        return ArangoDBRecordUtils.isDataChangeRecord(record);
    }

    @Override
    public boolean isRecordBetween(SourceRecord record, Object[] splitStart, Object[] splitEnd) {
        BsonDocument documentKey = getDocumentKey(record);
        // In the case of a compound index, we can also agree to only compare the range of the first
        // key.
        BsonDocument splitKeys = (BsonDocument) splitStart[0];
        String firstKey = splitKeys.getFirstKey();
        BsonValue keyValue = documentKey.get(firstKey);
        BsonValue lowerBound = ((BsonDocument) splitStart[1]).get(firstKey);
        BsonValue upperBound = ((BsonDocument) splitEnd[1]).get(firstKey);

        // for all range
        if (lowerBound.getBsonType() == BsonType.MIN_KEY
                && upperBound.getBsonType() == BsonType.MAX_KEY) {
            return true;
        }

        // keyValue -> [lower, upper)
        if (compareBsonValue(lowerBound, keyValue) <= 0
                && compareBsonValue(keyValue, upperBound) < 0) {
            return true;
        }

        return false;
    }

    @Override
    public void rewriteOutputBuffer(
            Map<Struct, SourceRecord> outputBuffer, SourceRecord changeRecord) {
        Struct key = (Struct) changeRecord.key();
        Struct value = (Struct) changeRecord.value();
        if (value != null) {
            OperationType operation =
                    OperationType.fromString(value.getString(ArangoDBEnvelope.OPERATION_TYPE_FIELD));
            switch (operation) {
                case INSERT:
                case UPDATE:
                case REPLACE:
                    outputBuffer.put(key, changeRecord);
                    break;
                case DELETE:
                    outputBuffer.remove(key);
                    break;
                default:
                    throw new IllegalStateException(
                            String.format(
                                    "Data change record meet UNKNOWN operation, the the record is %s.",
                                    changeRecord));
            }
        }
    }

    @Override
    public List<SourceRecord> formatMessageTimestamp(Collection<SourceRecord> snapshotRecords) {
        return snapshotRecords.stream()
                .peek(
                        record -> {
                            Struct value = (Struct) record.value();
                            // set message timestamp (source.ts_ms) to 0L
                            Struct source =
                                    new Struct(
                                            value.schema()
                                                    .field(ArangoDBEnvelope.SOURCE_FIELD)
                                                    .schema());
                            source.put(ArangoDBEnvelope.TIMESTAMP_KEY_FIELD, 0L);
                            source.put(ArangoDBEnvelope.SNAPSHOT_KEY_FIELD, "true");
                            value.put(ArangoDBEnvelope.SOURCE_FIELD, source);
                        })
                .collect(Collectors.toList());
    }

    @Override
    public void close() throws Exception {}
}
