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
import org.apache.flink.annotation.PublicEvolving;

import com.ververica.cdc.connectors.base.options.StartupOptions;
import com.ververica.cdc.connectors.arangodb.source.config.ArangoDBSourceConfigFactory;
import com.ververica.cdc.debezium.DebeziumDeserializationSchema;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * The builder class for {@link ArangoDBSource} to make it easier for the users to construct a {@link
 * ArangoDBSource}.
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
 * <p>Check the Java docs of each individual method to learn more about the settings to build a
 * {@link ArangoDBSource}.
 */
@Experimental
@PublicEvolving
public class ArangoDBSourceBuilder<T> {

    private final ArangoDBSourceConfigFactory configFactory = new ArangoDBSourceConfigFactory();
    private DebeziumDeserializationSchema<T> deserializer;

    /** The protocol connected to ArangoDB. For example ArangoDB or ArangoDB+srv. */
    public ArangoDBSourceBuilder<T> scheme(String scheme) {
        this.configFactory.scheme(scheme);
        return this;
    }

    /** The comma-separated list of hostname and port pairs of ArangoDB servers. */
    public ArangoDBSourceBuilder<T> hosts(String hosts) {
        this.configFactory.hosts(hosts);
        return this;
    }

    /**
     * Ampersand (i.e. &) separated ArangoDB connection options eg
     * replicaSet=test&connectTimeoutMS=300000
     * https://docs.ArangoDB.com/manual/reference/connection-string/#std-label-connections-connection-options
     */
    public ArangoDBSourceBuilder<T> connectionOptions(String connectionOptions) {
        this.configFactory.connectionOptions(connectionOptions);
        return this;
    }

    /** Name of the database user to be used when connecting to ArangoDB. */
    public ArangoDBSourceBuilder<T> username(String username) {
        this.configFactory.username(username);
        return this;
    }

    /** Password to be used when connecting to ArangoDB. */
    public ArangoDBSourceBuilder<T> password(String password) {
        this.configFactory.password(password);
        return this;
    }

    /** Regular expressions list that match database names to be monitored. */
    public ArangoDBSourceBuilder<T> databaseList(String... databases) {
        this.configFactory.databaseList(databases);
        return this;
    }

    /**
     * Regular expressions that match fully-qualified collection identifiers for collections to be
     * monitored. Each identifier is of the form {@code <databaseName>.<collectionName>}.
     */
    public ArangoDBSourceBuilder<T> collectionList(String... collections) {
        this.configFactory.collectionList(collections);
        return this;
    }

    /**
     * batch.size
     *
     * <p>The cursor batch size. Default: 1024
     */
    public ArangoDBSourceBuilder<T> batchSize(int batchSize) {
        this.configFactory.batchSize(batchSize);
        return this;
    }

    /**
     * poll.await.time.ms
     *
     * <p>The amount of time to wait before checking for new results on the change stream. Default:
     * 1000
     */
    public ArangoDBSourceBuilder<T> pollAwaitTimeMillis(int pollAwaitTimeMillis) {
        checkArgument(pollAwaitTimeMillis > 0);
        this.configFactory.pollAwaitTimeMillis(pollAwaitTimeMillis);
        return this;
    }

    /**
     * poll.max.batch.size
     *
     * <p>Maximum number of change stream documents to include in a single batch when polling for
     * new data. This setting can be used to limit the amount of data buffered internally in the
     * connector. Default: 1024
     */
    public ArangoDBSourceBuilder<T> pollMaxBatchSize(int pollMaxBatchSize) {
        this.configFactory.pollMaxBatchSize(pollMaxBatchSize);
        return this;
    }

    /**
     * scan.startup.mode
     *
     * <p>Optional startup mode for ArangoDB CDC consumer, valid enumerations are initial,
     * latest-offset, timestamp. Default: initial
     */
    public ArangoDBSourceBuilder<T> startupOptions(StartupOptions startupOptions) {
        this.configFactory.startupOptions(startupOptions);
        return this;
    }

    /**
     * heartbeat.interval.ms
     *
     * <p>The length of time in milliseconds between sending heartbeat messages. Heartbeat messages
     * contain the post batch resume token and are sent when no source records have been published
     * in the specified interval. This improves the resumability of the connector for low volume
     * namespaces. Use 0 to disable.
     */
    public ArangoDBSourceBuilder<T> heartbeatIntervalMillis(int heartbeatIntervalMillis) {
        this.configFactory.heartbeatIntervalMillis(heartbeatIntervalMillis);
        return this;
    }

    /**
     * scan.incremental.snapshot.chunk.size.mb
     *
     * <p>The chunk size mb of incremental snapshot. Default: 64mb.
     */
    public ArangoDBSourceBuilder<T> splitSizeMB(int splitSizeMB) {
        this.configFactory.splitSizeMB(splitSizeMB);
        return this;
    }

    /**
     * The group size of split meta, if the meta size exceeds the group size, the meta will be
     * divided into multiple groups.
     */
    public ArangoDBSourceBuilder<T> splitMetaGroupSize(int splitMetaGroupSize) {
        this.configFactory.splitMetaGroupSize(splitMetaGroupSize);
        return this;
    }

    /**
     * scan.incremental.close-idle-reader.enabled
     *
     * <p>Whether to close idle readers at the end of the snapshot phase. This feature depends on
     * FLIP-147: Support Checkpoints After Tasks Finished. The flink version is required to be
     * greater than or equal to 1.14, and the configuration <code>
     * 'execution.checkpointing.checkpoints-after-tasks-finish.enabled'</code> needs to be set to
     * true.
     *
     * <p>See more
     * https://cwiki.apache.org/confluence/display/FLINK/FLIP-147%3A+Support+Checkpoints+After+Tasks+Finished.
     */
    public ArangoDBSourceBuilder<T> closeIdleReaders(boolean closeIdleReaders) {
        this.configFactory.closeIdleReaders(closeIdleReaders);
        return this;
    }

    /**
     * scan.full-changelog
     *
     * <p>Whether to generate full mode row data by looking up full document pre- and post-image
     * collections. Requires ArangoDB >= 6.0.
     */
    public ArangoDBSourceBuilder<T> scanFullChangelog(boolean enableFullDocPrePostImage) {
        this.configFactory.scanFullChangelog(enableFullDocPrePostImage);
        return this;
    }

    /**
     * Whether disable cursor timeout during snapshot phase. Defaults to true. Only enable this when
     * ArangoDB server doesn't support noCursorTimeout option.
     */
    public ArangoDBSourceBuilder<T> disableCursorTimeout(boolean disableCursorTimeout) {
        this.configFactory.disableCursorTimeout(disableCursorTimeout);
        return this;
    }

    /**
     * The deserializer used to convert from consumed {@link
     * org.apache.kafka.connect.source.SourceRecord}.
     */
    public ArangoDBSourceBuilder<T> deserializer(DebeziumDeserializationSchema<T> deserializer) {
        this.deserializer = deserializer;
        return this;
    }

    /**
     * Build the {@link ArangoDBSource}.
     *
     * @return a ArangoDBParallelSource with the settings made for this builder.
     */
    public ArangoDBSource<T> build() {
        return new ArangoDBSource<>(configFactory, checkNotNull(deserializer));
    }
}
