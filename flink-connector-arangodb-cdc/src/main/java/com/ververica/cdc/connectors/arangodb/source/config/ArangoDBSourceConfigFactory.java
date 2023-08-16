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

package com.ververica.cdc.connectors.arangodb.source.config;

import org.apache.flink.annotation.Internal;

import com.ververica.cdc.connectors.base.config.SourceConfig.Factory;
import com.ververica.cdc.connectors.base.options.StartupOptions;

import java.util.Arrays;
import java.util.List;

import static com.ververica.cdc.connectors.base.options.SourceOptions.CHUNK_META_GROUP_SIZE;
import static com.ververica.cdc.connectors.base.utils.EnvironmentUtils.checkSupportCheckpointsAfterTasksFinished;
import static com.ververica.cdc.connectors.arangodb.internal.ArangoDBEnvelope.ARANGODB_SCHEME;
import static com.ververica.cdc.connectors.arangodb.internal.ArangoDBEnvelope.ARANGODB_SRV_SCHEME;
import static com.ververica.cdc.connectors.arangodb.source.config.ArangoDBSourceOptions.BATCH_SIZE;
import static com.ververica.cdc.connectors.arangodb.source.config.ArangoDBSourceOptions.HEARTBEAT_INTERVAL_MILLIS;
import static com.ververica.cdc.connectors.arangodb.source.config.ArangoDBSourceOptions.POLL_AWAIT_TIME_MILLIS;
import static com.ververica.cdc.connectors.arangodb.source.config.ArangoDBSourceOptions.POLL_MAX_BATCH_SIZE;
import static com.ververica.cdc.connectors.arangodb.source.config.ArangoDBSourceOptions.SCAN_INCREMENTAL_SNAPSHOT_CHUNK_SIZE_MB;
import static com.ververica.cdc.connectors.arangodb.source.config.ArangoDBSourceOptions.SCHEME;
import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/** A factory to construct {@link ArangoDBSourceConfig}. */
@Internal
public class ArangoDBSourceConfigFactory implements Factory<ArangoDBSourceConfig> {

    private static final long serialVersionUID = 1L;

    private String scheme = SCHEME.defaultValue();
    private String hosts;
    private String username;
    private String password;
    private List<String> databaseList;
    private List<String> collectionList;
    private String connectionOptions;
    private Integer batchSize = BATCH_SIZE.defaultValue();
    private Integer pollAwaitTimeMillis = POLL_AWAIT_TIME_MILLIS.defaultValue();
    private Integer pollMaxBatchSize = POLL_MAX_BATCH_SIZE.defaultValue();
    private boolean updateLookup = true;
    private StartupOptions startupOptions = StartupOptions.initial();
    private Integer heartbeatIntervalMillis = HEARTBEAT_INTERVAL_MILLIS.defaultValue();
    private Integer splitMetaGroupSize = CHUNK_META_GROUP_SIZE.defaultValue();
    private Integer splitSizeMB = SCAN_INCREMENTAL_SNAPSHOT_CHUNK_SIZE_MB.defaultValue();
    private boolean closeIdleReaders = false;
    private boolean enableFullDocPrePostImage = false;
    private boolean disableCursorTimeout = true;

    /** The protocol connected to ArangoDB. For example mongodb or mongodb+srv. */
    public ArangoDBSourceConfigFactory scheme(String scheme) {
        checkArgument(
                ARANGODB_SCHEME.equals(scheme) || ARANGODB_SRV_SCHEME.equals(scheme),
                String.format(
                        "The scheme should either be %s or %s",
                        ARANGODB_SCHEME, ARANGODB_SRV_SCHEME));
        this.scheme = scheme;
        return this;
    }

    /** The comma-separated list of hostname and port pairs of mongodb servers. */
    public ArangoDBSourceConfigFactory hosts(String hosts) {
        this.hosts = hosts;
        return this;
    }

    /**
     * Ampersand (i.e. &) separated ArangoDB connection options eg
     * replicaSet=test&connectTimeoutMS=300000
     * https://docs.mongodb.com/manual/reference/connection-string/#std-label-connections-connection-options
     */
    public ArangoDBSourceConfigFactory connectionOptions(String connectionOptions) {
        this.connectionOptions = connectionOptions;
        return this;
    }

    /** Name of the database user to be used when connecting to ArangoDB. */
    public ArangoDBSourceConfigFactory username(String username) {
        this.username = username;
        return this;
    }

    /** Password to be used when connecting to ArangoDB. */
    public ArangoDBSourceConfigFactory password(String password) {
        this.password = password;
        return this;
    }

    /** Regular expressions list that match database names to be monitored. */
    public ArangoDBSourceConfigFactory databaseList(String... databases) {
        this.databaseList = Arrays.asList(databases);
        return this;
    }

    /**
     * Regular expressions that match fully-qualified collection identifiers for collections to be
     * monitored. Each identifier is of the form {@code <databaseName>.<collectionName>}.
     */
    public ArangoDBSourceConfigFactory collectionList(String... collections) {
        this.collectionList = Arrays.asList(collections);
        return this;
    }

    /**
     * batch.size
     *
     * <p>The cursor batch size. Default: 1024
     */
    public ArangoDBSourceConfigFactory batchSize(int batchSize) {
        checkArgument(batchSize >= 0);
        this.batchSize = batchSize;
        return this;
    }

    /**
     * poll.await.time.ms
     *
     * <p>The amount of time to wait before checking for new results on the change stream. Default:
     * 1000
     */
    public ArangoDBSourceConfigFactory pollAwaitTimeMillis(int pollAwaitTimeMillis) {
        checkArgument(pollAwaitTimeMillis > 0);
        this.pollAwaitTimeMillis = pollAwaitTimeMillis;
        return this;
    }

    /**
     * poll.max.batch.size
     *
     * <p>Maximum number of change stream documents to include in a single batch when polling for
     * new data. This setting can be used to limit the amount of data buffered internally in the
     * connector. Default: 1024
     */
    public ArangoDBSourceConfigFactory pollMaxBatchSize(int pollMaxBatchSize) {
        checkArgument(pollMaxBatchSize > 0);
        this.pollMaxBatchSize = pollMaxBatchSize;
        return this;
    }

    /**
     * scan.startup.mode
     *
     * <p>Optional startup mode for ArangoDB CDC consumer, valid enumerations are initial,
     * latest-offset, timestamp. Default: initial
     */
    public ArangoDBSourceConfigFactory startupOptions(StartupOptions startupOptions) {
        checkNotNull(startupOptions);
        switch (startupOptions.startupMode) {
            case INITIAL:
            case LATEST_OFFSET:
            case TIMESTAMP:
                this.startupOptions = startupOptions;
                return this;
            default:
                throw new IllegalArgumentException(
                        "Unsupported startup mode " + startupOptions.startupMode);
        }
    }

    /**
     * heartbeat.interval.ms
     *
     * <p>The length of time in milliseconds between sending heartbeat messages. Heartbeat messages
     * contain the post batch resume token and are sent when no source records have been published
     * in the specified interval. This improves the resumability of the connector for low volume
     * namespaces. Use 0 to disable.
     */
    public ArangoDBSourceConfigFactory heartbeatIntervalMillis(int heartbeatIntervalMillis) {
        checkArgument(heartbeatIntervalMillis >= 0);
        this.heartbeatIntervalMillis = heartbeatIntervalMillis;
        return this;
    }

    /**
     * scan.incremental.snapshot.chunk.size.mb
     *
     * <p>The chunk size mb of incremental snapshot. Default: 64mb.
     */
    public ArangoDBSourceConfigFactory splitSizeMB(int splitSizeMB) {
        checkArgument(splitSizeMB > 0);
        this.splitSizeMB = splitSizeMB;
        return this;
    }

    /**
     * The group size of split meta, if the meta size exceeds the group size, the meta will be
     * divided into multiple groups.
     */
    public ArangoDBSourceConfigFactory splitMetaGroupSize(int splitMetaGroupSize) {
        this.splitMetaGroupSize = splitMetaGroupSize;
        return this;
    }

    /**
     * Whether to close idle readers at the end of the snapshot phase. This feature depends on
     * FLIP-147: Support Checkpoints After Tasks Finished. The flink version is required to be
     * greater than or equal to 1.14, and the configuration <code>
     * 'execution.checkpointing.checkpoints-after-tasks-finish.enabled'</code> needs to be set to
     * true.
     *
     * <p>See more
     * https://cwiki.apache.org/confluence/display/FLINK/FLIP-147%3A+Support+Checkpoints+After+Tasks+Finished.
     */
    public ArangoDBSourceConfigFactory closeIdleReaders(boolean closeIdleReaders) {
        this.closeIdleReaders = closeIdleReaders;
        return this;
    }

    /**
     * scan.full-changelog
     *
     * <p>Whether to generate full mode row data by looking up full document pre- and post-image
     * collections. Requires ArangoDB >= 6.0.
     */
    public ArangoDBSourceConfigFactory scanFullChangelog(boolean enableFullDocPrePostImage) {
        this.enableFullDocPrePostImage = enableFullDocPrePostImage;
        return this;
    }

    /**
     * whether pass <code>noCursorTimeout</code> config when creating ArangoDB cursor. Defaults to
     * true.
     */
    public ArangoDBSourceConfigFactory disableCursorTimeout(boolean disableCursorTimeout) {
        this.disableCursorTimeout = disableCursorTimeout;
        return this;
    }

    /** Creates a new {@link ArangoDBSourceConfig} for the given subtask {@code subtaskId}. */
    @Override
    public ArangoDBSourceConfig create(int subtaskId) {
        checkSupportCheckpointsAfterTasksFinished(closeIdleReaders);
        return new ArangoDBSourceConfig(
                scheme,
                hosts,
                username,
                password,
                databaseList,
                collectionList,
                connectionOptions,
                batchSize,
                pollAwaitTimeMillis,
                pollMaxBatchSize,
                updateLookup,
                startupOptions,
                heartbeatIntervalMillis,
                splitMetaGroupSize,
                splitSizeMB,
                closeIdleReaders,
                enableFullDocPrePostImage,
                disableCursorTimeout);
    }
}
