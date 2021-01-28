/*
 * Copyright (C) 2017-2019 Dremio Corporation
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

package com.dremio.exec.store.deltalake;

import static com.dremio.exec.store.deltalake.DeltaConstants.DELTA_FIELD_ADD;
import static com.dremio.exec.store.deltalake.DeltaConstants.DELTA_FIELD_COMMIT_INFO;
import static com.dremio.exec.store.deltalake.DeltaConstants.DELTA_FIELD_METADATA;
import static com.dremio.exec.store.deltalake.DeltaConstants.DELTA_FIELD_METADATA_PARTITION_COLS;
import static com.dremio.exec.store.deltalake.DeltaConstants.DELTA_FIELD_METADATA_SCHEMA_STRING;
import static com.dremio.exec.store.deltalake.DeltaConstants.DELTA_FIELD_PROTOCOL;
import static com.dremio.exec.store.deltalake.DeltaConstants.DELTA_FIELD_REMOVE;
import static com.dremio.exec.store.deltalake.DeltaConstants.DELTA_READ_VERSION;
import static com.dremio.exec.store.deltalake.DeltaConstants.DELTA_TIMESTAMP;
import static com.dremio.exec.store.deltalake.DeltaConstants.OP;
import static com.dremio.exec.store.deltalake.DeltaConstants.OP_METRICS;
import static com.dremio.exec.store.deltalake.DeltaConstants.OP_NUM_ADDED_BYTES;
import static com.dremio.exec.store.deltalake.DeltaConstants.OP_NUM_ADDED_FILES;
import static com.dremio.exec.store.deltalake.DeltaConstants.OP_NUM_FILES;
import static com.dremio.exec.store.deltalake.DeltaConstants.OP_NUM_OUTPUT_BYTES;
import static com.dremio.exec.store.deltalake.DeltaConstants.OP_NUM_OUTPUT_ROWS;
import static com.dremio.exec.store.deltalake.DeltaConstants.OP_NUM_REMOVED_BYTES;
import static com.dremio.exec.store.deltalake.DeltaConstants.OP_NUM_REMOVED_FILES;
import static com.dremio.exec.store.deltalake.DeltaConstants.PROTOCOL_MIN_READER_VERSION;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

import org.apache.arrow.util.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dremio.io.FSInputStream;
import com.dremio.io.file.FileSystem;
import com.dremio.io.file.Path;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import com.fasterxml.jackson.databind.module.SimpleModule;

/**
 * DeltaLog reader, specifically for the DeltaLake commit JSON log files. This reader is specifically
 * dedicated to read the initial portions of the data, which identify dataset size, protocol version
 * and the schema.
 */
public class DeltaLogCommitJsonReader implements DeltaLogReader {
    private static final ObjectMapper OBJECT_MAPPER = getObjectMapper();
    private static final Logger logger = LoggerFactory.getLogger(DeltaLogCommitJsonReader.class);

    DeltaLogCommitJsonReader() {
        // to be instantiated only from DeltaLogParser
    }

    @Override
    public DeltaLogSnapshot parseMetadata(FileSystem fs, Path commitFilePath) throws IOException {
        try (final FSInputStream commitFileIs = fs.open(commitFilePath);
             final BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(commitFileIs))) {
            /*
             * Order of entries in DeltaLake commit log file - commitInfo, protocol, metaData, remove, add
             * Apart from commitInfo, other sections are optional.
             */
            final DeltaLogSnapshot snapshot = OBJECT_MAPPER.readValue(bufferedReader.readLine(), DeltaLogSnapshot.class);

            String nextLine;
            while ((nextLine = bufferedReader.readLine())!=null) {
                final JsonNode json = OBJECT_MAPPER.readTree(nextLine);
                if (json.has(DELTA_FIELD_METADATA)) {
                    populateSchema(snapshot, json.get(DELTA_FIELD_METADATA));
                    break;
                } else if (json.has(DELTA_FIELD_PROTOCOL)) {
                    final int minReaderVersion = get(json, 1, JsonNode::intValue, DELTA_FIELD_PROTOCOL,
                            PROTOCOL_MIN_READER_VERSION);
                    Preconditions.checkState(minReaderVersion <= 1,
                            "Protocol version {} is incompatible for Dremio plugin", minReaderVersion);
                } else if (json.has(DELTA_FIELD_REMOVE) || json.has(DELTA_FIELD_ADD)) {
                    // No metadata change detected in this commit.
                    logger.debug("No metadata change detected in {}", commitFilePath);
                    break;
                }
            }

            return snapshot;
        }
    }

    public static void populateSchema(DeltaLogSnapshot snapshot, JsonNode metadata) throws IOException {
        // Check data file format
        final String format = get(metadata, "parquet", JsonNode::asText, "format", "provider");
        Preconditions.checkState(format.equalsIgnoreCase("parquet"), "Non-parquet delta lake tables aren't supported.");

        // Fetch partitions
        final List<String> partitionCols = new ArrayList<>();
        final JsonNode partitionColsJson = metadata.get(DELTA_FIELD_METADATA_PARTITION_COLS);
        if (partitionColsJson!=null) {
            for (int i = 0; i < partitionColsJson.size(); i++) {
                partitionCols.add(partitionColsJson.get(i).asText());
            }
        }

        // Fetch record schema
        final String schemaString = get(metadata, null, JsonNode::asText, DELTA_FIELD_METADATA_SCHEMA_STRING);

        snapshot.setSchema(schemaString, partitionCols);
    }

    private static ObjectMapper getObjectMapper() {
        final ObjectMapper objectMapper = new ObjectMapper();
        final SimpleModule module = new SimpleModule();
        module.addDeserializer(DeltaLogSnapshot.class, new DeltaSnapshotDeserializer());
        objectMapper.registerModule(module);
        return objectMapper;
    }

    private static class DeltaSnapshotDeserializer extends StdDeserializer<DeltaLogSnapshot> {
        public DeltaSnapshotDeserializer() {
            this(null);
        }

        public DeltaSnapshotDeserializer(Class<?> vc) {
            super(vc);
        }

        @Override
        public DeltaLogSnapshot deserialize(JsonParser jp, DeserializationContext ctxt) throws IOException {
            final JsonNode fullCommitInfoJson = jp.getCodec().readTree(jp);
            final JsonNode commitInfo = fullCommitInfoJson.get(DELTA_FIELD_COMMIT_INFO);
            logger.debug("CommitInfoJson is {}", commitInfo);
            final Function<JsonNode, Long> asLong = JsonNode::asLong;
            final String operationType = get(commitInfo, "UNKNOWN", JsonNode::asText, OP);

            final long numFiles = get(commitInfo, 0L, asLong, OP_METRICS, OP_NUM_FILES);
            final long numAddedFiles = get(commitInfo, 0L, asLong, OP_METRICS, OP_NUM_ADDED_FILES);
            final long numRemovedFiles = get(commitInfo, 0L, asLong, OP_METRICS, OP_NUM_REMOVED_FILES);
            final long netFilesAdded = Math.max(numFiles, numAddedFiles) - numRemovedFiles;

            final long netOutputRows = get(commitInfo, 0L, asLong, OP_METRICS, OP_NUM_OUTPUT_ROWS);

            final long numOutputBytes = get(commitInfo, 0L, asLong, OP_METRICS, OP_NUM_OUTPUT_BYTES);
            final long numAddedBytes = get(commitInfo, 0L, asLong, OP_METRICS, OP_NUM_ADDED_BYTES);
            final long numRemovedBytes = get(commitInfo, 0L, asLong, OP_METRICS, OP_NUM_REMOVED_BYTES);
            final long netBytesAdded = Math.max(numOutputBytes, numAddedBytes) - numRemovedBytes;

            final long timestamp = get(commitInfo, 0L, asLong, DELTA_TIMESTAMP);
            final long version = get(commitInfo, -1L, asLong, DELTA_READ_VERSION);
            return new DeltaLogSnapshot(operationType, netFilesAdded, netBytesAdded, netOutputRows, timestamp, version, false);
        }
    }

    private static <T> T get(JsonNode node, T defaultVal, Function<JsonNode, T> typeFunc, String... paths) {
        for (String path : paths) {
            if (node==null) {
                return defaultVal;
            }
            node = node.get(path);
        }
        return (node==null) ? defaultVal:typeFunc.apply(node);
    }
}