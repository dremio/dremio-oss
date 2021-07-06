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
import static com.dremio.exec.store.deltalake.DeltaConstants.DELTA_TIMESTAMP;
import static com.dremio.exec.store.deltalake.DeltaConstants.OP;
import static com.dremio.exec.store.deltalake.DeltaConstants.OP_METRICS;
import static com.dremio.exec.store.deltalake.DeltaConstants.OP_NUM_ADDED_BYTES;
import static com.dremio.exec.store.deltalake.DeltaConstants.OP_NUM_ADDED_FILES;
import static com.dremio.exec.store.deltalake.DeltaConstants.OP_NUM_DELETED_ROWS;
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
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;

import org.apache.arrow.util.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dremio.connector.metadata.DatasetSplit;
import com.dremio.exec.server.SabotContext;
import com.dremio.exec.store.file.proto.FileProtobuf;
import com.dremio.io.FSInputStream;
import com.dremio.io.file.FileAttributes;
import com.dremio.io.file.FileSystem;
import com.dremio.io.file.Path;
import com.dremio.sabot.exec.store.deltalake.proto.DeltaLakeProtobuf;
import com.dremio.sabot.exec.store.easy.proto.EasyProtobuf;
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
    public DeltaLogSnapshot parseMetadata(Path rootFolder, SabotContext context, FileSystem fs, FileAttributes fileAttributes, long version) throws IOException {
      final Path commitFilePath = fileAttributes.getPath();
        try (final FSInputStream commitFileIs = fs.open(commitFilePath);
             final BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(commitFileIs))) {
            /*
             * Order of entries in DeltaLake commit log file - commitInfo, protocol, metaData, remove, add
             * Apart from commitInfo, other sections are optional.
             */
            final DeltaLogSnapshot snapshot = OBJECT_MAPPER.readValue(bufferedReader.readLine(), DeltaLogSnapshot.class);

            String nextLine;
            boolean isOnlyMetadataChangeAction = false;
            while ((nextLine = bufferedReader.readLine())!=null) {
                final JsonNode json = OBJECT_MAPPER.readTree(nextLine);
                if (json.has(DELTA_FIELD_METADATA)) {
                    populateSchema(snapshot, json.get(DELTA_FIELD_METADATA));
                    isOnlyMetadataChangeAction = true;
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

            snapshot.setSplits(generateSplits(fileAttributes, snapshot.getDataFileEntryCount(), version));
            if (snapshot.isMissingRequiredValues()) {
              logger.debug("{} has missing required values", commitFilePath);
              // the snapshot is missing required values
              // this is either a schema change operation; or also contains add/remove records
              // if this is only a schema change operation, reset the missingRequiredValues flag on the snapshot
              if (isOnlyMetadataChangeAction) {
                logger.debug("{} is possibly a metadata only change", commitFilePath);
                // this could be a schema change only operation
                while ((nextLine = bufferedReader.readLine()) != null) {
                  final JsonNode jsonNode = OBJECT_MAPPER.readTree(nextLine);
                  if (jsonNode.has(DELTA_FIELD_ADD) || jsonNode.has(DELTA_FIELD_REMOVE)) {
                    logger.debug("{} contains added/removed files, not a metadata only change", commitFilePath);
                    isOnlyMetadataChangeAction = false;
                    break;
                  }
                }
              }

              if (isOnlyMetadataChangeAction) {
                snapshot.setMissingRequiredValues(false);
              } else {
                snapshot.finaliseMissingRequiredValues();
              }
            }

            logger.debug("For file {}, snaspshot is {}", commitFilePath, snapshot.toString());
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
            final AtomicBoolean missingRequiredValues = new AtomicBoolean(false);
            final JsonNode fullCommitInfoJson = jp.getCodec().readTree(jp);
            final JsonNode commitInfo = fullCommitInfoJson.get(DELTA_FIELD_COMMIT_INFO);
            logger.debug("CommitInfoJson is {}", commitInfo);
            final Function<JsonNode, Long> asLong = JsonNode::asLong;
            final String operationType = get(commitInfo, "UNKNOWN", JsonNode::asText, OP);

            final long numFiles = getRequired(commitInfo, missingRequiredValues, 0L, asLong, OP_METRICS, OP_NUM_FILES);
            final long numAddedFiles = get(commitInfo, 0L, asLong, OP_METRICS, OP_NUM_ADDED_FILES);
            final long numRemovedFiles = get(commitInfo, 0L, asLong, OP_METRICS, OP_NUM_REMOVED_FILES);
            final long netFilesAdded = Math.max(numFiles, numAddedFiles) - numRemovedFiles;
            final long totalFileEntries = Math.max(numFiles, numAddedFiles) + numRemovedFiles;
            long netOutputRows = getRequired(commitInfo, missingRequiredValues, 0L, asLong, OP_METRICS, OP_NUM_OUTPUT_ROWS);
            netOutputRows -= get(commitInfo, 0L, asLong, OP_METRICS, OP_NUM_DELETED_ROWS);

            final long numOutputBytes = get(commitInfo, 0L, asLong, OP_METRICS, OP_NUM_OUTPUT_BYTES);
            final long numAddedBytes = get(commitInfo, 0L, asLong, OP_METRICS, OP_NUM_ADDED_BYTES);
            final long numRemovedBytes = get(commitInfo, 0L, asLong, OP_METRICS, OP_NUM_REMOVED_BYTES);
            final long netBytesAdded = Math.max(numOutputBytes, numAddedBytes) - numRemovedBytes;

            final long timestamp = get(commitInfo, 0L, asLong, DELTA_TIMESTAMP);
            DeltaLogSnapshot result = new DeltaLogSnapshot(operationType, netFilesAdded, netBytesAdded, netOutputRows, totalFileEntries, timestamp, false);
            result.setMissingRequiredValues(missingRequiredValues.get());
            return result;
        }

        // get an required value
        private <T> T getRequired(JsonNode node, AtomicBoolean missingRequiredValues, T defaultVal, Function<JsonNode, T> typeFunc, String... paths) {
          node = findNode(node, paths);
          if (node == null) {
            missingRequiredValues.set(true);
          }
          return (node==null) ? defaultVal:typeFunc.apply(node);
        }
    }

    private static JsonNode findNode(JsonNode node, String... paths) {
      for (String path : paths) {
        if (node==null) {
          return node;
        }
        node = node.get(path);
      }
      return node;
    }

    // get an optional value
    private static <T> T get(JsonNode node, T defaultVal, Function<JsonNode, T> typeFunc, String... paths) {
      node = findNode(node, paths);
      return (node==null) ? defaultVal:typeFunc.apply(node);
    }

    private List<DatasetSplit> generateSplits(FileAttributes fileAttributes, long totalFileEntries, long version) {
      List<DatasetSplit> splits = new ArrayList<>(1);
      Preconditions.checkNotNull(fileAttributes, "File attributes are not set");

      DeltaLakeProtobuf.DeltaCommitLogSplitXAttr deltaExtended = DeltaLakeProtobuf.DeltaCommitLogSplitXAttr
        .newBuilder().setVersion(version).build();

      final EasyProtobuf.EasyDatasetSplitXAttr splitExtended = EasyProtobuf.EasyDatasetSplitXAttr.newBuilder()
        .setPath(fileAttributes.getPath().toString())
        .setStart(0)
        .setLength(fileAttributes.size())
        .setUpdateKey(FileProtobuf.FileSystemCachedEntity.newBuilder()
          .setPath(fileAttributes.getPath().toString())
          .setLength(fileAttributes.size())
          .setLastModificationTime(fileAttributes.lastModifiedTime().toMillis()))
        .setExtendedProperty(deltaExtended.toByteString())
        .build();

      splits.add(DatasetSplit.of(
        Collections.EMPTY_LIST, fileAttributes.size(), totalFileEntries, splitExtended::writeTo));
      return splits;
    }
}
