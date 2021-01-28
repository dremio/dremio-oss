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

import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import javax.annotation.concurrent.NotThreadSafe;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dremio.connector.metadata.BytesOutput;
import com.dremio.exec.store.dfs.FileSelection;
import com.dremio.io.file.FileAttributes;
import com.dremio.io.file.FileSystem;
import com.dremio.io.file.Path;
import com.dremio.sabot.exec.store.deltalake.proto.DeltaLakeProtobuf;
import com.dremio.sabot.exec.store.deltalake.proto.DeltaLakeProtobuf.DeltaLakeDatasetXAttr;
import com.dremio.service.namespace.file.proto.FileType;

/**
 * This class is responsible for orchestrating preparation of an overall snapshot by parsing oll Delta commit log files.
 */
@NotThreadSafe
public class DeltaLakeTable {
    private static final Logger logger = LoggerFactory.getLogger(DeltaLakeTable.class);

    private DeltaLogSnapshot deltaLogSnapshot = null;
    private final FileSystem fs;
    private final Path deltaLogDir;

    private long commitReadStartVersion = Long.MAX_VALUE;
    private long commitReadEndVersion = Long.MIN_VALUE;
    private long closestCheckpointVersion = -1L;
    private long closestLocalSnapshot = -1L;


    public DeltaLakeTable(FileSystem fs, FileSelection fileSelection) {
        this.fs = fs;
        final Path rootDir = Path.of(fileSelection.getSelectionRoot());
        this.deltaLogDir = rootDir.resolve(DeltaConstants.DELTA_LOG_DIR);
    }

    public DeltaLogSnapshot getConsolidatedSnapshot() throws IOException {
        loadSnapshotIfMissing();
        return deltaLogSnapshot;
    }

    private void loadSnapshotIfMissing() throws IOException {
        /**
         * NOTE: This function impl is done in order to facilitate E2E validations and is not the final version.
         * Final version will be covered as part of DX-26750
         */
        DeltaLogReader commitFileParser = DeltaLogReader.getInstance(FileType.JSON);
        deltaLogSnapshot = null;
        for (Path commitLogJson : getDeltaCommitFiles()) {
            // TODO parse commit files in parallel
            // TODO write logic to parse parquet, and start from checkpoint
            final DeltaLogSnapshot thisSnapshot = commitFileParser.parseMetadata(fs, commitLogJson);
            if (deltaLogSnapshot == null) {
                deltaLogSnapshot = thisSnapshot;
            } else {
                deltaLogSnapshot.merge(thisSnapshot);
            }
            commitReadStartVersion = Math.min(commitReadStartVersion, thisSnapshot.getVersionId());
            commitReadEndVersion = Math.max(commitReadEndVersion, thisSnapshot.getVersionId());
        }
    }

    private List<Path> getDeltaCommitFiles() throws IOException {
        // Makeshift code, which returns all files present in _delta_log folder
        final List<Path> logFiles = StreamSupport.stream(fs.list(deltaLogDir).spliterator(), false)
                .map(FileAttributes::getPath).sorted()
                .filter(p -> p.getName().endsWith(".json"))
                .collect(Collectors.toList());
        logger.debug("Identified log files {}", logFiles);
        return logFiles;
    }

    // Use the last read version as the read signature
    public BytesOutput readSignature() throws IOException {
        if (!fs.exists(deltaLogDir) || !fs.isDirectory(deltaLogDir)) {
            throw new IllegalStateException("missing _delta_log directory for the DeltaLake table");
        }

        return DeltaLakeProtobuf
                .DeltaLakeReadSignature
                .newBuilder()
                .setCommitReadEndVersion(this.commitReadEndVersion)
                .build()::writeTo;
    }

    public DeltaLakeDatasetXAttr buildDatasetXattr() {
        final DeltaLakeDatasetXAttr.Builder deltaXAttrBuilder = DeltaLakeDatasetXAttr.newBuilder();
        deltaXAttrBuilder.setClosestCheckpointVersion(this.closestCheckpointVersion);
        deltaXAttrBuilder.setClosestLocalSnapshot(this.closestLocalSnapshot);
        deltaXAttrBuilder.setCommitReadStartVersion(this.commitReadStartVersion);
        deltaXAttrBuilder.setCommitReadEndVersion(this.commitReadEndVersion);

        if (deltaLogSnapshot != null) {
            deltaXAttrBuilder.setNumFiles(this.deltaLogSnapshot.getNetFilesAdded());
            deltaXAttrBuilder.setSizeBytes(this.deltaLogSnapshot.getNetBytesAdded());
        }
        return deltaXAttrBuilder.build();
    }
}
