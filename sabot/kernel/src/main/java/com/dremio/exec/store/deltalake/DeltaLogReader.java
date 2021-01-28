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

import com.dremio.io.file.FileSystem;
import com.dremio.io.file.Path;
import com.dremio.service.namespace.file.proto.FileType;

/**
 * Reads one commit/checkpoint file for DeltaLake log files.
 */
public interface DeltaLogReader {

    static DeltaLogReader getInstance(FileType fileType) {
        switch (fileType) {
            case JSON:
                return new DeltaLogCommitJsonReader();
                //TODO Add DeltaLogParquetParser for checkpoint files
            default:
                throw new IllegalArgumentException("Commit file type is not supported " + fileType);
        }
    }

    /**
     * Parses the DeltaLake commit log file / checkpoint parquet
     *
     * @param fs
     * @param commitFilePath
     * @return
     */
    DeltaLogSnapshot parseMetadata(FileSystem fs, Path commitFilePath) throws IOException;
}
