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
package com.dremio.exec.store.hive.exec;

import java.io.IOException;

import org.apache.hadoop.mapred.FileSplit;

import com.dremio.exec.store.SplitAndPartitionInfo;
import com.dremio.exec.store.hive.HiveUtilities;
import com.dremio.hive.proto.HiveReaderProto;

/**
 * Pojo to hold required fields of hive parquet splits. Extracts fields from given {@link SplitAndPartitionInfo}
 */
public class HiveParquetSplit implements Comparable<HiveParquetSplit> {
    private final SplitAndPartitionInfo datasetSplit;
    private final FileSplit fileSplit;
    private final int partitionId;

    HiveParquetSplit(SplitAndPartitionInfo splitAndPartitionInfo) {
        this.datasetSplit = splitAndPartitionInfo;
        try {
            final HiveReaderProto.HiveSplitXattr splitAttr = HiveReaderProto.HiveSplitXattr.parseFrom(datasetSplit.getDatasetSplitInfo().getExtendedProperty());
            final FileSplit fullFileSplit = (FileSplit) HiveUtilities.deserializeInputSplit(splitAttr.getInputSplit());
            // make a copy of file split, we only need file path, start and length, throw away hosts
            this.fileSplit = new FileSplit(fullFileSplit.getPath(), fullFileSplit.getStart(), fullFileSplit.getLength(), (String[])null);
            this.partitionId = splitAttr.getPartitionId();
        } catch (IOException | ReflectiveOperationException e) {
            throw new RuntimeException("Failed to parse dataset split for " + datasetSplit.getPartitionInfo().getSplitKey(), e);
        }
    }

    public int getPartitionId() {
        return partitionId;
    }

    SplitAndPartitionInfo getDatasetSplit() {
        return datasetSplit;
    }

    FileSplit getFileSplit() {
        return fileSplit;
    }

    @Override
    public int compareTo(HiveParquetSplit other) {
        final int ret = fileSplit.getPath().compareTo(other.fileSplit.getPath());
        if (ret == 0) {
            return Long.compare(fileSplit.getStart(), other.getFileSplit().getStart());
        }
        return ret;
    }
}
