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
package com.dremio.exec.store.hive;

import com.dremio.exec.ExecConstants;
import com.dremio.exec.record.BatchSchema;
import com.dremio.exec.store.metadatarefresh.footerread.FooterReader;
import com.dremio.exec.store.metadatarefresh.footerread.ParquetFooterReader;
import com.dremio.io.file.FileSystem;
import com.dremio.sabot.exec.context.OperatorContext;
import com.google.common.base.Preconditions;

/**
 * Hive parquet footer reader that sets estimated record size, compression factor
 */
public class HiveParquetFooterReader extends ParquetFooterReader {
    public HiveParquetFooterReader(OperatorContext opContext, BatchSchema tableSchema, FileSystem fs,
                                   long estimatedRecordSize, double compressionFactor, boolean readFooter) {
        super(opContext, tableSchema, fs, estimatedRecordSize, compressionFactor, readFooter);
    }

    public static FooterReader getInstance(OperatorContext context, BatchSchema tableSchema, FileSystem fs) {
        Preconditions.checkArgument(tableSchema != null, "Unexpected state");
        final long estimatedRecordSize = tableSchema.estimateRecordSize((int) context.getOptions().getOption(ExecConstants.BATCH_LIST_SIZE_ESTIMATE),
                (int) context.getOptions().getOption(ExecConstants.BATCH_VARIABLE_FIELD_SIZE_ESTIMATE));
        final double compressionFactor = 30f;
        final boolean readFooter = HiveFooterReaderTableFunction.isAccuratePartitionStatsNeeded(context);
        return new HiveParquetFooterReader(context, tableSchema, fs,
                estimatedRecordSize, compressionFactor, readFooter);
    }
}
