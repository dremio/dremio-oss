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
package com.dremio.exec.store.metadatarefresh.footerread;

import com.dremio.exec.ExecConstants;
import com.dremio.exec.record.BatchSchema;
import com.dremio.sabot.exec.context.OperatorContext;
import com.google.common.base.Preconditions;
import java.io.IOException;
import org.apache.avro.file.DataFileConstants;

/**
 * In AVRO file there is no meta information which tell number of record counts in a file so we do
 * estimate calucation based of size.
 */
public class AvroRowCountEstimater implements FooterReader {

  private final int compressionFactor;
  private final long estimatedRecordSize;
  private final BatchSchema tableSchema;

  public AvroRowCountEstimater(BatchSchema tableSchema, OperatorContext context) {
    Preconditions.checkArgument(tableSchema != null, "Unexpected state");
    estimatedRecordSize =
        tableSchema.estimateRecordSize(
            (int) context.getOptions().getOption(ExecConstants.BATCH_LIST_SIZE_ESTIMATE),
            (int) context.getOptions().getOption(ExecConstants.BATCH_VARIABLE_FIELD_SIZE_ESTIMATE));
    this.tableSchema = tableSchema;
    this.compressionFactor = 10;
  }

  @Override
  public Footer getFooter(String path, long fileSize) throws IOException {
    if (fileSize < DataFileConstants.MAGIC.length) {
      throw new IOException(
          "Not a valid AVRO Data file " + path + " File Size is less than avro Magic bytes");
    }
    long uncompressedFileSize = fileSize * compressionFactor;
    long estimatedTotalRecords =
        (long) Math.ceil((double) uncompressedFileSize / estimatedRecordSize);
    return new AvroFooter(tableSchema, estimatedTotalRecords);
  }
}
