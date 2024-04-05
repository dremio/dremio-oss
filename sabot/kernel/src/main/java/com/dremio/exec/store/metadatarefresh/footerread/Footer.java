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

import com.dremio.exec.record.BatchSchema;
import org.apache.iceberg.FileFormat;

public class Footer {

  private final BatchSchema schema;
  private final long rowCount;
  private FileFormat fileFormat;

  public Footer(BatchSchema schema, long rowCount, FileFormat fileFormat) {
    this.schema = schema;
    this.rowCount = rowCount;
    this.fileFormat = fileFormat;
  }

  public BatchSchema getSchema() {
    return schema;
  }

  public long getRowCount() {
    return rowCount;
  }

  public FileFormat getFileFormat() {
    return fileFormat;
  }
}
