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
package org.apache.parquet.hadoop;

import java.io.IOException;

import org.apache.parquet.column.ParquetProperties;
import org.apache.parquet.column.page.PageWriteStore;
import org.apache.parquet.hadoop.CodecFactory.BytesCompressor;
import org.apache.parquet.schema.MessageType;

public class ColumnChunkPageWriteStoreExposer {

  public static ColumnChunkPageWriteStore newColumnChunkPageWriteStore(
      BytesCompressor compressor,
      MessageType schema,
      ParquetProperties parquetProperties
      ) {
    return new ColumnChunkPageWriteStore(compressor, schema, parquetProperties);
  }

  public static void flushPageStore(PageWriteStore pageStore, ParquetFileWriter w) throws IOException {
    ((ColumnChunkPageWriteStore) pageStore).flushToFileWriter(w);
  }

  // TODO(jaltekruse) - review, this used to have a method for closing a pageStore
  // the parquet code once rebased did not include this close method, make sure it isn't needed
  // I might have messed up the merge

}
