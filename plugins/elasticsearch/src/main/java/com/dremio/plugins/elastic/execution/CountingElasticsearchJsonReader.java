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
package com.dremio.plugins.elastic.execution;

import java.io.IOException;
import java.util.List;

import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.vector.complex.writer.BaseWriter.ComplexWriter;
import org.apache.calcite.util.Pair;

import com.dremio.common.expression.SchemaPath;

/**
 * Overrides {@link ElasticsearchJsonReader} to just return count instead of parsing the whole response.
 */
public class CountingElasticsearchJsonReader extends ElasticsearchJsonReader {

  private long recordCount;

  public CountingElasticsearchJsonReader(ArrowBuf managedBuf, List<SchemaPath> columns, String resourceName,
      FieldReadDefinition rootDefinition, boolean fieldsProjected, boolean metaUIDSelected, boolean metaIDSelected,
      boolean metaTypeSelected, boolean metaIndexSelected) {
    super(managedBuf, columns, resourceName, rootDefinition, fieldsProjected, metaUIDSelected, metaIDSelected,
        metaTypeSelected, metaIndexSelected);
  }

  @Override
  public Pair<String, Long> getScrollAndTotalSizeThenSeekToHits() throws IOException {
    Pair<String, Long> scrollIdAndRecordCount = super.getScrollAndTotalSizeThenSeekToHits();
    recordCount = scrollIdAndRecordCount.getValue();
    return scrollIdAndRecordCount;
  }

  @Override
  public void ensureAtLeastOneField(ComplexWriter writer) {
    // no-op
  }

  @Override
  public ReadState write(ComplexWriter writer) throws IOException {
    if (recordCount > 0) {
      recordCount--;
      return ReadState.WRITE_SUCCEED;
    }

    return ReadState.END_OF_STREAM;
  }
}
