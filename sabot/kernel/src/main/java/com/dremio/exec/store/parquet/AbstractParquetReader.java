/*
 * Copyright (C) 2017-2018 Dremio Corporation
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
package com.dremio.exec.store.parquet;

import java.util.List;

import org.apache.arrow.vector.SimpleIntVector;

import com.dremio.common.expression.SchemaPath;
import com.dremio.exec.store.AbstractRecordReader;
import com.dremio.sabot.exec.context.OperatorContext;

/**
 * Extension of AbstractRecordReader to keep track of deltas that may be needed to
 * pass from one recordreader to another to do proper filtering
 */
public abstract class AbstractParquetReader extends AbstractRecordReader {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(AbstractParquetReader.class);

  protected SimpleIntVector deltas;

  public AbstractParquetReader(final OperatorContext context, final List<SchemaPath> columns,
                               SimpleIntVector deltas) {
    super(context, columns);
    this.deltas = deltas;
  }
}
