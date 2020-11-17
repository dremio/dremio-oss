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
package com.dremio.sabot.op.values;

import java.util.Collections;

import com.dremio.common.exceptions.ExecutionSetupException;
import com.dremio.common.expression.SchemaPath;
import com.dremio.exec.physical.config.EmptyValues;
import com.dremio.exec.store.AbstractRecordReader;
import com.dremio.exec.store.parquet.RecordReaderIterator;
import com.dremio.sabot.exec.context.OperatorContext;
import com.dremio.sabot.exec.fragment.FragmentExecutionContext;
import com.dremio.sabot.op.scan.OutputMutator;
import com.dremio.sabot.op.scan.ScanOperator;
import com.dremio.sabot.op.spi.ProducerOperator;

public class EmptyValuesCreator implements ProducerOperator.Creator<EmptyValues> {

  @Override
  public ProducerOperator create(FragmentExecutionContext fec, OperatorContext context, EmptyValues config) throws ExecutionSetupException {
    return new ScanOperator(config, context, RecordReaderIterator.from(new EmptyRecordReader(context)));
  }

  public static class EmptyRecordReader extends AbstractRecordReader {

    public EmptyRecordReader(OperatorContext context) {
      super(context, Collections.singletonList(SchemaPath.getSimplePath("*")));
    }

    @Override
    public void setup(OutputMutator output) throws ExecutionSetupException {
    }

    @Override
    public int next() {
      return 0;
    }

    @Override
    public void close() throws Exception {
    }

  }
}
