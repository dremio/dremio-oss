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
package com.dremio.plugins.sysflight;

import static org.apache.arrow.vector.types.Types.getMinorTypeForArrowType;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.arrow.flight.FlightClient;
import org.apache.arrow.flight.FlightStream;
import org.apache.arrow.flight.Ticket;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.util.TransferPair;

import com.dremio.common.exceptions.ExecutionSetupException;
import com.dremio.common.expression.SchemaPath;
import com.dremio.exec.expr.TypeHelper;
import com.dremio.exec.record.BatchSchema;
import com.dremio.exec.store.AbstractRecordReader;
import com.dremio.sabot.exec.context.OperatorContext;
import com.dremio.sabot.op.scan.OutputMutator;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;

/**
 * RecordReader for SysFlight
 */
public class SysFlightRecordReader extends AbstractRecordReader {
  private static final org.slf4j.Logger LOGGER = org.slf4j.LoggerFactory.getLogger(SysFlightRecordReader.class);

  private final BatchSchema schema;
  private final FlightClient client;
  private final Ticket ticket;

  private List<Field> selectedFields;
  private ValueVector[] valueVectors;
  private int recordsRead = -1;
  private int batchRecordCount = 0;

  public SysFlightRecordReader(OperatorContext context, List<SchemaPath> columns,
    BatchSchema schema, FlightClient client, Ticket ticket) {
    super(context, columns);
    this.schema = schema;
    this.client = client;
    this.ticket = ticket;
  }

  @Override
  public void setup(OutputMutator output) throws ExecutionSetupException {
    final Set<String> selectedColumns = new HashSet<>();
    if(this.getColumns() != null){
      for(SchemaPath path : getColumns()) {
        Preconditions.checkArgument(path.isSimplePath());
        selectedColumns.add(path.getAsUnescapedPath().toLowerCase());
      }
    }
    selectedFields = new ArrayList<>();

    for (Field field : schema.getFields()) {
      if(selectedColumns.contains(field.getName().toLowerCase())) {
        selectedFields.add(field);
      }
    }

    valueVectors = new ValueVector[selectedFields.size()];

    int i = 0;
    for (Field field : selectedFields) {
      final Class<? extends ValueVector> vvClass = (Class<? extends ValueVector>) TypeHelper.getValueVectorClass(getMinorTypeForArrowType(field.getType()));
      valueVectors[i] = output.addField(field, vvClass);
      i++;
    }
  }

  @Override
  public int next() {
    if (recordsRead >= batchRecordCount) {
      return 0;
    }

    // todo: handle multiple batches within a stream
    // todo: maintain recommended batchRecordCount to the scan output
    try (FlightStream stream = client.getStream(ticket)) {
      while (stream.next()) {
        try (final VectorSchemaRoot root = stream.getRoot()) {
          final int currentBatchCount = root.getRowCount();
          batchRecordCount = currentBatchCount;
          recordsRead = currentBatchCount;

          int j = 0;
          for (Field f : selectedFields) {
            ValueVector dataVector = root.getVector(f);
            TransferPair tp = dataVector.makeTransferPair(valueVectors[j]);
            tp.transfer();
            j++;
          }
          return currentBatchCount;
        }
      }

      return 0;
    } catch (Exception e) {
      Throwables.throwIfUnchecked(e);
      throw new RuntimeException(e);
    }
  }

  @Override
  public void close() throws Exception {
  }
}
