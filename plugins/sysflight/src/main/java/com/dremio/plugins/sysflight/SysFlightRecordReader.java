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
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import org.apache.arrow.flight.FlightClient;
import org.apache.arrow.flight.FlightRuntimeException;
import org.apache.arrow.flight.FlightStream;
import org.apache.arrow.flight.Ticket;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Field;

import com.dremio.common.AutoCloseables;
import com.dremio.common.exceptions.ExecutionSetupException;
import com.dremio.common.exceptions.UserException;
import com.dremio.common.expression.SchemaPath;
import com.dremio.exec.expr.TypeHelper;
import com.dremio.exec.record.BatchSchema;
import com.dremio.exec.store.AbstractRecordReader;
import com.dremio.sabot.exec.context.OperatorContext;
import com.dremio.sabot.op.scan.OutputMutator;
import com.dremio.service.flight.FlightRpcUtils;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;

import io.grpc.Context;

/**
 * RecordReader for SysFlight
 */
public class SysFlightRecordReader extends AbstractRecordReader {
  private static final org.slf4j.Logger LOGGER = org.slf4j.LoggerFactory.getLogger(SysFlightRecordReader.class);

  private final BatchSchema schema;
  private final FlightClient client;
  private final Ticket ticket;
  private final int targetBatchSize;

  private List<Field> selectedFields;
  private ValueVector[] valueVectors;

  private Context.CancellableContext cancellableContext;
  private FlightStream stream;
  private VectorSchemaRoot batchHolder;

  private boolean batchHolderIsEmpty = true;
  private int ptrToNextRecord = 0;

  public SysFlightRecordReader(OperatorContext context, List<SchemaPath> columns,
    BatchSchema schema, FlightClient client, Ticket ticket) {
    super(context, columns);
    this.schema = schema;
    this.client = client;
    this.ticket = ticket;
    targetBatchSize = context.getTargetBatchSize();
    LOGGER.debug("Target batch size is {}", targetBatchSize);
  }

  @Override
  public void setup(OutputMutator output) throws ExecutionSetupException {
    cancellableContext = Context.current().fork().withCancellation();

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

    cancellableContext.run(() -> {
      try{
        stream = client.getStream(ticket);
      } catch (FlightRuntimeException fre) {
        Optional<UserException> ue = FlightRpcUtils.fromFlightRuntimeException(fre);
        throw ue.isPresent() ? ue.get() : fre;
      } catch (Exception e) {
        Throwables.throwIfUnchecked(e);
        throw new RuntimeException(e);
      }
    });
  }

  @Override
  public int next() {
    int toRead = targetBatchSize;

    while(toRead > 0){
      try {
        if (batchHolderIsEmpty) {
          if (stream.next()) {
            batchHolder = stream.getRoot();
            batchHolderIsEmpty = false;
            ptrToNextRecord = 0;
          } else {
            return targetBatchSize - toRead;
          }
        }

        int currBatchSize = batchHolder.getRowCount();

        int i = ptrToNextRecord;
        int k = toRead;
        int j = 0;
        for (Field f: selectedFields) {
          i = ptrToNextRecord;
          k = toRead;
          for (; i < currBatchSize && k > 0; i++, k--) {
            ValueVector dataVector = batchHolder.getVector(f);
            if (dataVector != null) {
              valueVectors[j].copyFromSafe(i, targetBatchSize - k, dataVector);
            }
          }
          j++;
        }
        ptrToNextRecord = i;
        toRead = k;

        if (ptrToNextRecord == currBatchSize) {
          batchHolder.close();
          batchHolderIsEmpty = true;
        }
      } catch (FlightRuntimeException fre) {
        Optional<UserException> ue = FlightRpcUtils.fromFlightRuntimeException(fre);
        throw ue.isPresent() ? ue.get() : fre;
      } catch (Exception e) {
        Throwables.throwIfUnchecked(e);
        throw new RuntimeException(e);
      }
    }

    return targetBatchSize - toRead;
  }

  @Override
  public void close() throws Exception {
    AutoCloseables.close(stream, batchHolder, AutoCloseables.all(Arrays.asList(valueVectors)));
    if (cancellableContext != null) {
      cancellableContext.close();
    }
    cancellableContext = null;
  }
}
