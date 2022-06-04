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
package com.dremio.sabot.op.dictionary;

import static java.lang.String.format;
import static org.apache.arrow.vector.types.Types.getMinorTypeForArrowType;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.apache.arrow.vector.AllocationHelper;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.DateMilliVector;
import org.apache.arrow.vector.Float4Vector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.TimeStampMilliVector;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.util.TransferPair;
import org.joda.time.DateTimeConstants;

import com.dremio.common.AutoCloseables;
import com.dremio.common.exceptions.ExecutionSetupException;
import com.dremio.common.expression.SchemaPath;
import com.dremio.common.util.MajorTypeHelper;
import com.dremio.exec.catalog.StoragePluginId;
import com.dremio.exec.expr.TypeHelper;
import com.dremio.exec.physical.config.DictionaryLookupPOP;
import com.dremio.exec.record.TypedFieldId;
import com.dremio.exec.record.VectorAccessible;
import com.dremio.exec.record.VectorContainer;
import com.dremio.exec.record.selection.SelectionVector2;
import com.dremio.exec.store.StoragePlugin;
import com.dremio.exec.store.dfs.FileSystemPlugin;
import com.dremio.exec.store.parquet.ParquetFormatPlugin;
import com.dremio.io.file.FileSystem;
import com.dremio.io.file.Path;
import com.dremio.sabot.exec.context.OperatorContext;
import com.dremio.sabot.op.spi.SingleInputOperator;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

/**
 * Dictionary Lookup operator.
 */
public class DictionaryLookupOperator implements SingleInputOperator {
  private final DictionaryLookupPOP config;
  private final OperatorContext context;
  private VectorContainer outgoing;
  private State state = State.NEEDS_SETUP;
  private VectorAccessible incoming;
  private Map<String, ValueVector> allocationVectors;
  private List<TransferPair> transferPairs;
  private final Map<String, VectorContainer> dictionaries = Maps.newHashMap();
  private int recordsConsumedCurrentBatch;
  private Map<String, ValueVector> dictionaryIdIncomingVectors;
  private boolean hasSv2 = false;

  public DictionaryLookupOperator(final OperatorContext operatorContext, final DictionaryLookupPOP config) {
    this.config = config;
    this.context = operatorContext;
  }

  @Override
  public State getState() {
    return state;
  }

  @Override
  public VectorAccessible setup(VectorAccessible incoming) throws Exception {
    this.incoming = incoming;
    switch (incoming.getSchema().getSelectionVectorMode()) {
      case NONE:
        this.outgoing = context.createOutputVectorContainer();
        break;

      case TWO_BYTE:
        this.hasSv2 = true;
        this.outgoing = context.createOutputVectorContainerWithSV(incoming.getSelectionVector2());
        break;

      case FOUR_BYTE:
        throw new UnsupportedOperationException("SV4 not supported by dictionary lookup operator");
    }

    this.transferPairs = Lists.newArrayList();
    this.allocationVectors = Maps.newHashMap();
    this.dictionaryIdIncomingVectors = Maps.newHashMap();
    for (Field field : incoming.getSchema().getFields()) {
      // transfer vectors for this field from incoming to outgoing
      final TypedFieldId typedFieldId = incoming.getValueVectorId(SchemaPath.getSimplePath(field.getName()));
      final ValueVector vvIn = incoming.getValueAccessorById(TypeHelper.getValueVectorClass(field), typedFieldId.getFieldIds()).getValueVector();

      if (config.getDictionaryEncodedFields().containsKey(field.getName())) {
        final ValueVector vvOut = outgoing.addOrGet(new Field(field.getName(), new FieldType(field.isNullable(),
          config.getDictionaryEncodedFields().get(field.getName()).getArrowType(), field.getDictionary()), field.getChildren()));
        // load dictionary
        if (!dictionaries.containsKey(field.getName())) {
          final VectorContainer dictionary = loadDictionary(field.getName());
          dictionaries.put(field.getName(), dictionary);
        }
        // setup lookup/copy optimize for sv2 and sv4?
        allocationVectors.put(field.getName(), vvOut);
        dictionaryIdIncomingVectors.put(field.getName(), vvIn);
      } else {
        // transfer vectors for this field from incoming to outgoing
        final ValueVector vvOut = outgoing.addOrGet(field);
        final TransferPair tp = vvIn.makeTransferPair(vvOut);
        transferPairs.add(tp);
      }
    }
    outgoing.buildSchema(incoming.getSchema().getSelectionVectorMode());
    outgoing.setInitialCapacity(context.getTargetBatchSize());
    state = State.CAN_CONSUME;
    return outgoing;
  }

  private void allocateNew() {
    // Allocate vv in the allocationVectors.
    for (final ValueVector v : this.allocationVectors.values()) {
      AllocationHelper.allocateNew(v, incoming.getRecordCount());
    }
  }

  public VectorContainer loadDictionary(String fieldName) throws IOException, ExecutionSetupException {
    final StoragePluginId id = config.getDictionaryEncodedFields().get(fieldName).getStoragePluginId();
    final StoragePlugin storagePlugin = config.getStoragePluginResolver().getSource(id);
    if (storagePlugin instanceof FileSystemPlugin) {
      final FileSystemPlugin<?> fsPlugin = (FileSystemPlugin<?>) storagePlugin;
      final FileSystem fs = fsPlugin.createFS(config.getProps().getUserName(), context);
      return ParquetFormatPlugin.loadDictionary(fs, Path.of(config.getDictionaryEncodedFields().get(fieldName).getDictionaryPath()), context.getAllocator());
    } else {
      throw new ExecutionSetupException(format("Storage plugin %s is not a filesystem plugin", id.getName()));
    }
  }

  private void decodeInt(IntVector input, IntVector output, IntVector dictionary) {
    if (hasSv2) {
      final SelectionVector2 sv2 = incoming.getSelectionVector2();
      for (int i = 0, svIndex = sv2.getIndex(i); i < recordsConsumedCurrentBatch; ++i) {
        final int id = input.get(svIndex);
        output.copyFromSafe(id, svIndex, dictionary);
      }
    } else {
      for (int i = 0; i < recordsConsumedCurrentBatch; ++i) {
        int id = input.get(i);
        output.copyFromSafe(id, i, dictionary);
      }
    }
  }

  private void decodeBigInt(IntVector input, BigIntVector output, BigIntVector dictionary) {
    if (hasSv2) {
      final SelectionVector2 sv2 = incoming.getSelectionVector2();
      for (int i = 0; i < recordsConsumedCurrentBatch; ++i) {
        final int svIndex = sv2.getIndex(i);
        final int id = input.get(svIndex);
        output.copyFromSafe(id, svIndex, dictionary);
      }
    } else {
      for (int i = 0; i < recordsConsumedCurrentBatch; ++i) {
        final int id = input.get(i);
        output.copyFromSafe(id, i, dictionary);
      }
    }
  }

  private void decodeFloat(IntVector input, Float4Vector output, Float4Vector dictionary) {
    if (hasSv2) {
      final SelectionVector2 sv2 = incoming.getSelectionVector2();
      for (int i = 0; i < recordsConsumedCurrentBatch; ++i) {
        final int svIndex = sv2.getIndex(i);
        final int id = input.get(svIndex);
        output.copyFromSafe(id, svIndex, dictionary);
      }
    } else {
      for (int i = 0; i < recordsConsumedCurrentBatch; ++i) {
        final int id = input.get(i);
        output.copyFromSafe(id, i, dictionary);
      }
    }
  }

  private void decodeDouble(IntVector input, Float8Vector output, Float8Vector dictionary) {
    if (hasSv2) {
      final SelectionVector2 sv2 = incoming.getSelectionVector2();
      for (int i = 0; i < recordsConsumedCurrentBatch; ++i) {
        final int svIndex = sv2.getIndex(i);
        final int id = input.get(svIndex);
        output.copyFromSafe(id, svIndex, dictionary);
      }
    } else {
      for (int i = 0; i < recordsConsumedCurrentBatch; ++i) {
        final int id = input.get(i);
        output.copyFromSafe(id, i, dictionary);
      }
    }
  }

  private void decodeBinary(IntVector input, VarBinaryVector output, VarBinaryVector dictionary) {
    if (hasSv2) {
      final SelectionVector2 sv2 = incoming.getSelectionVector2();
      for (int i = 0; i < recordsConsumedCurrentBatch; ++i) {
        final int svIndex = sv2.getIndex(i);
        final int id = input.get(svIndex);
        output.copyFromSafe(id, svIndex, dictionary);
      }
    } else {
      for (int i = 0; i < recordsConsumedCurrentBatch; ++i) {
        final int id = input.get(i);
        output.copyFromSafe(id, i, dictionary);
      }
    }
  }

  private void decodeVarChar(IntVector input, VarCharVector output, VarBinaryVector dictionary) {
    if (hasSv2) {
      final SelectionVector2 sv2 = incoming.getSelectionVector2();
      for (int i = 0; i < recordsConsumedCurrentBatch; ++i) {
        final int svIndex = sv2.getIndex(i);
        // TODO : Implement CopySafe between varchar and varbinary vectors.
        if (!input.isNull(svIndex)) {
          output.setNull(svIndex);
        } else {
          final int id = input.get(svIndex);
          final byte[] value = dictionary.get(id);
          output.setSafe(svIndex, value, 0, value.length);
        }
      }
    } else {
      for (int i = 0; i < recordsConsumedCurrentBatch; ++i) {
        if (input.isNull(i)) {
          output.setNull(i);
        } else {
          final int id = input.get(i);
          final byte[] value = dictionary.get(id);
          output.setSafe(i, value, 0, value.length);
        }
      }
    }
  }

  private void decodeBoolean(IntVector input, BitVector output) {
    if (hasSv2) {
      final SelectionVector2 sv2 = incoming.getSelectionVector2();
      for (int i = 0; i < recordsConsumedCurrentBatch; ++i) {
        final int svIndex = sv2.getIndex(i);
        if (input.isNull(svIndex)) {
          output.setNull(svIndex);
        } else {
          final int id = input.get(svIndex);
          output.setSafe(svIndex, id);
        }
      }
    } else {
      for (int i = 0; i < recordsConsumedCurrentBatch; ++i) {
        if (input.isNull(i)) {
          output.setNull(i);
        } else {
          final int id = input.get(i);
          output.setSafe(i, id);
        }
      }
    }
  }

  private void decodeDate(IntVector input, DateMilliVector output, IntVector dictionary) {
    // dates are stored as int32 in parquet dictionaries
    if (hasSv2) {
      final SelectionVector2 sv2 = incoming.getSelectionVector2();
      for (int i = 0; i < recordsConsumedCurrentBatch; ++i) {
        final int svIndex = sv2.getIndex(i);
        if (input.isNull(svIndex)) {
          output.setNull(svIndex);
        } else {
          final int id = input.get(svIndex);
          output.setSafe(svIndex, dictionary.get(id) * (long) DateTimeConstants.MILLIS_PER_DAY);
        }
      }
    } else {
      for (int i = 0; i < recordsConsumedCurrentBatch; ++i) {
        if (input.isNull(i)) {
          output.setNull(i);
        } else {
          int id = input.get(i);
          output.setSafe(i, dictionary.get(id) * (long) DateTimeConstants.MILLIS_PER_DAY);
        }
      }
    }
  }

  private void decodeTimestamp(IntVector input, TimeStampMilliVector output, BigIntVector dictionary) {
    // dates are stored as int32 in parquet dictionaries
    if (hasSv2) {
      final SelectionVector2 sv2 = incoming.getSelectionVector2();
      for (int i = 0; i < recordsConsumedCurrentBatch; ++i) {
        final int svIndex = sv2.getIndex(i);
        if (input.isNull(svIndex)) {
          output.setNull(svIndex);
        } else {
          final int id = input.get(svIndex);
          output.setSafe(svIndex, dictionary.get(id));
        }
      }
    } else {
      for (int i = 0; i < recordsConsumedCurrentBatch; ++i) {
        if (input.isNull(i)) {
          output.setNull(i);
        } else {
          int id = input.get(i);
          output.setSafe(i, dictionary.get(id));
        }
      }
    }
  }

  @Override
  public int outputData() throws Exception {
    state.is(State.CAN_PRODUCE);
    allocateNew();

    // transfer data for columns which are not to be decoded here.
    for (TransferPair tp : transferPairs) {
      tp.transfer();
    }

    for (Map.Entry<String, ValueVector> entry : dictionaryIdIncomingVectors.entrySet()) {
      final String fieldName = entry.getKey();
      final ArrowType outputType = config.getDictionaryEncodedFields().get(fieldName).getArrowType();
      switch (MajorTypeHelper.getMinorTypeFromArrowMinorType(getMinorTypeForArrowType(outputType))) {
        case INT: {
          final IntVector input = (IntVector) entry.getValue();
          final IntVector output = (IntVector) allocationVectors.get(fieldName);
          final IntVector dictionary = dictionaries.get(fieldName).getValueAccessorById(IntVector.class, 0).getValueVector();
          decodeInt(input, output, dictionary);
          output.setValueCount(recordsConsumedCurrentBatch);
        }
        break;

        case BIGINT: {
          final IntVector input = (IntVector) entry.getValue();
          final BigIntVector output = (BigIntVector) allocationVectors.get(fieldName);
          final BigIntVector dictionary = dictionaries.get(fieldName).getValueAccessorById(BigIntVector.class, 0).getValueVector();
          decodeBigInt(input, output, dictionary);
          output.setValueCount(recordsConsumedCurrentBatch);
        }
        break;

        case VARBINARY:
        case FIXEDSIZEBINARY: {
          final IntVector input = (IntVector) entry.getValue();
          final VarBinaryVector output = (VarBinaryVector) allocationVectors.get(fieldName);
          final VarBinaryVector dictionary = dictionaries.get(fieldName).getValueAccessorById(VarBinaryVector.class, 0).getValueVector();
          decodeBinary(input, output, dictionary);
          output.setValueCount(recordsConsumedCurrentBatch);
        }
        break;

        case VARCHAR:
        case VAR16CHAR: {
          final IntVector input = (IntVector) entry.getValue();
          final VarCharVector output = (VarCharVector) allocationVectors.get(fieldName);
          final VarBinaryVector dictionary = dictionaries.get(fieldName).getValueAccessorById(VarBinaryVector.class, 0).getValueVector();
          decodeVarChar(input, output, dictionary);
          output.setValueCount(recordsConsumedCurrentBatch);
        }
        break;

        case FLOAT4: {
          final IntVector input = (IntVector) entry.getValue();
          final Float4Vector output = (Float4Vector) allocationVectors.get(fieldName);
          final Float4Vector dictionary = dictionaries.get(fieldName).getValueAccessorById(Float4Vector.class, 0).getValueVector();
          decodeFloat(input, output, dictionary);
          output.setValueCount(recordsConsumedCurrentBatch);
        }
        break;

        case FLOAT8: {
          final IntVector input = (IntVector) entry.getValue();
          final Float8Vector output = (Float8Vector) allocationVectors.get(fieldName);
          final Float8Vector dictionary = dictionaries.get(fieldName).getValueAccessorById(Float8Vector.class, 0).getValueVector();
          decodeDouble(input, output, dictionary);
          output.setValueCount(recordsConsumedCurrentBatch);
        }
        break;

        case BIT: {
          final IntVector input = (IntVector) entry.getValue();
          final BitVector output = (BitVector) allocationVectors.get(fieldName);
          decodeBoolean(input, output);
          output.setValueCount(recordsConsumedCurrentBatch);
        }
        break;

        case DATE: {
          final IntVector input = (IntVector) entry.getValue();
          final DateMilliVector output = (DateMilliVector) allocationVectors.get(fieldName);
          final IntVector dictionary = dictionaries.get(fieldName).getValueAccessorById(IntVector.class, 0).getValueVector();
          decodeDate(input, output, dictionary);
          output.setValueCount(recordsConsumedCurrentBatch);
        }
        break;

        case TIMESTAMP: {
          final IntVector input = (IntVector) entry.getValue();
          final TimeStampMilliVector output = (TimeStampMilliVector) allocationVectors.get(fieldName);
          final BigIntVector dictionary = dictionaries.get(fieldName).getValueAccessorById(BigIntVector.class, 0).getValueVector();
          decodeTimestamp(input, output, dictionary);
          output.setValueCount(recordsConsumedCurrentBatch);
        }
        break;

        default:
          break;
      }
    }
    if (hasSv2) { // Since incoming schema shouldn't change this is a safe assumption
      // copy sv2 of incoming batch
      outgoing.getSelectionVector2().referTo(incoming.getSelectionVector2());
    }
    state = State.CAN_CONSUME;
    return outgoing.setAllCount(recordsConsumedCurrentBatch);
  }

  @Override
  public void noMoreToConsume() throws Exception {
    state.is(State.CAN_CONSUME);
    state = State.DONE;
  }

  @Override
  public void consumeData(int records) throws Exception {
    state.is(State.CAN_CONSUME);
    recordsConsumedCurrentBatch = records;
    state = State.CAN_PRODUCE;
  }

  @Override
  public <OUT, IN, EXCEP extends Throwable> OUT accept(OperatorVisitor<OUT, IN, EXCEP> visitor, IN value) throws EXCEP {
    return visitor.visitSingleInput(this, value);
  }

  @Override
  public void close() throws Exception {
    try {
      AutoCloseables.close(outgoing, dictionaries.values());
    } finally {
      dictionaries.clear();
    }
  }

  public static class DictionaryLookupCreator implements SingleInputOperator.Creator<DictionaryLookupPOP>{

    @Override
    public SingleInputOperator create(OperatorContext context, DictionaryLookupPOP operator) throws ExecutionSetupException {
      return new DictionaryLookupOperator(context, operator);
    }
  }
}
