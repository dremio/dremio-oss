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
package com.dremio.sabot.op.fromjson;

import java.io.EOFException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.complex.writer.BaseWriter.ComplexWriter;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.util.TransferPair;

import com.dremio.common.AutoCloseables;
import com.dremio.common.exceptions.ExecutionSetupException;
import com.dremio.common.exceptions.FieldSizeLimitExceptionHelper;
import com.dremio.common.exceptions.UserException;
import com.dremio.common.expression.Describer;
import com.dremio.common.expression.SchemaPath;
import com.dremio.exec.ExecConstants;
import com.dremio.exec.exception.JsonFieldChangeExceptionContext;
import com.dremio.exec.exception.SetupException;
import com.dremio.exec.record.TypedFieldId;
import com.dremio.exec.record.VectorAccessible;
import com.dremio.exec.record.VectorAccessibleComplexWriter;
import com.dremio.exec.record.VectorContainer;
import com.dremio.exec.record.VectorWrapper;
import com.dremio.exec.store.easy.json.JsonProcessor.ReadState;
import com.dremio.exec.vector.complex.fn.JsonReader;
import com.dremio.sabot.exec.context.OperatorContext;
import com.dremio.sabot.op.fromjson.ConvertFromJsonPOP.ConversionColumn;
import com.dremio.sabot.op.spi.SingleInputOperator;
import com.google.common.base.Preconditions;

public class ConvertFromJsonOperator implements SingleInputOperator {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(ConvertFromJsonOperator.class);

  private final ConvertFromJsonPOP config;
  private final OperatorContext context;

  private State state = State.NEEDS_SETUP;
  private VectorAccessible incoming;
  private VectorContainer outgoing;
  private List<TransferPair> transfers = new ArrayList<>();
  private List<JsonConverter<?>> converters = new ArrayList<>();

  public ConvertFromJsonOperator(OperatorContext context, ConvertFromJsonPOP config) {
    this.config = config;
    this.context = context;
  }

  @Override
  public VectorAccessible setup(VectorAccessible accessible) throws Exception {
    state.is(State.NEEDS_SETUP);
    this.incoming = accessible;
    this.outgoing = context.createOutputVectorContainer();
    outgoing.setInitialCapacity(context.getTargetBatchSize());

    final Map<String, ConversionColumn> cMap = new HashMap<>();
    for(ConversionColumn c : config.getColumns()){
      cMap.put(c.getInputField().toLowerCase(), c);
    }

    final int sizeLimit = Math.toIntExact(this.context.getOptions().getOption(ExecConstants.LIMIT_FIELD_SIZE_BYTES));

    for(VectorWrapper<?> w: accessible){
      final Field f = w.getField();
      final ValueVector incomingVector = w.getValueVector();
      ConversionColumn conversion = cMap.get(f.getName().toLowerCase());
      if(conversion != null){
        Field updatedField = conversion.asField(f.getName());
        ValueVector outgoingVector = outgoing.addOrGet(updatedField);
        Preconditions.checkArgument(incomingVector instanceof VarBinaryVector || incomingVector instanceof VarCharVector, "Incoming field [%s] should have been either a varchar or varbinary.", Describer.describe(f));
        if(incomingVector instanceof VarBinaryVector){
          converters.add(new BinaryConverter(conversion, sizeLimit, (VarBinaryVector) incomingVector, outgoingVector));
        } else {
          converters.add(new CharConverter(conversion, sizeLimit, (VarCharVector) incomingVector, outgoingVector));
        }

      } else {
        TransferPair pair = incomingVector.getTransferPair(context.getAllocator());
        transfers.add(pair);
        outgoing.add(pair.getTo());
      }
    }

    if (converters.size() != config.getColumns().size()) {
      throw new SetupException(String.format("Expected %d input column(s) but only found %d", config.getColumns().size(),
        converters.size()));
    }

    outgoing.buildSchema();
    state = State.CAN_CONSUME;
    return outgoing;
  }

  @Override
  public void consumeData(int records) throws Exception {
    state.is(State.CAN_CONSUME);
    state = State.CAN_PRODUCE;
  }

  @Override
  public int outputData() throws Exception {
    state.is(State.CAN_PRODUCE);

    final int records = incoming.getRecordCount();
    for(JsonConverter<?> converter : converters){
      converter.convert(records);
    }

    for(TransferPair transfer : transfers){
      transfer.transfer();
      transfer.getTo().setValueCount(records);
    }

    outgoing.setRecordCount(records);
    state = State.CAN_CONSUME;

    return records;
  }

  @Override
  public <OUT, IN, EXCEP extends Throwable> OUT accept(OperatorVisitor<OUT, IN, EXCEP> visitor, IN value) throws EXCEP {
    return visitor.visitSingleInput(this, value);
  }

  @Override
  public void close() throws Exception {
    AutoCloseables.close(outgoing);
  }

  @Override
  public void noMoreToConsume() throws Exception {
    state.is(State.CAN_CONSUME);
    state = State.DONE;
  }

  @Override
  public State getState() {
    return state;
  }

  private class BinaryConverter extends JsonConverter<VarBinaryVector> {

    BinaryConverter(ConversionColumn column, int sizeLimit, VarBinaryVector vector, ValueVector outgoingVector) {
      super(column, sizeLimit, vector, outgoingVector);
    }

    @Override
    byte[] getBytes(int inputIndex) {
      if (vector.isNull(inputIndex)) {
        return null;
      }
      final byte[] data = vector.get(inputIndex);
      FieldSizeLimitExceptionHelper.checkReadSizeLimit(data.length, sizeLimit, inputIndex, logger);
      return data;
    }
  }

  private class CharConverter extends JsonConverter<VarCharVector> {

    CharConverter(ConversionColumn column, int sizeLimit, VarCharVector vector, ValueVector outgoingVector) {
      super(column, sizeLimit, vector, outgoingVector);
    }

    @Override
    byte[] getBytes(int inputIndex) {
      if (vector.isNull(inputIndex)) {
        return null;
      }

      final byte[] data = vector.get(inputIndex);
      FieldSizeLimitExceptionHelper.checkReadSizeLimit(data.length, sizeLimit, inputIndex, logger);
      return data;
    }
  }

  private abstract class JsonConverter<T extends ValueVector> {
    private final ConversionColumn column;
    private final ComplexWriter writer;
    private final JsonReader reader;
    protected final T vector;
    private final ValueVector outgoingVector;
    protected final int sizeLimit;

    public JsonConverter(ConversionColumn column, int sizeLimit, T vector, ValueVector outgoingVector) {
      this.column = column;
      this.vector = vector;
      this.writer = VectorAccessibleComplexWriter.getWriter(column.getInputField(), outgoing);
      this.reader = new JsonReader(context.getManagedBuffer(), sizeLimit, context.getOptions().getOption(ExecConstants.JSON_READER_ALL_TEXT_MODE_VALIDATOR), false, false);
      this.outgoingVector = outgoingVector;
      this.sizeLimit = sizeLimit;
    }

    abstract byte[] getBytes(int inputIndex);

    public void convert(final int records) {
      try {
        outgoingVector.allocateNew();
        for(int i = 0; i < records; i++){
          writer.setPosition(i);
          byte[] bytes = getBytes(i);
          if (bytes == null || bytes.length == 0) {
            continue;
          }
          reader.setSource(bytes);

          final ReadState state = reader.write(writer);
          if (state == ReadState.END_OF_STREAM) {
            throw new EOFException("Unexpected end of stream while parsing JSON data.");
          }
          writer.setValueCount(records);
        }

      } catch (Exception ex) {
        throw UserException.dataReadError(ex).message("Failure converting field %s from JSON.", column.getInputField())
            .build(logger);
      }

      if (outgoing.isNewSchema()) {
        // build the schema so we can get the "updated" schema
        outgoing.buildSchema();

        // retrieve the schema of the input field
        final SchemaPath path = SchemaPath.getSimplePath(column.getInputField());
        final TypedFieldId typedFieldId = outgoing.getSchema().getFieldId(path);

        // throw a fieldChangeError back to coordinator with necessary context to update schema.
        throw UserException.jsonFieldChangeError()
          .message("New field in the schema found.  Please reattempt the query.  Multiple attempts may be necessary to fully learn the schema.")
          .setAdditionalExceptionContext(new JsonFieldChangeExceptionContext(column.getOriginTable(),
            column.getOriginField(),
            typedFieldId.getFinalType()))
          .build(logger);
      }
    }
  }

  @SuppressWarnings("unused")
  public static class ConvertCreator implements Creator<ConvertFromJsonPOP> {

    @Override
    public SingleInputOperator create(OperatorContext context, ConvertFromJsonPOP operator) throws ExecutionSetupException {
      return new ConvertFromJsonOperator(context, operator);
    }

  }
}
