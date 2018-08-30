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
package com.dremio.sabot.op.copier;

import java.util.ArrayList;
import java.util.List;

import org.apache.arrow.memory.OutOfMemoryException;
import org.apache.arrow.vector.util.TransferPair;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.types.pojo.Field;

import com.dremio.common.AutoCloseables;
import com.dremio.common.exceptions.ExecutionSetupException;
import com.dremio.exec.ExecConstants;
import com.dremio.exec.exception.SchemaChangeException;
import com.dremio.exec.expr.ClassProducer;
import com.dremio.exec.expr.CodeGenerator;
import com.dremio.exec.physical.config.SelectionVectorRemover;
import com.dremio.exec.record.VectorAccessible;
import com.dremio.exec.record.VectorContainer;
import com.dremio.exec.record.VectorWrapper;
import com.dremio.exec.record.BatchSchema.SelectionVectorMode;
import com.dremio.exec.vector.CopyUtil;
import com.dremio.sabot.exec.context.FunctionContext;
import com.dremio.sabot.exec.context.OperatorContext;
import com.dremio.sabot.op.spi.SingleInputOperator;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

public class CopierOperator implements SingleInputOperator {

  private final OperatorContext context;
  private final ClassProducer producer;

  private State state = State.NEEDS_SETUP;
  private int copyOffset = 0;

  private VectorAccessible incoming;
  private VectorContainer output;
  private Copier copier;
  private List<TransferPair> transferPairs = new ArrayList<>();

  // random vector to determine if no copy is needed.
  private ValueVector randomVector;
  // if set to true, outputData() will use transfer instead of copy when no copy is needed
  private boolean checkForStraightCopy;

  public CopierOperator(OperatorContext context, SelectionVectorRemover popConfig) throws OutOfMemoryException {
    this.producer = context.getClassProducer();
    this.context = context;
  }

  @Override
  public VectorAccessible setup(VectorAccessible incoming) {
    this.incoming = incoming;

    for(VectorWrapper<?> w : incoming){
      randomVector = w.getValueVector();
      checkForStraightCopy = true;
    }

    output = context.createOutputVectorContainer(incoming.getSchema());
    switch(incoming.getSchema().getSelectionVectorMode()){
    case NONE:
      this.copier = getStraightCopier();
      checkForStraightCopy = false;
      break;
    case TWO_BYTE:
      this.copier = getGenerated2Copier();
      break;
    case FOUR_BYTE:
      this.copier = getGenerated4Copier();
      break;
    default:
      throw new UnsupportedOperationException();
    }
    output.buildSchema(SelectionVectorMode.NONE);
    state = State.CAN_CONSUME;

    if(checkForStraightCopy){
      for(VectorWrapper<?> vv : incoming){
        TransferPair tp = vv.getValueVector().makeTransferPair(output.addOrGet(vv.getField()));
        transferPairs.add(tp);
      }
    }

    return output;
  }

  @Override
  public State getState() {
    return state;
  }

  @Override
  public void noMoreToConsume() {
    state = State.DONE;
  }

  @Override
  public void consumeData(int targetRecords) {
    // do nothing.
    state = State.CAN_PRODUCE;

  }

  @Override
  public int outputData() {
    if(checkForStraightCopy && incoming.getRecordCount() == randomVector.getValueCount()){
      for(TransferPair tp : transferPairs){
        tp.transfer();
      }
      output.setRecordCount(incoming.getRecordCount());
      state = State.CAN_CONSUME;
      return incoming.getRecordCount();
    }

    int recordCount = incoming.getRecordCount() - this.copyOffset;
    int copiedRecords = copier.copyRecords(copyOffset, recordCount);
    if(copiedRecords < recordCount){
      copyOffset = copyOffset + copiedRecords;
    }else{
      copyOffset = 0;
      state = State.CAN_CONSUME;
    }

    if(copiedRecords > 0){
      for(VectorWrapper<?> v : output){
        v.getValueVector().setValueCount(copiedRecords);
      }
    }
    output.setRecordCount(copiedRecords);
    return copiedRecords;
  }

  @Override
  public void close() throws Exception {
    AutoCloseables.close(output);
  }

  private class StraightCopier implements Copier {

    private List<TransferPair> pairs = Lists.newArrayList();

    @Override
    public void setupRemover(FunctionContext context, VectorAccessible incoming, VectorAccessible outgoing){
      for(VectorWrapper<?> vv : incoming){
        TransferPair tp = vv.getValueVector().makeTransferPair(output.addOrGet(vv.getField()));
        pairs.add(tp);
      }
    }

    @Override
    public int copyRecords(int index, int recordCount) {
      assert index == 0 && recordCount == incoming.getRecordCount() : "Straight copier cannot split batch";
      for(TransferPair tp : pairs){
        tp.transfer();
      }
      return recordCount;
    }

    @Override
    public void close() throws Exception {
    }

  }

  @Override
  public <OUT, IN, EXCEP extends Throwable> OUT accept(OperatorVisitor<OUT, IN, EXCEP> visitor, IN value) throws EXCEP {
    return visitor.visitSingleInput(this, value);
  }

  private Copier getStraightCopier(){
    StraightCopier copier = new StraightCopier();
    copier.setupRemover(producer.getFunctionContext(), incoming, output);
    return copier;
  }

  private Copier getGenerated2Copier() throws SchemaChangeException{
    return getGenerated2Copier(producer, incoming, output);
  }

  public static Copier getGenerated2Copier(ClassProducer producer, VectorAccessible incoming, VectorContainer output) throws SchemaChangeException{
    Preconditions.checkArgument(incoming.getSchema().getSelectionVectorMode() == SelectionVectorMode.TWO_BYTE,
      "getGenerated2Copier expects a TWO_BYTE selectionVectorMode");

    for(VectorWrapper<?> vv : incoming){
      vv.getValueVector().makeTransferPair(output.addOrGet(vv.getField()));
    }

    final CodeGenerator<Copier> cg = producer.createGenerator(Copier.TEMPLATE_DEFINITION2);
    CopyUtil.generateCopies(cg.getRoot(), incoming, false);
    Copier copier = cg.getImplementationClass();
    copier.setupRemover(producer.getFunctionContext(), incoming, output);

    return copier;
  }

  private Copier getGenerated4Copier() throws SchemaChangeException {
    Preconditions.checkArgument(incoming.getSchema().getSelectionVectorMode() == SelectionVectorMode.FOUR_BYTE);
    return getGenerated4Copier(context.getClassProducer(), incoming, output);
  }

  public static Copier getGenerated4Copier(ClassProducer producer, VectorAccessible incoming, VectorContainer outgoing) throws SchemaChangeException{

    for(Field f : incoming.getSchema()){
      outgoing.addIfMissing(f, false);
    }
    final CodeGenerator<Copier> cg = producer.createGenerator(Copier.TEMPLATE_DEFINITION4);
    CopyUtil.generateCopies(cg.getRoot(), incoming, true);
    Copier copier = cg.getImplementationClass();
    copier.setupRemover(producer.getFunctionContext(), incoming, outgoing);
    return copier;
  }

  public static class SVCreator implements SingleInputOperator.Creator<SelectionVectorRemover> {

    @Override
    public SingleInputOperator create(OperatorContext context, SelectionVectorRemover operator)
        throws ExecutionSetupException {
      if(context.getOptions().getOption(ExecConstants.ENABLE_VECTORIZED_COPIER)){
        return new VectorizedCopyOperator(context, operator);
      } else {
        return new CopierOperator(context, operator);
      }
    }
  }


}
