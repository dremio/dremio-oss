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
package com.dremio.exec.vector;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.OutOfMemoryException;
import org.apache.arrow.memory.util.hash.ArrowBufHasher;
import org.apache.arrow.vector.BaseValueVector;
import org.apache.arrow.vector.BufferBacked;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.compare.VectorVisitor;
import org.apache.arrow.vector.complex.reader.FieldReader;
import org.apache.arrow.vector.holders.ObjectHolder;
import org.apache.arrow.vector.ipc.message.ArrowFieldNode;
import org.apache.arrow.vector.types.Types.MinorType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.util.CallBack;
import org.apache.arrow.vector.util.TransferPair;

import com.dremio.common.util.ObjectType;

import io.netty.buffer.ArrowBuf;

public class ObjectVector extends BaseValueVector implements FieldVector {
  private final String name;
  private int maxCount = 0;
  private int count = 0;
  private int allocationSize = 4096;

  private List<Object[]> objectArrayList = new ArrayList<>();

  public ObjectVector(String name, BufferAllocator allocator) {
    super(allocator);
    this.name = name;
  }

  @Override
  public String getName() {
    return name;
  }

  public void addNewArray() {
    objectArrayList.add(new Object[allocationSize]);
    maxCount += allocationSize;
  }

  @Override
  public Field getField() {
    return new Field(name, false, ObjectType.INTERNAL_OBJECT_TYPE, null);
  }

  @Override
  public FieldReader getReader() {
    throw new UnsupportedOperationException("ObjectVector does not support this");
  }


  public void set(int index, Object obj) {
    int listOffset = index / allocationSize;
    if (listOffset >= objectArrayList.size()) {
      addNewArray();
    }
    objectArrayList.get(listOffset)[index % allocationSize] = obj;
  }

  public boolean setSafe(int index, long value) {
    set(index, value);
    return true;
  }

  protected void set(int index, ObjectHolder holder) {
    set(index, holder.obj);
  }

  public boolean setSafe(int index, ObjectHolder holder){
    set(index, holder);
    return true;
  }

  @Override
  public void setValueCount(int valueCount) {
    count = valueCount;
  }

  public void reset() {
    count = 0;
    maxCount = 0;
    objectArrayList = new ArrayList<>();
    addNewArray();
  }

  public void generateTestData(int values) { }

  @Override
  public void setInitialCapacity(int numRecords) {
    // NoOp
  }

  @Override
  public void allocateNew() throws OutOfMemoryException {
    addNewArray();
  }

  public void allocateNew(int valueCount) throws OutOfMemoryException {
    while (maxCount < valueCount) {
      addNewArray();
    }
  }

  @Override
  public boolean allocateNewSafe() {
    allocateNew();
    return true;
  }

  @Override
  public int getBufferSize() {
    throw new UnsupportedOperationException("ObjectVector does not support this");
  }

  @Override
  public int getBufferSizeFor(final int valueCount) {
    throw new UnsupportedOperationException("ObjectVector does not support this");
  }

  @Override
  public void close() {
    clear();
  }

  @Override
  public void clear() {
    objectArrayList.clear();
    maxCount = 0;
    count = 0;
  }

  @Override
  public long getValidityBufferAddress() {
    throw new UnsupportedOperationException();
  }

  @Override
  public long getDataBufferAddress() {
    throw new UnsupportedOperationException();
  }

  @Override
  public long getOffsetBufferAddress() {
    throw new UnsupportedOperationException();
  }

  @Override
  public ArrowBuf getValidityBuffer() {
    throw new UnsupportedOperationException();
  }

  @Override
  public ArrowBuf getDataBuffer() {
    throw new UnsupportedOperationException();
  }

  @Override
  public ArrowBuf getOffsetBuffer() {
    throw new UnsupportedOperationException();
  }

  @Override
  public MinorType getMinorType() {
    throw new UnsupportedOperationException("ObjectVector does not support this");
  }

  @Override
  public TransferPair getTransferPair(BufferAllocator allocator) {
    throw new UnsupportedOperationException("ObjectVector does not support this");
  }

  @Override
  public TransferPair makeTransferPair(ValueVector to) {
    throw new UnsupportedOperationException("ObjectVector does not support this");
  }

  @Override
  public TransferPair getTransferPair(String ref, BufferAllocator allocator) {
    throw new UnsupportedOperationException("ObjectVector does not support this");
  }

  @Override
  public TransferPair getTransferPair(String s, BufferAllocator bufferAllocator, CallBack callBack) {
    throw new UnsupportedOperationException("ObjectVector does not support this");
  }

  @Override
  public int getValueCapacity() {
    return maxCount;
  }

  @Override
  public ArrowBuf[] getBuffers(boolean clear) {
    throw new UnsupportedOperationException("ObjectVector does not support this");
  }

//  @Override
//  public void load(UserBitShared.SerializedField metadata, ArrowBuf buffer) {
//    throw new UnsupportedOperationException("ObjectVector does not support this");
//  }
//
//  @Override
//  public UserBitShared.SerializedField getMetadata() {
//    throw new UnsupportedOperationException("ObjectVector does not support this");
//  }

  @Override
  public Iterator<ValueVector> iterator() {
    throw new UnsupportedOperationException("ObjectVector does not support this");
  }

  @Override
  public Object getObject(int index) {
    int listOffset = index / allocationSize;
    if (listOffset >= objectArrayList.size()) {
      addNewArray();
    }
    return objectArrayList.get(listOffset)[index % allocationSize];
  }

  @Override
  public int getNullCount() {
    return 0;
  }

  @Override
  public boolean isNull(int index) {
    return false;
  }

  @Override
  public int getValueCount() {
    return count;
  }

  public Object get(int index) {
    return getObject(index);
  }

  public void get(int index, ObjectHolder holder){
    holder.obj = getObject(index);
  }

  @Override
  public void initializeChildrenFromFields(List<Field> children) {
    throw new UnsupportedOperationException("NYI");
  }

  @Override
  public List<FieldVector> getChildrenFromFields() {
    throw new UnsupportedOperationException("NYI");
  }

  @Override
  public void loadFieldBuffers(ArrowFieldNode fieldNode, List<ArrowBuf> ownBuffers) {
    throw new UnsupportedOperationException("NYI");
  }

  @Override
  public List<ArrowBuf> getFieldBuffers() {
    throw new UnsupportedOperationException("NYI");
  }

  @Override
  public List<BufferBacked> getFieldInnerVectors() {
    throw new UnsupportedOperationException("NYI");
  }

  @Override
  public void reAlloc() {
    addNewArray();
  }

  @Override
  public int hashCode(int index) {
    throw new UnsupportedOperationException("NYI");
  }

  @Override
  public int hashCode(int i, ArrowBufHasher arrowBufHasher) {
    throw new UnsupportedOperationException("NYI");
  }

  @Override
  public <OUT, IN> OUT accept(VectorVisitor<OUT, IN> vectorVisitor, IN in) {
    throw new UnsupportedOperationException("ObjectVector does not support this");
  }
}
