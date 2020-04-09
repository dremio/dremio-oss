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
package com.dremio.exec.record;

import static com.dremio.common.util.MajorTypeHelper.getArrowMinorType;
import static com.google.common.base.Preconditions.checkNotNull;

import java.lang.reflect.Array;
import java.util.Iterator;
import java.util.List;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.complex.FieldIdUtil2;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.arrow.vector.util.CallBack;
import org.apache.arrow.vector.util.TransferPair;

import com.dremio.common.expression.BasePath;
import com.dremio.common.expression.CompleteType;
import com.dremio.common.types.TypeProtos.MajorType;
import com.dremio.exec.expr.TypeHelper;
import com.dremio.exec.record.BatchSchema.SelectionVectorMode;
import com.dremio.exec.record.selection.SelectionVector2;
import com.dremio.exec.record.selection.SelectionVector4;
import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.Lists;

public class VectorContainer implements Iterable<VectorWrapper<?>>, VectorAccessible, AutoCloseable {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(VectorContainer.class);

  private final Resetter resetter = new Resetter();
  protected final List<VectorWrapper<?>> wrappers = Lists.newArrayList();
  protected final BufferAllocator allocator;

  private BatchSchema schema;
  private int recordCount = -1;

  public VectorContainer() {
    this.allocator = null;
  }

  public VectorContainer(BufferAllocator allocator) {
    this.allocator = allocator;
  }

  @Override
  public String toString() {
    return super.toString()
        + "[recordCount = " + recordCount
        + ", schema = " + schema
        + ", wrappers = " + wrappers
        + ", ...]";
  }

  public void addHyperList(List<ValueVector> vectors) {
    addHyperList(vectors, true);
  }

  private void clearSchema(){
    schema = null;
  }

  public void addHyperList(List<ValueVector> vectors, boolean releasable) {
    clearSchema();
    ValueVector[] vv = new ValueVector[vectors.size()];
    for (int i = 0; i < vv.length; i++) {
      vv[i] = vectors.get(i);
    }
    add(vv, releasable);
  }

  /**
   * Transfer vectors from containerIn to this.
   */
  void transferIn(VectorContainer containerIn) {
    Preconditions.checkArgument(this.wrappers.size() == containerIn.wrappers.size());
    for (int i = 0; i < this.wrappers.size(); ++i) {
      containerIn.wrappers.get(i).transfer(this.wrappers.get(i));
    }
  }

  public void addSchema(Schema schema){
    clearSchema();
    for(Field f : schema.getFields()) {
      addOrGet(f);
    }
  }

  /**
   * Transfer vectors from this to containerOut
   */
  public void transferOut(VectorContainer containerOut) {
    Preconditions.checkArgument(this.wrappers.size() == containerOut.wrappers.size());
    for (int i = 0; i < this.wrappers.size(); ++i) {
      this.wrappers.get(i).transfer(containerOut.wrappers.get(i));
    }
  }

  private class Resetter implements CallBack {
    @Override
    public void doWork() {
      clearSchema();
    }
  }

  public void addIfMissing(Field field, boolean isHyper) {
    final TypedFieldId id = FieldIdUtil2.getFieldId(getPartialSchema(), BasePath.getSimple(field.getName()));
    if(id == null){
      if(isHyper){
        addEmptyHyper(field);
      }else{
        ValueVector vector = TypeHelper.getNewVector(field, allocator, resetter);
        add(vector);
      }
    }
  }

  public boolean isNewSchema(){
    return schema == null;
  }

  @SuppressWarnings("unchecked")
  public <T extends ValueVector> T addOrGet(final Field field) {
    final TypedFieldId id = FieldIdUtil2.getFieldId(getPartialSchema(), BasePath.getSimple(field.getName()));
    final ValueVector vector;
    if (id != null) {
      final Class<?> clazz = CompleteType.fromField(field).getValueVectorClass();
      vector = getValueAccessorById(id.getFieldIds()).getValueVector();
      if (id.getFieldIds().length == 1 && clazz != null && !clazz.isAssignableFrom(vector.getClass())) {
        final ValueVector newVector = TypeHelper.getNewVector(field, allocator, resetter);
        replace(vector, newVector);
        return (T) newVector;
      }
    } else {
      vector = TypeHelper.getNewVector(field, allocator, resetter);
      add(vector);
    }
    return (T) vector;
  }

  public <T extends ValueVector> T addOrGet(String name, MajorType type, Class<T> clazz) {
    Field field = new Field(name, true, getArrowMinorType(type.getMinorType()).getType(), null);
    return addOrGet(field);
  }

  /**
   * Get a set of transferred clones of this container. Note that this guarantees that the vectors in the cloned
   * container have the same TypedFieldIds as the existing container, allowing interchangeability in generated code. In
   * the case of hyper vectors, this container actually doesn't do a full transfer, rather creating a clone vector
   * wrapper only.
   *
   * @param incoming
   *          The RecordBatch iterator the contains the batch we should take over.
   * @return A cloned vector container.
   */
  @SuppressWarnings("rawtypes")
  public static VectorContainer getTransferClone(VectorAccessible incoming, BufferAllocator allocator) {
    final VectorContainer vc = new VectorContainer(allocator);
    for (VectorWrapper<?> w : incoming) {
      vc.cloneAndTransfer(w);
    }
    vc.setRecordCount(incoming.getRecordCount());
    vc.buildSchema(SelectionVectorMode.NONE);
    return vc;
  }

  public static void transferFromRoot(VectorSchemaRoot root, VectorContainer container, BufferAllocator allocator) {
    container.clear();
    // iterate over and transfer columns
    for (FieldVector fv : root.getFieldVectors()) {
      final TransferPair tp = fv.getTransferPair(allocator);
      tp.transfer();
      container.add(tp.getTo());
    }

    container.setRecordCount(root.getRowCount());
    container.addSchema(root.getSchema());
    container.buildSchema();
  }

  public static VectorContainer create(BufferAllocator allocator, Schema schema){
    VectorContainer container = new VectorContainer(allocator);
    for (Field field : schema.getFields()) {
      container.addOrGet(field);
    }
    container.buildSchema(SelectionVectorMode.NONE);
    return container;
  }

  private void cloneAndTransfer(VectorWrapper<?> wrapper) {
    wrappers.add(wrapper.cloneAndTransfer(allocator, resetter));
  }

  public void addCollection(Iterable<ValueVector> vectors) {
    clearSchema();
    for (ValueVector vv : vectors) {
      wrappers.add(SimpleVectorWrapper.create(vv));
    }
  }

  public TypedFieldId add(ValueVector vv) {
    clearSchema();
    int i = wrappers.size();
    wrappers.add(SimpleVectorWrapper.create(vv));
    return new TypedFieldId(CompleteType.fromField(vv.getField()), i);
  }

  public <T extends ValueVector> void addEmptyHyper(Field f) {
    clearSchema();
    ValueVector[] c = (ValueVector[]) Array.newInstance(TypeHelper.getValueVectorClass(f), 0);
    wrappers.add(HyperVectorWrapper.create(f, c, true));
  }

  public void add(ValueVector[] hyperVector) {
    add(hyperVector, true);
  }

  public void add(ValueVector[] hyperVector, boolean releasable) {
    assert hyperVector.length != 0;
    clearSchema();
    Class<?> clazz = hyperVector[0].getClass();
    ValueVector[] c = (ValueVector[]) Array.newInstance(clazz, hyperVector.length);
    for (int i = 0; i < hyperVector.length; i++) {
      c[i] = hyperVector[i];
    }
    // todo: work with a merged schema.
    wrappers.add(HyperVectorWrapper.create(hyperVector[0].getField(), c, releasable));
  }

  public void remove(ValueVector v) {
    clearSchema();
    for (Iterator<VectorWrapper<?>> iter = wrappers.iterator(); iter.hasNext();) {
      VectorWrapper<?> w = iter.next();
      if (!w.isHyper() && v == w.getValueVector()) {
        w.clear();
        iter.remove();
        return;
      }
    }
    throw new IllegalStateException("You attempted to remove a vector that didn't exist.");
  }

  public void replace(ValueVector old, ValueVector newVector) {
    clearSchema();
    int i = 0;
    for (VectorWrapper<?> w : wrappers){
      if (!w.isHyper() && old == w.getValueVector()) {
        w.clear();
        wrappers.set(i, new SimpleVectorWrapper<ValueVector>(newVector));
        return;
      }
      i++;
    }
    throw new IllegalStateException("You attempted to remove a vector that didn't exist.");
  }

  @Override
  public TypedFieldId getValueVectorId(BasePath path) {
    return FieldIdUtil2.getFieldId(getPartialSchema(), path);
  }

  @SuppressWarnings("unchecked")
  @Override
  public <T extends ValueVector> VectorWrapper<T> getValueAccessorById(Class<T> clazz, int... fieldIds) {
    Preconditions.checkArgument(fieldIds.length >= 1);
    VectorWrapper<?> va = wrappers.get(fieldIds[0]);

    if (va == null) {
      return null;
    }

    if (fieldIds.length == 1 && clazz != null && !clazz.isAssignableFrom(va.getVectorClass())) {
      throw new IllegalStateException(String.format(
          "Failure while reading vector.  Expected vector class of %s but was holding vector class %s, field= %s ",
          clazz.getCanonicalName(), va.getVectorClass().getCanonicalName(), va.getField()));
    }

    return (VectorWrapper<T>) va.getChildWrapper(fieldIds);

  }

  private VectorWrapper<?> getValueAccessorById(int... fieldIds) {
    Preconditions.checkArgument(fieldIds.length >= 1);
    VectorWrapper<?> va = wrappers.get(fieldIds[0]);

    if (va == null) {
      return null;
    }
    return va.getChildWrapper(fieldIds);
  }

  public boolean hasSchema() {
    return schema != null;
  }

  private BatchSchema getPartialSchema() {
    if(schema == null){
      SchemaBuilder bldr = BatchSchema.newBuilder().setSelectionVectorMode(SelectionVectorMode.NONE);
      for (VectorWrapper<?> v : wrappers) {
        bldr.addField(v.getField());
      }
      return bldr.build();
    } else {
      return schema;
    }
  }

  @SuppressWarnings("unused")
  private BufferAllocator getAllocator() {
    throw new UnsupportedOperationException("Intentionally not exposing the allocator of the VectorContainer. " +
                                              "Doing so might accidentally expose the fragment output allocator");
  }

  @Override
  public BatchSchema getSchema() {
    // throws if buildSchema(...) was not called before this call, which happens if the caller did not
    // build the schema in the first place, or the schema was cleared using clearSchema(...)
    checkNotNull(schema, "Container schema is not set. Either schema was not built, or schema was cleared.");
    return schema;
  }

  public void buildSchema(){
    buildSchema(SelectionVectorMode.NONE);
  }

  public void buildSchema(SelectionVectorMode mode) {
    SchemaBuilder bldr = BatchSchema.newBuilder().setSelectionVectorMode(mode);
    for (VectorWrapper<?> v : wrappers) {
      bldr.addField(v.getField());
    }
    this.schema = bldr.build();
  }

  @Override
  public Iterator<VectorWrapper<?>> iterator() {
    return wrappers.iterator();
  }

  public void clear() {
    close();
  }

  @Override
  public void close(){
    clearSchema();
    zeroVectors();
    wrappers.clear();
  }

  public void setRecordCount(int recordCount) {
    this.recordCount = recordCount;
  }

  @Override
  public int getRecordCount() {
    Preconditions.checkState(recordCount != -1, "Record count not set for this vector container");
    return recordCount;
  }

  @Override
  public SelectionVector2 getSelectionVector2() {
    throw new UnsupportedOperationException();
  }

  @Override
  public SelectionVector4 getSelectionVector4() {
    throw new UnsupportedOperationException();
  }

  /**
   * Clears the contained vectors.  (See {@link ValueVector#clear}).
   */
  public void zeroVectors() {
    for (VectorWrapper<?> w : wrappers) {
      w.clear();
    }
  }

  public int getNumberOfColumns() {
    return this.wrappers.size();
  }

  public void setInitialCapacity(int initialCapacity) {
    for (VectorWrapper<?> w : wrappers) {
      final ValueVector vv = w.getValueVector();
      vv.setInitialCapacity(initialCapacity);
    }
  }

  public void allocateNew() {
    for (VectorWrapper<?> w : wrappers) {
      w.getValueVector().allocateNew();
    }
  }

  public int setAllCount(int records){
    if(records != 0){
      for(VectorWrapper<?> w : this){
        w.getValueVector().setValueCount(records);
      }
    }
    setRecordCount(records);
    return records;
  }

  public static List<FieldVector[]> getHyperFieldVectors(VectorAccessible a){
    return FluentIterable.from(a).transform(HYPER_WRAPPER_TO_FIELD).toList();
  }

  public static List<FieldVector> getFieldVectors(VectorAccessible a){
    return FluentIterable.from(a).transform(WRAPPER_TO_FIELD).toList();
  }

  public static final Function<VectorWrapper<?>, FieldVector>  WRAPPER_TO_FIELD = new Function<VectorWrapper<?>, FieldVector>(){
    @Override
    public FieldVector apply(VectorWrapper<?> input) {
      return (FieldVector) input.getValueVector();
    }};

  public static final Function<VectorWrapper<?>, FieldVector[]>  HYPER_WRAPPER_TO_FIELD = new Function<VectorWrapper<?>, FieldVector[]>(){
    @Override
    public FieldVector[] apply(VectorWrapper<?> input) {
      return (FieldVector[]) input.getValueVectors();
    }};
}
