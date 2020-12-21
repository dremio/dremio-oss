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

import static com.google.common.base.Preconditions.checkArgument;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.arrow.flatbuf.Buffer;
import org.apache.arrow.flatbuf.FieldNode;
import org.apache.arrow.flatbuf.RecordBatch;
import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.util.LargeMemoryUtil;
import org.apache.arrow.vector.AllocationHelper;
import org.apache.arrow.vector.BufferLayout;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.TypeLayout;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.compression.NoCompressionCodec;
import org.apache.arrow.vector.ipc.message.ArrowFieldNode;
import org.apache.arrow.vector.ipc.message.ArrowRecordBatch;
import org.apache.arrow.vector.types.pojo.Field;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dremio.common.expression.BasePath;
import com.dremio.exec.record.selection.SelectionVector2;
import com.dremio.exec.record.selection.SelectionVector4;
import com.dremio.sabot.op.receiver.RawFragmentBatch;
import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.collect.FluentIterable;


/**
 * Holds record batch loaded from record batch message.
 */
public class ArrowRecordBatchLoader implements VectorAccessible, Iterable<VectorWrapper<?>>, AutoCloseable {
  private final static Logger logger = LoggerFactory.getLogger(ArrowRecordBatchLoader.class);

  private VectorContainer container;
  private int valueCount;
  private BatchSchema schema;

  public ArrowRecordBatchLoader(VectorContainer container) {
    this.container = container;
    this.schema = container.getSchema();
  }

  public ArrowRecordBatchLoader(BufferAllocator allocator, BatchSchema schema) {
    Preconditions.checkNotNull(allocator);
    this.schema = schema;
    this.container = VectorContainer.create(allocator, schema);
  }


  /**
   * Loads data in batch into Vectors in VectorContainer
   * @param batch
   * @return the total size of the data
   */
  public int load(RawFragmentBatch batch) {
    container.zeroVectors();
    int size = 0;
    try {
      RecordBatch recordBatch = RecordBatch.getRootAsRecordBatch(batch.getHeader().getArrowRecordBatch().asReadOnlyByteBuffer());
      if (batch.getBody() == null) {
        for (VectorWrapper<?> w : container) {
          AllocationHelper.allocate(w.getValueVector(), 0, 0, 0);
        }
        container.setRecordCount(0);
      }

      if (recordBatch.length() > Integer.MAX_VALUE) {
        throw new IllegalArgumentException("record batch length too big: " + recordBatch.length());
      }
      valueCount = (int)recordBatch.length();
      if (valueCount == 0) {
        return 0;
      }
      size = (batch.getBody() == null) ? 0 : LargeMemoryUtil.checkedCastToInt(batch.getBody().readableBytes());
      load(recordBatch, container, batch.getBody());
    } catch (final Throwable cause) {
      // We have to clean up new vectors created here and pass over the actual cause. It is upper layer who should
      // adjudicate to call upper layer specific clean up logic.
      container.zeroVectors();
      throw cause;
    }
    container.setRecordCount(valueCount);
    return size;
  }

  public static ArrowRecordBatch deserializeRecordBatch(RecordBatch recordBatchFB,
                                                        ArrowBuf body) throws IOException {
    // Now read the body
    int nodesLength = recordBatchFB.nodesLength();
    List<ArrowFieldNode> nodes = new ArrayList<>();
    for (int i = 0; i < nodesLength; ++i) {
      FieldNode node = recordBatchFB.nodes(i);
      if ((int)node.length() != node.length() ||
        (int)node.nullCount() != node.nullCount()) {
        throw new IOException("Cannot currently deserialize record batches with " +
          "node length larger than Int.MAX_VALUE");
      }
      nodes.add(new ArrowFieldNode((int)node.length(), (int)node.nullCount()));
    }
    List<ArrowBuf> buffers = new ArrayList<>();
    for (int i = 0; i < recordBatchFB.buffersLength(); ++i) {
      Buffer bufferFB = recordBatchFB.buffers(i);
      ArrowBuf vectorBuffer = body.slice((int)bufferFB.offset(), (int)bufferFB.length());
      buffers.add(vectorBuffer);
    }
    if ((int)recordBatchFB.length() != recordBatchFB.length()) {
      throw new IOException("Cannot currently deserialize record batches over 2GB");
    }
    ArrowRecordBatch arrowRecordBatch =
      new ArrowRecordBatch((int)recordBatchFB.length(), nodes, buffers, NoCompressionCodec.DEFAULT_BODY_COMPRESSION, false);
    for (ArrowBuf buf : buffers) {
      buf.release();
    }
    return arrowRecordBatch;
  }

  public static void load(RecordBatch recordBatch, VectorAccessible vectorAccessible, ArrowBuf body) {
    List<Field> fields = vectorAccessible.getSchema().getFields();
    List<FieldVector> fieldVectors = FluentIterable.from(vectorAccessible)
      .transform(new Function<VectorWrapper<?>, FieldVector>() {
        @Override
        public FieldVector apply(VectorWrapper<?> wrapper) {
          return (FieldVector) wrapper.getValueVector();
        }
      }).toList();
    try {
      ArrowRecordBatch arrowRecordBatch = deserializeRecordBatch(recordBatch, body);
      Iterator<ArrowFieldNode> nodes = arrowRecordBatch.getNodes().iterator();
      Iterator<ArrowBuf> buffers = arrowRecordBatch.getBuffers().iterator();
      for (int i = 0; i < fields.size(); ++i) {
        Field field = fields.get(i);
        FieldVector fieldVector = fieldVectors.get(i);
        loadBuffers(fieldVector, field, buffers, nodes);
      }
      if (buffers.hasNext()) {
        throw new IllegalArgumentException("not all buffers were consumed. " + buffers);
      }
    } catch (IOException e) {
      throw new RuntimeException("could not deserialize batch for " + vectorAccessible.getSchema(), e);
    }
  }

  private static void loadBuffers(FieldVector vector, Field field, Iterator<ArrowBuf> buffers, Iterator<ArrowFieldNode> nodes) {
    checkArgument(nodes.hasNext(), "no more field nodes for for field %s and vector %s", field, vector);
    ArrowFieldNode fieldNode = nodes.next();
    List<BufferLayout> bufferLayouts = TypeLayout.getTypeLayout(field.getType()).getBufferLayouts();
    List<ArrowBuf> ownBuffers = new ArrayList<>(bufferLayouts.size());
    for (int j = 0; j < bufferLayouts.size(); j++) {
      ownBuffers.add(buffers.next());
    }
    try {
      vector.loadFieldBuffers(fieldNode, ownBuffers);
    } catch (RuntimeException e) {
      throw new IllegalArgumentException("Could not load buffers for field " +
          field + ". error message: " + e.getMessage(), e);
    }
    List<Field> children = field.getChildren();
    if (children.size() > 0) {
      List<FieldVector> childrenFromFields = vector.getChildrenFromFields();
      checkArgument(children.size() == childrenFromFields.size(), "should have as many children as in the schema: found " + childrenFromFields.size() + " expected " + children.size());
      for (int i = 0; i < childrenFromFields.size(); i++) {
        Field child = children.get(i);
        FieldVector fieldVector = childrenFromFields.get(i);
        loadBuffers(fieldVector, child, buffers, nodes);
      }
    }
  }

  @Override
  public TypedFieldId getValueVectorId(BasePath path) {
    return container.getValueVectorId(path);
  }

  @Override
  public int getRecordCount() {
    return valueCount;
  }

  @Override
  public <T extends ValueVector> VectorWrapper<T> getValueAccessorById(Class<T> clazz, int... ids){
    return container.getValueAccessorById(clazz, ids);
  }

  public WritableBatch getWritableBatch(){
    boolean isSV2 = (schema.getSelectionVectorMode() == BatchSchema.SelectionVectorMode.TWO_BYTE);
    return WritableBatch.getBatchNoHVWrap(valueCount, container, isSV2);
  }

  @Override
  public Iterator<VectorWrapper<?>> iterator() {
    return this.container.iterator();
  }

  @Override
  public SelectionVector2 getSelectionVector2() {
    throw new UnsupportedOperationException();
  }

  @Override
  public SelectionVector4 getSelectionVector4() {
    throw new UnsupportedOperationException();
  }

  @Override
  public BatchSchema getSchema() {
    return schema;
  }

  public void resetRecordCount() {
    valueCount = 0;
  }

  /**
   * Clears this loader, which clears the internal vector container (see
   * {@link VectorContainer#clear}) and resets the record count to zero.
   */
  public void clear() {
    close();
  }

  public void close(){
    container.clear();
    resetRecordCount();
  }
}
