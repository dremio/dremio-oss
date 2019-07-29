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
package com.dremio.exec.store.pojo;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.sql.Timestamp;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.arrow.memory.OutOfMemoryException;
import org.apache.arrow.vector.ValueVector;
import org.apache.calcite.rel.type.RelDataType;

import com.dremio.common.exceptions.ExecutionSetupException;
import com.dremio.common.expression.SchemaPath;
import com.dremio.exec.physical.base.GroupScan;
import com.dremio.exec.planner.types.JavaTypeFactoryImpl;
import com.dremio.exec.record.BatchSchema;
import com.dremio.exec.store.AbstractRecordReader;
import com.dremio.exec.store.RecordDataType;
import com.dremio.exec.store.pojo.Writers.BitWriter;
import com.dremio.exec.store.pojo.Writers.DoubleWriter;
import com.dremio.exec.store.pojo.Writers.EnumWriter;
import com.dremio.exec.store.pojo.Writers.IntWriter;
import com.dremio.exec.store.pojo.Writers.LongWriter;
import com.dremio.exec.store.pojo.Writers.StringWriter;
import com.dremio.exec.store.pojo.Writers.TimeStampMilliWriter;
import com.dremio.sabot.op.scan.OutputMutator;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

public class PojoRecordReader<T> extends AbstractRecordReader implements Iterable<T> {

  private final Class<T> pojoClass;
  private final List<T> pojoObjects;
  private PojoWriter[] writers;
  private boolean doCurrent;
  private T currentPojo;

  private Iterator<T> currentIterator;

  public PojoRecordReader(Class<T> pojoClass, Iterator<T> iterator) {
    this(pojoClass, iterator, null);
  }

  public PojoRecordReader(Class<T> pojoClass, Iterator<T> iterator, List<SchemaPath> columns) {
    super(null, columns);
    this.pojoClass = pojoClass;
    this.pojoObjects = ImmutableList.copyOf(iterator);
  }

  @Override
  public void setup(OutputMutator output) throws ExecutionSetupException {

    final Set<String> selectedColumns = new HashSet<>();
    if(this.getColumns() != null) {
      for(SchemaPath path : getColumns()) {
        Preconditions.checkArgument(path.isSimplePath());
        selectedColumns.add(path.getAsUnescapedPath().toLowerCase());
      }
    }

    Field[] fields = pojoClass.getDeclaredFields();
    List<PojoWriter> writers = Lists.newArrayList();

    for (int i = 0; i < fields.length; i++) {
      Field f = fields[i];

      if (Modifier.isStatic(f.getModifiers())) {
        continue;
      }

      Class<?> type = f.getType();
      PojoWriter w = null;
      if(type == int.class || type == Integer.class) {
        w = new IntWriter(f);
      } else if(type == long.class || type == Long.class) {
        w = new LongWriter(f);
      } else if(type == boolean.class || type == Boolean.class) {
        w = new BitWriter(f);
      } else if(type == double.class || type == Double.class) {
        w = new DoubleWriter(f);
      } else if(type.isEnum()) {
        w = new EnumWriter(f, output.getManagedBuffer());
      } else if(type == String.class) {
        w = new StringWriter(f, output.getManagedBuffer());
      } else if (type == Timestamp.class) {
        w = new TimeStampMilliWriter(f);
      } else {
        throw new ExecutionSetupException(String.format("PojoRecord reader doesn't yet support conversions from type [%s].", type));
      }

      // only add writers that are included in selected columns.
      if(getColumns() == null || getColumns().equals(GroupScan.ALL_COLUMNS) || selectedColumns.contains(f.getName().toLowerCase())) {
        writers.add(w);
        w.init(output);
      }
    }

    this.writers = writers.toArray(new PojoWriter[writers.size()]);


    currentIterator = pojoObjects.iterator();
  }

  @Override
  public void allocate(Map<String, ValueVector> vectorMap) throws OutOfMemoryException {
    for (final ValueVector v : vectorMap.values()) {
      v.allocateNew();
    }
  }

  private void allocate() {
    for (PojoWriter writer : writers) {
      writer.allocate();
    }
  }

  private void setValueCount(int i) {
    for (PojoWriter writer : writers) {
      writer.setValueCount(i);
    }
  }

  @Override
  public int next() {
    boolean allocated = false;
    //injector.injectPause(operatorContext.getExecutionControls(), "read-next", logger);
    try {
      int i =0;
      while (doCurrent || currentIterator.hasNext()) {
        if (doCurrent) {
          doCurrent = false;
        } else {
          currentPojo = currentIterator.next();
        }

        if (!allocated) {
          allocate();
          allocated = true;
        }

        for (PojoWriter writer : writers) {
          writer.writeField(currentPojo, i);
        }
        i++;
      }

      if (i != 0 ) {
        setValueCount(i);
      }
      return i;
    } catch (IllegalArgumentException | IllegalAccessException e) {
      throw new RuntimeException("Failure while trying to use PojoRecordReader.", e);
    }
  }

  @Override
  public Iterator<T> iterator() {
    return pojoObjects.iterator();
  }

  @Override
  public void close() {
  }

  @Override
  protected boolean supportsSkipAllQuery() {
    return true;
  }

  public static BatchSchema getSchema(Class<?> pojoClass){
    RecordDataType dataType = new PojoDataType(pojoClass);
    RelDataType type = dataType.getRowType(JavaTypeFactoryImpl.INSTANCE);
    return BatchSchema.fromCalciteRowType(type);

  }
}
