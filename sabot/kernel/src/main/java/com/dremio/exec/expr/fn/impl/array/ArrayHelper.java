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
package com.dremio.exec.expr.fn.impl.array;

import org.apache.arrow.vector.complex.impl.ComplexCopier;
import org.apache.arrow.vector.complex.impl.UnionListReader;
import org.apache.arrow.vector.complex.reader.FieldReader;
import org.apache.arrow.vector.complex.writer.BaseWriter;
import org.apache.arrow.vector.complex.writer.FieldWriter;

/**
 * Utility class for array functions
 */
public final class ArrayHelper {
  private ArrayHelper() {
  }

  //this method will choose writer by reader's minor type and copy data from reader to writer
  public static void copyListElements(UnionListReader listReader, BaseWriter.ListWriter listWriter) {
    copyToList(listReader.reader(), listWriter);
  }

  public static void copyToList(FieldReader fieldReader, BaseWriter.ListWriter listWriter) {
    FieldWriter writer;
    switch (fieldReader.getMinorType()){
      case LIST:
      case LARGELIST:
      case FIXED_SIZE_LIST:
        writer = (org.apache.arrow.vector.complex.writer.FieldWriter) listWriter.list();
        break;
      case STRUCT:
        writer = (org.apache.arrow.vector.complex.writer.FieldWriter) listWriter.struct();
        break;
      case MAP:
        writer = (org.apache.arrow.vector.complex.writer.FieldWriter) listWriter.map();
        break;
      default:
        writer = (org.apache.arrow.vector.complex.writer.FieldWriter) listWriter;
    }
    ComplexCopier.copy(fieldReader, writer);
  }

  public static void removeNullList(FieldReader in, BaseWriter.ComplexWriter out) {
    UnionListReader listReader = (UnionListReader) in;
    BaseWriter.ListWriter listWriter = out.rootAsList();
    listWriter.startList();
    while (listReader.next()){
      if (listReader.reader().readObject() == null) {
        continue;
      }
      FieldWriter writer;
      switch (in.reader().getMinorType()){
        case LIST:
        case FIXED_SIZE_LIST:
          writer = (org.apache.arrow.vector.complex.writer.FieldWriter) listWriter.list();
          break;
        case STRUCT:
          writer = (org.apache.arrow.vector.complex.writer.FieldWriter) listWriter.struct();
          break;
        case LARGELIST:
          throw new UnsupportedOperationException(in.reader().getMinorType().toString());
        case MAP:
          writer = (org.apache.arrow.vector.complex.writer.FieldWriter) listWriter.map(false);
          break;
        default:
          writer = (org.apache.arrow.vector.complex.writer.FieldWriter) listWriter;
      }
      ComplexCopier.copy(listReader.reader(), writer);
    }
    listWriter.endList();
  }

  public static void generateIntList(BaseWriter.ComplexWriter out, int start, int stop, int step){
    boolean isIncrementalRange = start <= stop;

    org.apache.arrow.vector.complex.writer.BaseWriter.ListWriter listWriter = out.rootAsList();
    listWriter.startList();
    if (isIncrementalRange && step > 0) {
      for (int i = start; i < stop; i += step) {
        listWriter.integer().writeInt(i);
      }
    } else if (!isIncrementalRange && step < 0) {
      for (int i = start; i > stop; i += step) {
        listWriter.integer().writeInt(i);
      }
    }
    listWriter.endList();
  }
}
