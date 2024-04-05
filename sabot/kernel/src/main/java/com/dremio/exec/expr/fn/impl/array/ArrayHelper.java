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

import java.util.Stack;
import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.vector.complex.impl.ComplexCopier;
import org.apache.arrow.vector.complex.impl.UnionListReader;
import org.apache.arrow.vector.complex.reader.FieldReader;
import org.apache.arrow.vector.complex.writer.BaseWriter;
import org.apache.arrow.vector.complex.writer.FieldWriter;

/** Utility class for array functions */
public final class ArrayHelper {
  private ArrayHelper() {}

  // this method will choose writer by reader's minor type and copy data from reader to writer
  public static void copyListElements(
      UnionListReader listReader, BaseWriter.ListWriter listWriter) {
    copyToList(listReader.reader(), listWriter);
  }

  public static void copyToList(FieldReader fieldReader, BaseWriter.ListWriter listWriter) {
    FieldWriter writer;
    switch (fieldReader.getMinorType()) {
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
    while (listReader.next()) {
      if (listReader.reader().readObject() == null) {
        continue;
      }
      FieldWriter writer;
      switch (in.reader().getMinorType()) {
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

  public static void generateIntList(BaseWriter.ComplexWriter out, int start, int stop, int step) {
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

  public static final class ListSorter {
    private Stack<Integer> sortStack = new Stack<>();
    private ArrowBuf sortIndexes;
    private final FieldReader reader;
    private final int initialPositiion;

    public ListSorter(ArrowBuf sortIndexes, FieldReader reader, int initialPosition) {
      this.sortIndexes = sortIndexes;
      this.reader = reader;
      this.initialPositiion = initialPosition;
    }

    public void sort(int size) {
      for (int i = 0; i < size; i++) {
        setIndexMap(i, i);
      }
      if (size < 2) {
        return;
      }

      sortStack.push(0);
      sortStack.push(size - 1);

      while (true) {
        if (sortStack.isEmpty()) {
          break;
        }
        int endIndex = sortStack.pop();
        int startIndex = sortStack.pop();
        if (startIndex >= endIndex) {
          continue;
        }

        int medIndex = findMedian(startIndex, endIndex);

        medIndex = swapValues(startIndex, medIndex, endIndex);
        if (medIndex < 0) {
          continue;
        }
        sortStack.push(startIndex);
        sortStack.push(medIndex - 1);
        sortStack.push(medIndex + 1);
        sortStack.push(endIndex);
      }
    }

    protected int swapValues(int start, int med, int end) {
      Comparable medValue = (Comparable) readObject(med);
      swapIndexes(med, end);

      int i = start - 1;

      for (int j = start; j < end; j++) {
        Comparable value = (Comparable) readObject(j);
        if (compare(value, medValue) <= 0) {
          i++;
          swapIndexes(i, j);
        }
      }

      swapIndexes(i + 1, end);
      return i + 1;
    }

    protected void swapIndexes(int leftIndex, int rightIndex) {
      int leftIndexMapped = mapIndex(leftIndex);
      int rightIndexMapped = mapIndex(rightIndex);
      setIndexMap(leftIndex, rightIndexMapped);
      setIndexMap(rightIndex, leftIndexMapped);
    }

    protected int findMedian(int startIndex, int endIndex) {
      if (startIndex == endIndex - 1) {
        return startIndex;
      } else {
        int medIndex = (endIndex - startIndex) / 2 + startIndex;
        Comparable startValue = (Comparable) readObject(startIndex);
        Comparable medValue = (Comparable) readObject(medIndex);
        Comparable endValue = (Comparable) readObject(endIndex);
        int startVsEnd = compare(startValue, endValue);
        int midVsEnd = compare(medValue, endValue);
        int midVsStart = compare(medValue, startValue);

        if (startVsEnd < 0) {
          if (midVsStart < 0) {
            return startIndex;
          } else {
            if (midVsEnd < 0) {
              return medIndex;
            } else {
              return endIndex;
            }
          }
        } else {
          if (midVsStart > 0) {
            return startIndex;
          } else {
            if (midVsEnd < 0) {
              return endIndex;
            } else {
              return medIndex;
            }
          }
        }
      }
    }

    public int mapIndex(long i) {
      return sortIndexes.getInt(i * 4);
    }

    protected void setIndexMap(long i, int value) {
      sortIndexes.setInt(i * 4, value);
    }

    protected Object readObject(int index) {
      reader.setPosition(initialPositiion + mapIndex(index));
      Object o = reader.readObject();
      if (o instanceof org.apache.arrow.vector.util.Text) {
        return o.toString();
      }
      return o;
    }

    protected int compare(Comparable a, Comparable b) {
      if (a == null) {
        return -1;
      }
      if (b == null) {
        return 1;
      }
      return a.compareTo(b);
    }
  }
}
