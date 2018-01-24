/*
 * Copyright (C) 2017 Dremio Corporation
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
package com.dremio.exec.util;

import java.util.List;

import org.apache.commons.lang3.StringUtils;
import org.apache.arrow.vector.ValueVector;

import com.dremio.exec.record.VectorAccessible;
import com.dremio.exec.record.VectorWrapper;
import com.dremio.exec.record.selection.SelectionVector2;
import com.dremio.exec.record.selection.SelectionVector4;
import com.google.common.collect.Lists;

/**
 * This is a tool for printing the content of record batches to screen. Used for debugging.
 */
public class BatchPrinter {

  private static final int ROWS_TO_PRINT = 10;

  private static void printBatchSV4(VectorAccessible batch, SelectionVector4 sv4) {
    List<String> columns = Lists.newArrayList();
    for (VectorWrapper<?> vw : batch) {
      columns.add(vw.getValueVectors()[0].getField().getName());
    }
    int width = columns.size();
    for (int j = 0; j < sv4.getCount(); j++) {
      if (j%50 == 0) {
        System.out.println(StringUtils.repeat("-", width * 17 + 1));
        for (String column : columns) {
          System.out.printf("| %-15s", width <= 15 ? column : column.substring(0, 14));
        }
        System.out.printf("|\n");
        System.out.println(StringUtils.repeat("-", width*17 + 1));
      }
      for (VectorWrapper<?> vw : batch) {
        Object o = vw.getValueVectors()[sv4.get(j) >>> 16].getObject(sv4.get(j) & 65535);
        String value;
        if (o == null) {
          value = "null";
        } else if (o instanceof byte[]) {
          value = new String((byte[]) o);
        } else {
          value = o.toString();
        }
        System.out.printf("| %-15s",value.length() <= 15 ? value : value.substring(0,14));
      }
      System.out.printf("|\n");
    }
    System.out.printf("|\n");
  }

  public static void printBatch(VectorAccessible batch) {
    switch(batch.getSchema().getSelectionVectorMode()){
    case FOUR_BYTE:
      printBatchSV4(batch, batch.getSelectionVector4());
      break;
    case NONE:
      printBatchNoSV(batch);
      break;
    case TWO_BYTE:
      printBatchSV2(batch, batch.getSelectionVector2());
      break;
    default:
      break;

    }
  }
  private static void printBatchNoSV(VectorAccessible batch) {
    List<String> columns = Lists.newArrayList();
    List<ValueVector> vectors = Lists.newArrayList();
    for (VectorWrapper<?> vw : batch) {
      columns.add(vw.getValueVector().getField().getName());
      vectors.add(vw.getValueVector());
    }
    int width = columns.size();
    int rows = batch.getRecordCount();
    rows = Math.min(ROWS_TO_PRINT, rows);
    for (int row = 0; row < rows; row++) {
      if (row%50 == 0) {
        System.out.println(StringUtils.repeat("-", width * 52 + 1));
        for (String column : columns) {
          System.out.printf("| %-50s", width <= 50 ? column : column.substring(0, Math.min(column.length(), 49)));
        }
        System.out.printf("|\n");
        System.out.println(StringUtils.repeat("-", width*52 + 1));
      }
      for (ValueVector vv : vectors) {
        Object o = vv.getObject(row);
        String value;
        if (o == null) {
          value = "null";
        } else
        if (o instanceof byte[]) {
          value = new String((byte[]) o);
        } else {
          value = o.toString();
        }
        System.out.printf("| %-50s",value.length() <= 50 ? value : value.substring(0, 49));
      }
      System.out.printf("|\n");
    }
  }

  private static void printBatchSV2(VectorAccessible batch, SelectionVector2 sv2) {
    List<String> columns = Lists.newArrayList();
    List<ValueVector> vectors = Lists.newArrayList();
    for (VectorWrapper<?> vw : batch) {
      columns.add(vw.getValueVector().getField().getName());
      vectors.add(vw.getValueVector());
    }
    int width = columns.size();
    int rows = batch.getRecordCount();
    rows = Math.min(ROWS_TO_PRINT, rows);
    for (int i = 0; i < rows; i++) {
      if (i%50 == 0) {
        System.out.println(StringUtils.repeat("-", width * 32 + 1));
        for (String column : columns) {
          System.out.printf("| %-30s", width <= 30 ? column : column.substring(0, 29));
        }
        System.out.printf("|\n");
        System.out.println(StringUtils.repeat("-", width*32 + 1));
      }
      int row = sv2.getIndex(i);
      for (ValueVector vv : vectors) {
        Object o = vv.getObject(row);
        String value;
        if (o == null) {
          value = "null";
        } else
        if (o instanceof byte[]) {
          value = new String((byte[]) o);
        } else {
          value = o.toString();
        }
        System.out.printf("| %-30s",value.length() <= 30 ? value : value.substring(0, 29));
      }
      System.out.printf("|\n");
    }
  }
}
