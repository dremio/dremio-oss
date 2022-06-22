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
package com.dremio.exec.util;

import java.util.List;

import org.apache.arrow.vector.ValueVector;
import org.apache.commons.lang3.StringUtils;

import com.dremio.exec.record.VectorAccessible;
import com.dremio.exec.record.VectorWrapper;
import com.dremio.exec.record.selection.SelectionVector2;
import com.dremio.exec.record.selection.SelectionVector4;
import com.google.common.collect.Lists;

/**
 * This is a tool for printing the content of record batches to console or a logger, or both. Used for debugging.
 */
public class BatchPrinter {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(BatchPrinter.class);

  private static final int ROWS_TO_PRINT = 10;

  private static void printBatchSV4(VectorAccessible batch, SelectionVector4 sv4,
                                    boolean debugPrint, boolean debugLog) {
    List<String> columns = Lists.newArrayList();
    for (VectorWrapper<?> vw : batch) {
      columns.add(vw.getValueVectors()[0].getField().getName());
    }
    int width = columns.size();
    for (int j = 0; j < sv4.getCount(); j++) {
      if (j%50 == 0) {
        print(String.format(StringUtils.repeat("-", width * 17 + 1) + "%n"), debugPrint, debugLog);
        StringBuilder columnData = new StringBuilder();
        for (String column : columns) {
          columnData.append(String.format("| %-15s", width <= 15 ? column : column.substring(0, 14)));
        }
        print(columnData.toString(), debugPrint, debugLog);
        print(String.format("|%n"), debugPrint, debugLog);
        print(String.format(StringUtils.repeat("-", width*17 + 1) + "%n"), debugPrint, debugLog);
      }
      StringBuilder columnData = new StringBuilder();
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
        columnData.append(String.format("| %-15s",value.length() <= 15 ? value : value.substring(0,14)));
      }
      print(columnData.toString(), debugPrint, debugLog);
      print(String.format("|%n"), debugPrint, debugLog);
    }
    print(String.format("|%n"), debugPrint, debugLog);
  }

  public static void printBatch(VectorAccessible batch,
                                boolean debugPrint, boolean debugLog) {
    switch(batch.getSchema().getSelectionVectorMode()){
    case FOUR_BYTE:
      printBatchSV4(batch, batch.getSelectionVector4(), debugPrint, debugLog);
      break;
    case NONE:
      printBatchNoSV(batch, debugPrint, debugLog);
      break;
    case TWO_BYTE:
      printBatchSV2(batch, batch.getSelectionVector2(), debugPrint, debugLog);
      break;
    default:
      break;

    }
  }
  private static void printBatchNoSV(VectorAccessible batch,
                                     boolean debugPrint, boolean debugLog) {
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
        print(String.format(StringUtils.repeat("-", width * 52 + 1) + "%n"), debugPrint, debugLog);
        StringBuilder columnData = new StringBuilder();
        for (String column : columns) {
          columnData.append(String.format("| %-50s", width <= 50 ? column : column.substring(0, Math.min(column.length(), 49))));
        }
        print(columnData.toString(), debugPrint, debugLog);
        print(String.format("|%n"), debugPrint, debugLog);
        print(String.format(StringUtils.repeat("-", width*52 + 1) + "%n"), debugPrint, debugLog);
      }
      StringBuilder columnData = new StringBuilder();
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
        columnData.append(String.format("| %-50s",value.length() <= 50 ? value : value.substring(0, 49)));
      }
      print(columnData.toString(), debugPrint, debugLog);
      print(String.format("|%n"), debugPrint, debugLog);
    }
  }

  private static void printBatchSV2(VectorAccessible batch, SelectionVector2 sv2,
                                    boolean debugPrint, boolean debugLog) {
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
        print(String.format(StringUtils.repeat("-", width * 32 + 1) + "%n"), debugPrint, debugLog);
        StringBuilder columnData = new StringBuilder();
        for (String column : columns) {
          columnData.append(String.format("| %-30s", width <= 30 ? column : column.substring(0, 29)));
        }
        print(columnData.toString(), debugPrint, debugLog);
        print(String.format("|%n"), debugPrint, debugLog);
        print(String.format(StringUtils.repeat("-", width*32 + 1) + "%n"), debugPrint, debugLog);
      }
      int row = sv2.getIndex(i);
      StringBuilder columnData = new StringBuilder();
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
        columnData.append(String.format("| %-30s",value.length() <= 30 ? value : value.substring(0, 29)));
      }
      print(columnData.toString(), debugPrint, debugLog);
      print(String.format("|%n"), debugPrint, debugLog);
    }
  }

  private static void print(String str, boolean debugPrint, boolean debugLog) {
    if (debugLog) {
      logger.debug(str);
    }
    if (debugPrint) {
      System.out.print(str);
    }
  }
}
