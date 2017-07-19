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
package com.dremio.exec.store.hbase;

import static com.dremio.common.util.MajorTypeHelper.getArrowMinorType;
import static com.dremio.common.util.MajorTypeHelper.getFieldForNameAndMajorType;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableSet;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.apache.arrow.vector.NullableVarBinaryVector;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.complex.NullableMapVector;
import org.apache.arrow.vector.types.Types.MinorType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.FirstKeyOnlyFilter;

import com.dremio.common.exceptions.ExecutionSetupException;
import com.dremio.common.expression.PathSegment;
import com.dremio.common.expression.PathSegment.NameSegment;
import com.dremio.common.expression.SchemaPath;
import com.dremio.exec.exception.SchemaChangeException;
import com.dremio.exec.store.AbstractRecordReader;
import com.dremio.sabot.exec.context.OperatorContext;
import com.dremio.sabot.exec.context.OperatorStats;
import com.dremio.sabot.op.scan.OutputMutator;
import com.google.common.base.Preconditions;
import com.google.common.base.Stopwatch;
import com.google.common.collect.Sets;

public class HBaseRecordReader extends AbstractRecordReader implements HBaseConstants {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(HBaseRecordReader.class);

  private OutputMutator outputMutator;

  private Map<String, NullableMapVector> familyVectorMap;
  private NullableVarBinaryVector rowKeyVector;

  private Table hTable;
  private ResultScanner resultScanner;

  private TableName hbaseTableName;
  private final Scan hbaseScan;
  private final OperatorContext context;

  private final boolean sample;
  private boolean rowKeyOnly;
  private final Connection connection;

  public HBaseRecordReader(
      Connection connection,
      HBaseSubScan.HBaseSubScanSpec subScanSpec,
      List<SchemaPath> projectedColumns,
      OperatorContext context,
      boolean sample) {
    super(context, projectedColumns);
    this.connection = connection;
    hbaseTableName = TableName.valueOf(
        Preconditions.checkNotNull(subScanSpec, "HBase reader needs a sub-scan spec").getTableName());
    hbaseScan = new Scan(
        subScanSpec.getStartRow() == null ? HConstants.EMPTY_START_ROW : subScanSpec.getStartRow(),
        subScanSpec.getStopRow() == null ? HConstants.EMPTY_END_ROW : subScanSpec.getStopRow());
    this.context = context;
    this.sample = sample;
    hbaseScan
        .setFilter(subScanSpec.getScanFilter())
        .setCaching((int) numRowsPerBatch);

    if (!isStarQuery()) {
      for (SchemaPath column : projectedColumns) {
        if (!column.getRootSegment().getPath().equalsIgnoreCase(ROW_KEY)) {
          NameSegment root = column.getRootSegment();
          byte[] family = root.getPath().getBytes();
          PathSegment child = root.getChild();
          if (child != null && child.isNamed()) {
            byte[] qualifier = child.getNameSegment().getPath().getBytes();
            hbaseScan.addColumn(family, qualifier);
          } else {
            hbaseScan.addFamily(family);
          }
        }
      }
    }

    /* if only the row key was requested, add a FirstKeyOnlyFilter to the scan
     * to fetch only one KV from each row. If a filter is already part of this
     * scan, add the FirstKeyOnlyFilter as the LAST filter of a MUST_PASS_ALL
     * FilterList.
     */
    if (rowKeyOnly) {
      hbaseScan.setFilter(
              HBaseUtils.andFilterAtIndex(hbaseScan.getFilter(), HBaseUtils.LAST_FILTER, new FirstKeyOnlyFilter()));
    }
  }

  public void setNumRowsPerBatch(long numRowsPerBatch) {
    this.numRowsPerBatch = numRowsPerBatch;
  }

  @Override
  protected Collection<SchemaPath> transformColumns(Collection<SchemaPath> columns) {
    Set<SchemaPath> transformed = Sets.newLinkedHashSet();
    rowKeyOnly = true;
    if (!isStarQuery()) {
      for (SchemaPath column : columns) {
        if (column.getRootSegment().getPath().equalsIgnoreCase(ROW_KEY)) {
          transformed.add(ROW_KEY_PATH);
          continue;
        }
        rowKeyOnly = false;
        NameSegment root = column.getRootSegment();
        transformed.add(SchemaPath.getSimplePath(root.getPath()));
      }
    } else {
      rowKeyOnly = false;
      transformed.add(ROW_KEY_PATH);
    }
    return transformed;
  }

  @Override
  public void setup(OutputMutator output) throws ExecutionSetupException {
    this.outputMutator = output;
    familyVectorMap = new HashMap<>();

    try {
      hTable = connection.getTable(hbaseTableName);

      // Add top-level column-family map vectors to output in the order specified
      // when creating reader (order of first appearance in query).
      for (SchemaPath column : getColumns()) {
        if (column.equals(ROW_KEY_PATH)) {
          if (sample) {
            Field field = new Field(column.getAsNamePart().getName(), true, MinorType.VARBINARY.getType(), null);
            rowKeyVector = outputMutator.addField(field, NullableVarBinaryVector.class);
          } else {
            rowKeyVector = (NullableVarBinaryVector) output.getVector(ROW_KEY);
          }
        } else {
          getOrCreateFamilyVector(output, column.getRootSegment().getPath(), false);
        }
      }

      // Add map and child vectors for any HBase column families and/or HBase
      // columns that are requested (in order to avoid later creation of dummy
      // NullableIntVectors for them).
      final Set<Map.Entry<byte[], NavigableSet<byte []>>> familiesEntries =
          hbaseScan.getFamilyMap().entrySet();
      for (Map.Entry<byte[], NavigableSet<byte []>> familyEntry : familiesEntries) {
        final String familyName = new String(familyEntry.getKey(),
                                             StandardCharsets.UTF_8);
        final NullableMapVector familyVector = getOrCreateFamilyVector(output, familyName, false);
        final Set<byte []> children = familyEntry.getValue();
        if (null != children) {
          for (byte[] childNameBytes : children) {
            final String childName = new String(childNameBytes,
                                                StandardCharsets.UTF_8);
            getOrCreateColumnVector(familyVector, childName);
          }
        }
      }
      resultScanner = hTable.getScanner(hbaseScan);
    } catch (SchemaChangeException | IOException e) {
      throw new ExecutionSetupException(e);
    }
  }

  @Override
  public int next() {
    Stopwatch watch = Stopwatch.createStarted();
    if (rowKeyVector != null) {
      rowKeyVector.clear();
      rowKeyVector.allocateNew();
    }
    for (ValueVector v : familyVectorMap.values()) {
      v.clear();
      v.allocateNew();
    }

    int rowCount = 0;
    done:
    for (; rowCount < numRowsPerBatch; rowCount++) {
      Result result = null;
      final OperatorStats operatorStats = context == null ? null : context.getStats();
      try {
        if (operatorStats != null) {
          operatorStats.startWait();
        }
        try {
          result = resultScanner.next();
        } finally {
          if (operatorStats != null) {
            operatorStats.stopWait();
          }
        }
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
      if (result == null) {
        break done;
      }

      // parse the result and populate the value vectors
      Cell[] cells = result.rawCells();
      if (rowKeyVector != null) {
        rowKeyVector.getMutator().setSafe(rowCount, cells[0].getRowArray(), cells[0].getRowOffset(), cells[0].getRowLength());
      }
      if (!rowKeyOnly) {
        for (final Cell cell : cells) {
          final int familyOffset = cell.getFamilyOffset();
          final int familyLength = cell.getFamilyLength();
          final byte[] familyArray = cell.getFamilyArray();
          final NullableMapVector mv = getOrCreateFamilyVector(outputMutator, new String(familyArray, familyOffset, familyLength), true);
          mv.getMutator().setIndexDefined(rowCount);
          final int qualifierOffset = cell.getQualifierOffset();
          final int qualifierLength = cell.getQualifierLength();
          final byte[] qualifierArray = cell.getQualifierArray();
          final NullableVarBinaryVector v = getOrCreateColumnVector(mv, new String(qualifierArray, qualifierOffset, qualifierLength));

          final int valueOffset = cell.getValueOffset();
          final int valueLength = cell.getValueLength();
          final byte[] valueArray = cell.getValueArray();
          v.getMutator().setSafe(rowCount, valueArray, valueOffset, valueLength);
        }
      }
    }

    setOutputRowCount(rowCount);
    logger.debug("Took {} ms to get {} records", watch.elapsed(TimeUnit.MILLISECONDS), rowCount);
    return rowCount;
  }

  private NullableMapVector getOrCreateFamilyVector(OutputMutator output, String familyName, boolean allocateOnCreate) {
    NullableMapVector v = familyVectorMap.get(familyName);
    if(v == null) {
      SchemaPath column = SchemaPath.getSimplePath(familyName);
      Field field = getFieldForNameAndMajorType(column.getAsNamePart().getName(), COLUMN_FAMILY_TYPE);
      if (sample) {
        v = outputMutator.addField(field, NullableMapVector.class);
        if (allocateOnCreate) {
          v.allocateNew();
        }
      } else {
        v = (NullableMapVector) output.getVector(column.getAsNamePart().getName());
      }
      getColumns().add(column);
      familyVectorMap.put(familyName, v);
    }
    return v;
  }

  private NullableVarBinaryVector getOrCreateColumnVector(NullableMapVector mv, String qualifier) {
    int oldSize = mv.size();
    NullableVarBinaryVector v = mv.addOrGet(qualifier, FieldType.nullable(getArrowMinorType(COLUMN_TYPE.getMinorType()).getType()), NullableVarBinaryVector.class);
    if (oldSize != mv.size()) {
      v.allocateNew();
    }
    return v;
  }

  @Override
  public void close() {
    try {
      if (resultScanner != null) {
        resultScanner.close();
      }
      if (hTable != null) {
        hTable.close();
      }
    } catch (IOException e) {
      logger.warn("Failure while closing HBase table: " + hbaseTableName, e);
    }
  }

  private void setOutputRowCount(int count) {
    for (ValueVector vv : familyVectorMap.values()) {
      vv.getMutator().setValueCount(count);
    }
    if (rowKeyVector != null) {
      rowKeyVector.getMutator().setValueCount(count);
    }
  }
}
