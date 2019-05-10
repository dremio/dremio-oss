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

import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.complex.StructVector;
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
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FirstKeyOnlyFilter;

import com.dremio.common.AutoCloseables;
import com.dremio.common.exceptions.ExecutionSetupException;
import com.dremio.common.expression.CompleteType;
import com.dremio.common.expression.PathSegment;
import com.dremio.common.expression.PathSegment.NameSegment;
import com.dremio.common.expression.SchemaPath;
import com.dremio.exec.exception.SchemaChangeException;
import com.dremio.exec.proto.CoordExecRPC.HBaseSubScanSpec;
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

  private Map<String, StructVector> familyVectorMap;
  private VarBinaryVector rowKeyVector;

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
      HBaseSubScanSpec subScanSpec,
      List<SchemaPath> projectedColumns,
      OperatorContext context,
      boolean sample) {
    super(context, projectedColumns);
    this.connection = connection;
    hbaseTableName = TableName.valueOf(
        Preconditions.checkNotNull(subScanSpec, "HBase reader needs a sub-scan spec").getNamespace(),
        Preconditions.checkNotNull(subScanSpec, "HBase reader needs a sub-scan spec").getTableName());
    hbaseScan = new Scan(
        subScanSpec.hasStartRow() ? subScanSpec.getStartRow().toByteArray() : HConstants.EMPTY_START_ROW,
        subScanSpec.hasStopRow() ? subScanSpec.getStopRow().toByteArray() : HConstants.EMPTY_END_ROW);
    this.context = context;
    this.sample = sample;
    Filter filter =
      subScanSpec.hasSerializedFilter() ?
        HBaseUtils.deserializeFilter(subScanSpec.getSerializedFilter().toByteArray()) : null;
    hbaseScan
        .setFilter(filter)
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
            Field field = CompleteType.VARBINARY.toField(column.getAsNamePart().getName());
            rowKeyVector = outputMutator.addField(field, VarBinaryVector.class);
          } else {
            rowKeyVector = (VarBinaryVector) output.getVector(ROW_KEY);
          }
        } else {
          getOrCreateFamilyVector(output, column.getRootSegment().getPath(), false);
        }
      }

      // Add map and child vectors for any HBase column families and/or HBase
      // columns that are requested (in order to avoid later creation of dummy
      // IntVectors for them).
      final Set<Map.Entry<byte[], NavigableSet<byte []>>> familiesEntries =
          hbaseScan.getFamilyMap().entrySet();
      for (Map.Entry<byte[], NavigableSet<byte []>> familyEntry : familiesEntries) {
        final String familyName = new String(familyEntry.getKey(),
                                             StandardCharsets.UTF_8);
        final StructVector familyVector = getOrCreateFamilyVector(output, familyName, false);
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
        rowKeyVector.setSafe(rowCount, cells[0].getRowArray(), cells[0].getRowOffset(), cells[0].getRowLength());
      }
      if (!rowKeyOnly) {
        for (final Cell cell : cells) {
          final int familyOffset = cell.getFamilyOffset();
          final int familyLength = cell.getFamilyLength();
          final byte[] familyArray = cell.getFamilyArray();
          final StructVector sv = getOrCreateFamilyVector(outputMutator, new String(familyArray, familyOffset, familyLength), true);
          sv.setIndexDefined(rowCount);
          final int qualifierOffset = cell.getQualifierOffset();
          final int qualifierLength = cell.getQualifierLength();
          final byte[] qualifierArray = cell.getQualifierArray();
          final VarBinaryVector v = getOrCreateColumnVector(sv, new String(qualifierArray, qualifierOffset, qualifierLength));

          final int valueOffset = cell.getValueOffset();
          final int valueLength = cell.getValueLength();
          final byte[] valueArray = cell.getValueArray();
          v.setSafe(rowCount, valueArray, valueOffset, valueLength);
        }
      }
    }

    setOutputRowCount(rowCount);
    logger.debug("Took {} ms to get {} records", watch.elapsed(TimeUnit.MILLISECONDS), rowCount);
    return rowCount;
  }

  private StructVector getOrCreateFamilyVector(OutputMutator output, String familyName, boolean allocateOnCreate) {
    StructVector v = familyVectorMap.get(familyName);
    if(v == null) {
      SchemaPath column = SchemaPath.getSimplePath(familyName);
      Field field = getFieldForNameAndMajorType(column.getAsNamePart().getName(), COLUMN_FAMILY_TYPE);
      if (sample) {
        v = outputMutator.addField(field, StructVector.class);
        if (allocateOnCreate) {
          v.allocateNew();
        }
      } else {
        v = (StructVector) output.getVector(column.getAsNamePart().getName());
      }
      getColumns().add(column);
      familyVectorMap.put(familyName, v);
    }
    return v;
  }

  private VarBinaryVector getOrCreateColumnVector(StructVector sv, String qualifier) {
    int oldSize = sv.size();
    VarBinaryVector v = sv.addOrGet(qualifier, FieldType.nullable(getArrowMinorType(COLUMN_TYPE.getMinorType()).getType()), VarBinaryVector.class);
    if (oldSize != sv.size()) {
      v.allocateNew();
    }
    return v;
  }

  @Override
  public void close() throws Exception {
    AutoCloseables.close(resultScanner, hTable);
  }

  private void setOutputRowCount(int count) {
    for (ValueVector vv : familyVectorMap.values()) {
      vv.setValueCount(count);
    }
    if (rowKeyVector != null) {
      rowKeyVector.setValueCount(count);
    }
  }

  @Override
  protected boolean supportsSkipAllQuery() {
    return true;
  }
}
