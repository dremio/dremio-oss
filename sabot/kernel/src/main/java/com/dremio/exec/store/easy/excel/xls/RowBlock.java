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
package com.dremio.exec.store.easy.excel.xls;

import java.util.ArrayList;
import java.util.List;

import org.apache.poi.hssf.model.RecordStream;
import org.apache.poi.hssf.record.ArrayRecord;
import org.apache.poi.hssf.record.BlankRecord;
import org.apache.poi.hssf.record.BoolErrRecord;
import org.apache.poi.hssf.record.ColumnInfoRecord;
import org.apache.poi.hssf.record.DVALRecord;
import org.apache.poi.hssf.record.DrawingRecord;
import org.apache.poi.hssf.record.DrawingSelectionRecord;
import org.apache.poi.hssf.record.EOFRecord;
import org.apache.poi.hssf.record.FormulaRecord;
import org.apache.poi.hssf.record.GutsRecord;
import org.apache.poi.hssf.record.LabelRecord;
import org.apache.poi.hssf.record.LabelSSTRecord;
import org.apache.poi.hssf.record.MergeCellsRecord;
import org.apache.poi.hssf.record.NumberRecord;
import org.apache.poi.hssf.record.ObjRecord;
import org.apache.poi.hssf.record.RKRecord;
import org.apache.poi.hssf.record.Record;
import org.apache.poi.hssf.record.RowRecord;
import org.apache.poi.hssf.record.SharedFormulaRecord;
import org.apache.poi.hssf.record.TableRecord;
import org.apache.poi.hssf.record.TextObjectRecord;
import org.apache.poi.hssf.record.WindowOneRecord;
import org.apache.poi.hssf.record.WindowTwoRecord;
import org.apache.poi.hssf.record.aggregates.PageSettingsBlock;
import org.apache.poi.hssf.record.aggregates.RowRecordsAggregate;
import org.apache.poi.hssf.record.aggregates.SharedValueManager;
import org.apache.poi.hssf.record.pivottable.ViewDefinitionRecord;
import org.apache.poi.ss.util.CellReference;

/**
 * Handles a Row block that contains all cell values of a sheet along with row formatting information.
 *
 * Simplified version of {@link org.apache.poi.hssf.model.RowBlocksReader}

 */
class RowBlock {
  private final List<Record> plainRecords = new ArrayList<>();
  private final List<SharedFormulaRecord> shFrmRecords = new ArrayList<>();
  private final List<CellReference> firstCellRefs = new ArrayList<>();
  private final List<ArrayRecord> arrayRecords = new ArrayList<>();
  private final List<TableRecord> tableRecords = new ArrayList<>();
  private final List<MergeCellsRecord> mergeCellRecords = new ArrayList<>();
  private Record prevRec = null;

  public void process(Record r) {
    switch (r.getSid()) {
      case MergeCellsRecord.sid:
        mergeCellRecords.add((MergeCellsRecord) r);
        break;
      case SharedFormulaRecord.sid:
        shFrmRecords.add((SharedFormulaRecord) r);
        if (!(prevRec instanceof FormulaRecord)) {
          throw new RuntimeException("Shared formula record should follow a FormulaRecord");
        }
        FormulaRecord fr = (FormulaRecord) prevRec;
        firstCellRefs.add(new CellReference(fr.getRow(), fr.getColumn()));
        break;
      case ArrayRecord.sid:
        arrayRecords.add((ArrayRecord) r);
        break;
      case TableRecord.sid:
        tableRecords.add((TableRecord) r);
        break;
      default:
        plainRecords.add(r);
        break;
    }
    prevRec = r;
  }

  private SharedValueManager getSharedFormulaManager() {
    SharedFormulaRecord[] sharedFormulaRecs = new SharedFormulaRecord[shFrmRecords.size()];
    CellReference[] firstCells = new CellReference[firstCellRefs.size()];
    ArrayRecord[] arrayRecs = new ArrayRecord[arrayRecords.size()];
    TableRecord[] tableRecs = new TableRecord[tableRecords.size()];
    shFrmRecords.toArray(sharedFormulaRecs);
    firstCellRefs.toArray(firstCells);
    arrayRecords.toArray(arrayRecs);
    tableRecords.toArray(tableRecs);

    return SharedValueManager.create(sharedFormulaRecs, firstCells, arrayRecs, tableRecs);
  }

  private RecordStream getPlainRecordStream() {
    return new RecordStream(plainRecords, 0);
  }

  RowRecordsAggregate getRowRecordsAggregate() {
    return new RowRecordsAggregate(getPlainRecordStream(), getSharedFormulaManager());
  }

  List<MergeCellsRecord> getMergeCellRecords() {
    return mergeCellRecords;
  }

  /**
   * @return <code>true</code> if the specified record ID terminates a sequence of Row block records
   * It is assumed that at least one row or cell value record has been found prior to the current
   * record
   */
  static boolean isEndOfRowBlock(int sid) {
    switch(sid) {
      case ViewDefinitionRecord.sid:
        // should have been prefixed with DrawingRecord (0x00EC), but bug 46280 seems to allow this
      case DrawingRecord.sid:
      case DrawingSelectionRecord.sid:
      case ObjRecord.sid:
      case TextObjectRecord.sid:
      case ColumnInfoRecord.sid: // See Bugzilla 53984
      case GutsRecord.sid:   // see Bugzilla 50426
      case WindowOneRecord.sid:
        // should really be part of workbook stream, but some apps seem to put this before WINDOW2
      case WindowTwoRecord.sid:
        return true;

      case DVALRecord.sid:
        return true;
      case EOFRecord.sid:
        // WINDOW2 should always be present, so shouldn't have got this far
        throw new RuntimeException("Found EOFRecord before WindowTwoRecord was encountered");
    }
    return PageSettingsBlock.isComponentRecord(sid);
  }

  static boolean isRowBlockRecord(int sid) {
    switch (sid) {
      case RowRecord.sid:

      case BlankRecord.sid:
      case BoolErrRecord.sid:
      case FormulaRecord.sid:
      case LabelRecord.sid:
      case LabelSSTRecord.sid:
      case NumberRecord.sid:
      case RKRecord.sid:

      case ArrayRecord.sid:
      case SharedFormulaRecord.sid:
      case TableRecord.sid:
        return true;
    }
    return false;
  }

}
