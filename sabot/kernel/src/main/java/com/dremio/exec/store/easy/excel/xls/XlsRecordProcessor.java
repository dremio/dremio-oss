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

import java.io.IOException;
import java.io.InputStream;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.vector.complex.impl.VectorContainerWriter;
import org.apache.arrow.vector.complex.writer.BaseWriter;
import org.apache.poi.hssf.OldExcelFormatException;
import org.apache.poi.hssf.record.BOFRecord;
import org.apache.poi.hssf.record.BlankRecord;
import org.apache.poi.hssf.record.BoolErrRecord;
import org.apache.poi.hssf.record.BoundSheetRecord;
import org.apache.poi.hssf.record.CellValueRecordInterface;
import org.apache.poi.hssf.record.DateWindow1904Record;
import org.apache.poi.hssf.record.EOFRecord;
import org.apache.poi.hssf.record.FormulaRecord;
import org.apache.poi.hssf.record.LabelRecord;
import org.apache.poi.hssf.record.LabelSSTRecord;
import org.apache.poi.hssf.record.MergeCellsRecord;
import org.apache.poi.hssf.record.NumberRecord;
import org.apache.poi.hssf.record.Record;
import org.apache.poi.hssf.record.RecordFactoryInputStream;
import org.apache.poi.hssf.record.SSTRecord;
import org.apache.poi.hssf.record.aggregates.FormulaRecordAggregate;
import org.apache.poi.hssf.record.aggregates.RowRecordsAggregate;
import org.apache.poi.ss.usermodel.CellType;
import org.apache.poi.ss.usermodel.DateUtil;
import org.apache.poi.ss.usermodel.FormulaError;
import org.apache.poi.ss.util.CellRangeAddress;
import org.apache.poi.util.LocaleUtil;

import com.dremio.common.AutoCloseables;
import com.dremio.common.exceptions.FieldSizeLimitExceptionHelper;
import com.dremio.common.exceptions.UserException;
import com.dremio.exec.store.easy.excel.ColumnNameHandler;
import com.dremio.exec.store.easy.excel.ExcelFormatPluginConfig;
import com.dremio.exec.store.easy.excel.ExcelParser;
import com.dremio.exec.store.easy.excel.SheetNotFoundException;
import com.google.common.base.Charsets;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

/**
 * XLS parser based on Apache POI.<br>
 * <br>
 * Uses a combination of {@link XlsInputStream} and {@link BlockStoreInputStream} to read the file
 * in direct memory. It still relies on a {@link RecordFactoryInputStream} which will instantiate all
 * cells in heap.
 * <br>
 * This class takes care of closing the given {@link XlsInputStream} when we close it.
 */
public class XlsRecordProcessor implements ExcelParser {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(XlsRecordProcessor.class);

  private final InputStream workbookStream;

  private final BaseWriter.StructWriter writer;
  private final RecordFactoryInputStream recordStream;
  private final FormatManager formatManager;
  private ArrowBuf managedBuf;
  private boolean skipQuery;
  private final int maxCellSize;

  private SSTRecord sstRecord;
  private boolean dateWindow1904; // true if workbook is using 1904 date system

  private Iterator<CellValueRecordInterface> cellValueIterator;

  private int lastRow = -1; // last read row
  private CellValueRecordInterface nextCell; // next unprocessed cell

  private boolean missingRowBlock; // some empty xls files don't contain a row block

  private final ColumnNameHandler columnNameHandler = new ColumnNameHandler();

  /**
   * Merge cell map. Key is the top-left cell in the merged region. Value is null if there is no merge cell
   * expansion needed or no merge cells found.
   */
  private Map<Integer, MergedCell> mergeCells;

  /* lookup table to find if a particular column is to be projected or not */
  private final HashSet<String> columnsToProject;

  /**
   * Extracts the workbook stream for the byte array and instantiates a {@link RecordFactoryInputStream} that will
   * be used to process al Records of the stream.
   *
   * @param is XLS InputStream
   * @param writer will be used to write the retrieved record into the outgoing vector container
   * @param managedBuf managed buffer used to allocate VarChar values
   * @param skipQuery if a query should skip columns
   * @param maxCellSize maximum allowable size of variable length cells
   *
   * @throws IOException if data doesn't start with a proper XLS header
   * @throws SheetNotFoundException if the sheet is not part of the workbook
   */
  public XlsRecordProcessor(final XlsInputStream is, final ExcelFormatPluginConfig pluginConfig,
                            final VectorContainerWriter writer, final ArrowBuf managedBuf,
                            final HashSet<String> columnsToProject, final boolean skipQuery,
                            final int maxCellSize) throws IOException, SheetNotFoundException {
    this.writer = writer.rootAsStruct();
    this.managedBuf = managedBuf;
    this.columnsToProject = columnsToProject;
    this.skipQuery = skipQuery;
    this.maxCellSize = maxCellSize;

    // following code reads and orders all of the workbook's sectors into a new byte array
    try {
      workbookStream = XlsReader.createWorkbookInputStream(is);
    } catch (OldExcelFormatException e) {
      throw UserException.dataReadError(e).build(logger);
    }

    // WARNING RecordFactoryInputStream will ignore DBCells among others and will also instantiate every single
    // Record it parses
    recordStream = new RecordFactoryInputStream(workbookStream, true);

    // keeps track of Format/ExtendedFormat records
    formatManager = new FormatManager();

    init(pluginConfig.sheet, pluginConfig.extractHeader, pluginConfig.hasMergedCells);
  }

  /**
   * Extracts a set of general information from the workbook stream, moves to the beginning
   * of the target sheet, then extract all row blocks and merged cells from the stream.
   * This includes:
   * <ul>
   *   <li>all BoundSheet records</li>
   *   <li>SST record</li>
   *   <li>DateWindow1904 record</li>
   * </ul>
   *
   * @param sheetName target sheet we want to read
   * @param extractHeader if set to true, will use the first row values as column names
   * @param handleMergeCells if set to true, will take into account merged cells
   *
   * @throws SheetNotFoundException couldn't find the target sheet
   */
  private void init(final String sheetName, boolean extractHeader, boolean handleMergeCells)
          throws SheetNotFoundException {
    if (handleMergeCells) {
      mergeCells = Maps.newHashMap();
    }

    processWorkbook(sheetName);
    missingRowBlock = !processSheet();

    if (extractHeader && !missingRowBlock) {
      parseHeaders();
    }
  }

  /**
   * parses all records between BOF and EOF records.<br>
   * Finds the sheet index of the target sheet and extracts global information needed to process
   * all cells of the target sheet.
   *
   * @param sheetName target sheet name
   */
  private void processWorkbook(final String sheetName) throws SheetNotFoundException {

    List<BoundSheetRecord> boundSheets = null;
    boolean handlingBoundSheets = false;
    int sheetIndex = -1;

    // when sheetName is null, or empty, read the first sheet in the workbook
    if (sheetName == null || sheetName.isEmpty()) {
      sheetIndex = 0;
    }

    Record r = recordStream.nextRecord();
    Preconditions.checkState(r != null && r.getSid() == BOFRecord.sid, "improper start of workbook: " + r);

    while (true) {
      r = Preconditions.checkNotNull(recordStream.nextRecord(), "workbook stream ended abruptly");
      final short sid = r.getSid();
      if (sid == EOFRecord.sid) {
        break;
      }

      // properly handle a BoundSheet block so we can figure out the index of the target sheet
      // a BoundSheet block is a set of BoundSheet records that are all contiguous
      if (sheetName != null) {
        if (sid == BoundSheetRecord.sid) {
          if (!handlingBoundSheets) { // start of a BoundSheet block
            assert boundSheets == null : "Found more than a single BoundSheets block";
            boundSheets = Lists.newArrayList();
            handlingBoundSheets = true;
          }

          boundSheets.add((BoundSheetRecord) r);
        } else if (handlingBoundSheets) { // end of a bound sheets block
          handlingBoundSheets = false;

          // order found bound sheets by their Bof position
          final BoundSheetRecord[] orderedBSRs = BoundSheetRecord.orderByBofPosition(boundSheets);

          // compute the index of the target sheet
          int idx = 0;
          for (BoundSheetRecord bsr : orderedBSRs) {
            if (bsr.getSheetname().equalsIgnoreCase(sheetName)) {
              sheetIndex = idx;
              break;
            }

            idx++;
          }

          if (sheetIndex == -1) {
            throw new SheetNotFoundException();
          }
        }
      }

      if (sid == SSTRecord.sid) {
        sstRecord = (SSTRecord) r;
      }
      if (sid == DateWindow1904Record.sid) {
        dateWindow1904 = ((DateWindow1904Record)r).getWindowing() == 1;
      }

      formatManager.processRecordInternally(r);
    }

    moveToStartOfSheet(sheetIndex);
  }

  /**
   * moves right after the BOF record of the target sheet
   *
   * @param sheetIndex index of target sheet
   */
  private void moveToStartOfSheet(int sheetIndex) {
    int currentSheet = -1;
    while(currentSheet < sheetIndex) {
      final Record r = Preconditions.checkNotNull(recordStream.nextRecord(), "stream ended abruptly");
      final short sid = r.getSid();

      if (sid == BOFRecord.sid) { // start of a stream
        BOFRecord bof = (BOFRecord) r;
        if (bof.getType() != BOFRecord.TYPE_WORKSHEET) {
          continue; // ignored stream
        }

        currentSheet++;
      }
    }
  }

  /**
   * Processes target sheet using {@link RowRecordsAggregate} to extract all non empty cells.
   *
   * @return false if the sheet is missing a proper 'row block'
   */
  private boolean processSheet() {
    boolean inRowBlock = false;
    RowBlock rowBlock = null;
    RowRecordsAggregate rowsAggregate = null;

    while(true) {
      final Record r = Preconditions.checkNotNull(recordStream.nextRecord(), "sheet ended abruptly");
      final short sid = r.getSid();
      if (sid == EOFRecord.sid) {
        break; // end of target sheet
      }

      // a row block is a contiguous suite of specific records that contain cell values along with
      // row specific data (like row formatting)
      if (inRowBlock) { // found another row related record
        if (!RowBlock.isEndOfRowBlock(sid)) {
          rowBlock.process(r);
        } else { // we just exited the row block
          assert rowsAggregate == null : "Row block encountered more than once";
          inRowBlock = false;
          if (mergeCells != null) { // a row block may contain "loose" merge cells
            for (MergeCellsRecord mcr : rowBlock.getMergeCellRecords()) {
              processMergeCellRecord(mcr);
            }
          }
          rowsAggregate = rowBlock.getRowRecordsAggregate();
        }
      } else if (RowBlock.isRowBlockRecord(sid)) { // we just entered the row block
        inRowBlock = true;
        rowBlock = new RowBlock();
      }

      // process any merge cells found outside the row block
      if (!inRowBlock && sid == MergeCellsRecord.sid && mergeCells != null) {
        processMergeCellRecord((MergeCellsRecord) r);
      }
    }

    if (rowsAggregate == null) {
      return false;
    }

    if (mergeCells != null && mergeCells.isEmpty()) {
      // sheet didn't contain any merge cells
      mergeCells = null;
    }

    // WARNING rowsAggregate populates a cell matrix with all cells of the sheet
    cellValueIterator = rowsAggregate.getCellValueIterator();

    return true;
  }

  /**
   * Stores all {@link CellRangeAddress} areas found in the record in mergeCells map
   *
   * @param mcr MergeCellsRecord
   */
  private void processMergeCellRecord(final MergeCellsRecord mcr) {
    int nRegions = mcr.getNumAreas();
    for (int i = 0; i < nRegions; i++) {
      final CellRangeAddress area = mcr.getAreaAt(i);
      int cellId = computeCellId(area.getFirstRow(), area.getFirstColumn());
      MergedCell prev = mergeCells.put(cellId, new MergedCell(area));
      assert prev == null : "two merge cells share the same top/left cell";
    }

  }

  /**
   * @return row * 256 + col
   */
  private static int computeCellId(int row, int col) {
    assert col < 256 : "Excel column index should never exceed 255";
    return row * 256 + col;
  }

  /**
   * extracts column name from first row, if any.<br>
   * If first row is empty will use default Excel notation: A, B, C, ...
   */
  private void parseHeaders() {
    CellValueRecordInterface cell = getNextCell();

    while (cell != null && cell.getRow() == 0) {
      final int colIndex = cell.getColumn();
      final Object value = resolveCellValue(cell);
      if (value != null) {
        columnNameHandler.setColumnName(colIndex, value.toString());
      }

      cell = getNextCell();
    }

    // if we have a single row in the file, cell will be null
    nextCell = cell;
    lastRow = 0;
  }

  /**
   * @return next cell or null if we reached end of the sheet
   */
  private CellValueRecordInterface getNextCell() {
    return cellValueIterator.hasNext() ? cellValueIterator.next() : null;
  }

  /**
   * writer next record into the StructWriter.
   *
   * @return State.END_OF_STREAM when no more record were found, State.READ_SUCCESSFUL otherwise
   */
  @Override
  public State parseNextRecord() throws Exception {
    if (missingRowBlock) {
      return State.END_OF_STREAM;
    }
    if (nextCell == null && !cellValueIterator.hasNext()) {
      return State.END_OF_STREAM;
    }

    int curRow = lastRow + 1;
    if (nextCell == null) { // we just started processing the first row
      nextCell = cellValueIterator.next();
    }

    writer.start();
    try {
      // get all cells for current row
      while (nextCell != null && nextCell.getRow() == curRow) {

        final Object value = resolveCellValue(nextCell);
        final int column = nextCell.getColumn();
        final int colId = computeCellId(curRow, column);
        final MergedCell mergedCell = mergeCells == null ? null : mergeCells.get(colId);

        if (mergedCell != null) {
            mergedCell.setValue(value);
            // don't write the cell value in the writer now, it will be done by handleMergeCells()
          } else {
            writeValue(column, value);
        }

        nextCell = getNextCell();
      }

      if (mergeCells != null) {
        handleMergeCells(curRow);
      }
    } finally {
      writer.end();
    }

    lastRow = curRow;
    return State.READ_SUCCESSFUL;
  }

  /**
   * add any merged cells to the current row
   *
   * @param row current processed row
   */
  private void handleMergeCells(int row) {
    for (MergedCell range : mergeCells.values()) {
      if (range.containsRow(row) && range.hasValue()) {
        range.writeValues();
      }
    }
  }

  /**
   * @param cell current cell
   *
   * @return cell's value. Could be Boolean, String, or Double.
   *         returns Null if cell is blank
   */
  private Object resolveCellValue(CellValueRecordInterface cell) {
    if (cell instanceof FormulaRecordAggregate) {
      FormulaRecordAggregate fra = (FormulaRecordAggregate) cell;
      FormulaRecord fr = fra.getFormulaRecord();
      CellType type;
      try {
        type = CellType.forInt(fr.getCachedResultType());
      } catch (IllegalArgumentException e) {
        throw UserException.dataReadError(e)
          .message("Unexpected formula result type at cell (%d, %d)", cell.getRow(), cell.getColumn())
          .build(logger);
      }

      switch (type) {
        case BOOLEAN:
          return fr.getCachedBooleanValue();
        case STRING:
          return fra.getStringValue();
        case NUMERIC:
          return fr.getValue();
        case ERROR:
          return FormulaError.forInt(fr.getCachedErrorValue()).getString();
        default:
          throw UserException.dataReadError()
                  .message("Unexpected formula result type at cell (%d, %d)", cell.getRow(), cell.getColumn())
                  .build(logger);
      }
    } else {
      int sid = ((Record) cell).getSid();
      switch (sid) {
        case BlankRecord.sid:
          return null; // blank cell
        case BoolErrRecord.sid: {
          final BoolErrRecord brec = (BoolErrRecord) cell;
          if (brec.isBoolean()) {
            return brec.getBooleanValue();
          } else {
            return FormulaError.forInt(brec.getErrorValue()).getString();
          }
        }
        case LabelSSTRecord.sid: {
          final LabelSSTRecord lrec = (LabelSSTRecord) cell;
          final int sstIndex = lrec.getSSTIndex();
          return sstRecord.getString(sstIndex).toString();
        }
        case LabelRecord.sid: {
          final LabelRecord lrec = (LabelRecord) cell;
          return lrec.getValue();
        }
        case NumberRecord.sid: {
          final NumberRecord nrec = (NumberRecord) cell;
          final double value = nrec.getValue();
          if (formatManager.isDateFormat(cell)) {
            return DateUtil.getJavaDate(value, dateWindow1904, LocaleUtil.TIMEZONE_UTC).getTime();
          } else {
            return value;
          }
        }
      }
      throw UserException.dataReadError()
              .message("Unexpected cell record at (%d, %d)", cell.getRow(), cell.getColumn())
              .build(logger);
    }
  }

  /**
   * writes given value into the specified column vector
   */
  private void writeValue(final int column, final Object value) {
    final String columnName = columnNameHandler.getColumnName(column);

    /* Project push-down. Nothing to do if this column is not
     * in the set of columns to be projected.
     */
    if (skipQuery || (columnsToProject != null && !columnsToProject.contains((columnName)))) {
      return;
    }

    if (value != null) { // make sure we ignore blank cells
      if (value instanceof Boolean) {
        writeBoolean(columnName, (boolean) value);
      } else if (value instanceof String) {
        writeVarChar(columnName, (String) value);
      } else if (value instanceof Long) {
        writeTimestamp(columnName, (long) value);
      } else {
        writeFloat8(columnName, (double) value);
      }
    }
  }

  private void writeBoolean(final String columnName, boolean value) {
    writer.bit(columnName).writeBit(value ? 1:0);
  }

  private void writeVarChar(final String columnName, final String value) {
    final byte[] b = value.getBytes(Charsets.UTF_8);
    FieldSizeLimitExceptionHelper.checkSizeLimit(b.length, maxCellSize, columnName, logger);

    managedBuf = managedBuf.reallocIfNeeded(b.length);
    managedBuf.setBytes(0, b);
    writer.varChar(columnName).writeVarChar(0, b.length, managedBuf);
  }

  private void writeTimestamp(final String columnName, final long dateMillis) {
    writer.timeStampMilli(columnName).writeTimeStampMilli(dateMillis);
  }

  private void writeFloat8(final String columnName, final double value) {
    writer.float8(columnName).writeFloat8(value);
  }

  /**
   * Inner class used to store a merged cell along with it's top/left cell's value.
   */
  private class MergedCell {
    private final CellRangeAddress range;
    private Object value;

    MergedCell(CellRangeAddress range) {
      this.range = range;
    }

    boolean containsRow(int row) {
      return row >= range.getFirstRow() && row <= range.getLastRow();
    }

    boolean hasValue() {
      return value != null;
    }

    void setValue(Object value) {
      assert this.value == null : "setting value of a MergedCell more than once";
      this.value = value;
    }

    void writeValues() {
      for (int col = range.getFirstColumn(); col <= range.getLastColumn(); col++) {
        final String columnName = columnNameHandler.getColumnName(col);
        if (!skipQuery && (columnsToProject == null || columnsToProject.contains((columnName)))) {
          XlsRecordProcessor.this.writeValue(col, value);
        }
      }
    }
  }

  @Override
  public void close() throws Exception {
    AutoCloseables.close(workbookStream);
  }
}
