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
package com.dremio.exec.store.easy.excel;

import static com.google.common.base.Preconditions.checkNotNull;
import static javax.xml.stream.XMLStreamConstants.CHARACTERS;
import static javax.xml.stream.XMLStreamConstants.END_ELEMENT;
import static javax.xml.stream.XMLStreamConstants.START_ELEMENT;

import java.io.IOException;
import java.io.InputStream;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamReader;

import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.vector.complex.impl.VectorContainerWriter;
import org.apache.arrow.vector.complex.writer.BaseWriter.StructWriter;
import org.apache.poi.openxml4j.opc.OPCPackage;
import org.apache.poi.ss.usermodel.DateUtil;
import org.apache.poi.ss.util.CellReference;
import org.apache.poi.util.LocaleUtil;
import org.apache.poi.xssf.eventusermodel.XSSFReader;
import org.apache.poi.xssf.model.SharedStringsTable;
import org.apache.poi.xssf.model.StylesTable;
import org.apache.poi.xssf.usermodel.XSSFRichTextString;
import org.openxmlformats.schemas.spreadsheetml.x2006.main.CTXf;

import com.dremio.common.AutoCloseables;
import com.dremio.common.exceptions.FieldSizeLimitExceptionHelper;
import com.dremio.common.exceptions.UserException;
import com.dremio.common.types.TypeProtos.MinorType;
import com.google.common.base.Charsets;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Ordering;

/**
 * Implements cursor based streaming XML parser for reading an excel sheet.
 */
public class StAXBasedParser implements ExcelParser {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(StAXBasedParser.class);

  private static final Map<String, MinorType> TYPE_MAP = ImmutableMap.<String, MinorType>builder()
      .put("b", MinorType.BIT)    // t="b" - boolean value
      .put("e", MinorType.VARCHAR) // t="e" - error value, consider as VARCHAR
      .put("s", MinorType.VARCHAR) // t="s" - value is a string and it is shared strings table
      .put("str", MinorType.VARCHAR) // t="str" - formula, consider as the evaluated formula value as VARCHAR
      .put("inlineStr", MinorType.VARCHAR) // t="inlineStr"
      .put("n", MinorType.FLOAT8) // t="n" - consider number as double.
      .build();

  private static final Ordering<MergeCellRegion> MERGE_CELL_REGION_ORDERING =
      Ordering.from(new Comparator<MergeCellRegion>() {
        @Override
        public int compare(MergeCellRegion o1, MergeCellRegion o2) {
          // Sort in descending order of affinity values
          return Integer.compare(o1.rowEnd, o2.rowEnd);
        }
      });

  private final SharedStringsTable sst;
  private final StylesTable styles;
  private final InputStream sheetInputStream;
  private final StructWriter writer;

  private ArrowBuf managedBuf;
  private XMLStreamReader xmlStreamReader;

  private boolean emptySheet = true;

  private long runningRecordCount = 1;

  private String currentCellRef;
  private int currentColumnIndex;
  private boolean lookupNextValueInSST;
  private MinorType valueTypeFromAttribute;
  private MinorType valueTypeFromStyle;
  private boolean skipQuery;
  private final int maxCellSize;

  private Map<String, MinorType> styleToTypeCache = Maps.newHashMap();
  private Map<Integer, MinorType> indexToLastTypeCache = Maps.newHashMap();

  private final OPCPackage pkgInputStream;

  private final ColumnNameHandler columnNameHandler = new ColumnNameHandler();

  /* lookup table to find if a particular column is to be projected or not */
  private final HashSet<String> columnsToProject;

  /**
   * Merge cell map. Key is the top-left cell in the merged region. Value is null if there is no merge cell
   * expansion needed or no merge cells found.
   */
  private Map<String, MergeCellRegion> mergeCells;

  /**
   * List of merge cells sorted by row end for better look up.
   */
  private List<MergeCellRegion> mergeCellsSortedByRowEnd;

  /**
   * Create instance
   * @param inputStream sheet input stream
   * @param pluginConfig config options
   * @param writer {@link VectorContainerWriter} for writing values into vectors.
   * @param managedBuf Workspace buffer.
   * @param skipQuery if a query should skip columns
   * @param maxCellSize maximum allowable size of variable length cells
   */
  public StAXBasedParser(final InputStream inputStream, final ExcelFormatPluginConfig pluginConfig,
                         final VectorContainerWriter writer, final ArrowBuf managedBuf,
                         final HashSet<String> columnsToProject, final boolean skipQuery,
                         final int maxCellSize) throws Exception {
    this.pkgInputStream = OPCPackage.open(inputStream);
    this.writer = writer.rootAsStruct();
    this.managedBuf = managedBuf;
    this.columnsToProject = columnsToProject;
    this.skipQuery = skipQuery;
    this.maxCellSize = maxCellSize;

    final XSSFReader xssfReader = new XSSFReader(pkgInputStream);

    // Find the sheet id of the given sheet name in workbook
    try (final InputStream wbInputStream = xssfReader.getWorkbookData()) {
      final String sheetId = ExcelUtil.getSheetId(wbInputStream, pluginConfig.sheet);
      if (sheetId == null) {
        throw new SheetNotFoundException();
      }

      // Open the InputStream for sheet
      sheetInputStream = xssfReader.getSheet(sheetId);
    }


    // WARNING: XSSFReader can actually return null instances of sst and styles
    sst = xssfReader.getSharedStringsTable();
    styles = checkNotNull(xssfReader.getStylesTable(), "Expected a valid styles table instance");

    init(pluginConfig.extractHeader, pluginConfig.hasMergedCells);
  }

  /**
   * Initialize the parser based on given parameters.
   *
   * @param extractHeader Whether to consider the first row as header row.
   * @param hasMergedCells Does the sheet has any merged cells. Supporting merged cells is an expensive operation.
   *                       Indicating there are no merged cells, improves the read time.
   * @throws XMLStreamException
   * @throws IOException
   */
  private void init(final boolean extractHeader, final boolean hasMergedCells) throws XMLStreamException, IOException {
    if (hasMergedCells) {
      sheetInputStream.mark(-1);
      xmlStreamReader = ExcelUtil.XML_INPUT_FACTORY.createXMLStreamReader(sheetInputStream);
      parseMergedCellInfo();
      sheetInputStream.reset();
      xmlStreamReader.close();
    }


    xmlStreamReader = ExcelUtil.XML_INPUT_FACTORY.createXMLStreamReader(sheetInputStream);

    // parse until we encounter "sheetData" element
    while(xmlStreamReader.hasNext()) {
      if (xmlStreamReader.getEventType() == START_ELEMENT &&
          ExcelUtil.SHEET_DATA.equals(xmlStreamReader.getLocalName())) {
        emptySheet = false;

        if (extractHeader) {
          parseHeaders();
          runningRecordCount++;
        }
        break;
      }
      xmlStreamReader.next();
    }
  }

  /**
   * Helper method to parse the first row contents as column names. Assumes the current
   * cursor is set to start of the first row.
   * @throws XMLStreamException
   */
  private void parseHeaders() throws XMLStreamException {

    int columnIndex = -1;
    String columnNameFromHeaderRow;

    boolean inValue = false;
    boolean lookupValueInSST = false;
    boolean inInlineString = false;
    while (xmlStreamReader.hasNext()) {
      xmlStreamReader.next();
      final int event = xmlStreamReader.getEventType();
      switch (event) {
        case START_ELEMENT: {
          final String name = xmlStreamReader.getLocalName();
          if (ExcelUtil.VALUE.equals(name) ||
              (inInlineString && ExcelUtil.INLINE_STRING_TEXT.equals(name))) {
            inValue = true;
          } else if (ExcelUtil.CELL.equals(name)) {
            lookupValueInSST = ExcelUtil.SST_STRING.equals(xmlStreamReader.getAttributeValue(/*namespaceURI=*/null, ExcelUtil.TYPE));
            String cellRef = xmlStreamReader.getAttributeValue(/*namespaceURI=*/null, ExcelUtil.CELL_REF);
            String columnNameFromRef = ExcelUtil.getColumnName(cellRef);
            columnIndex = CellReference.convertColStringToIndex(columnNameFromRef);
          } else if (ExcelUtil.INLINE_STRING.equals(name)) {
            inInlineString = true;
          }
          break;
        }

        case CHARACTERS: {
          if (inValue) {
            columnNameFromHeaderRow = resolveValue(xmlStreamReader.getText(), lookupValueInSST);
            columnNameHandler.setColumnName(columnIndex, columnNameFromHeaderRow);
          }
          break;
        }

        case END_ELEMENT: {
          final String name = xmlStreamReader.getLocalName();
          if (ExcelUtil.VALUE.equals(name) ||
              (inInlineString && ExcelUtil.INLINE_STRING_TEXT.equals(name))) {
            inValue = false;
            lookupValueInSST = false;
          } else if (ExcelUtil.INLINE_STRING.equals(name)) {
            inInlineString = false;
          } else if (ExcelUtil.ROW.equals(name)) {
            return;
          }
          break;
        }
      }
    }
  }

  /**
   * Helper method which parses the merge cell boundaries and creates data structures for fast lookup.
   * @throws XMLStreamException
   */
  private void parseMergedCellInfo() throws XMLStreamException {
    mergeCells = Maps.newHashMap();

    while (xmlStreamReader.hasNext()) {
      xmlStreamReader.next();
      final int event = xmlStreamReader.getEventType();
      switch (event) {
        case START_ELEMENT: {
          final String name = xmlStreamReader.getLocalName();
          if (ExcelUtil.MERGE_CELL.equals(name)) {
            String[] corners = xmlStreamReader.getAttributeValue(/*namespaceURI=*/null, ExcelUtil.MERGE_CELL_REF).split(":");
            mergeCells.put(corners[0], MergeCellRegion.create(corners[0], corners[1]));
          }
          break;
        }

        case END_ELEMENT: {
          final String name = xmlStreamReader.getLocalName();
          if (ExcelUtil.MERGE_CELLS.equals(name)) {
            if (mergeCells.size() == 0) {
              mergeCells = null;
            } else {
              mergeCellsSortedByRowEnd =
                  Lists.newLinkedList(MERGE_CELL_REGION_ORDERING.sortedCopy(mergeCells.values()));
            }
            return;
          }
          break;
        }
      }
    }
  }

  /**
   * Read the next record in stream.
   * @return {@link State#READ_SUCCESSFUL} when a row is read. {@link State#END_OF_STREAM} when there are no more rows.
   * @throws XMLStreamException
   */
  @Override
  public State parseNextRecord() throws XMLStreamException {
    if (emptySheet) {
      return State.END_OF_STREAM;
    }

    writer.start();
    try {
      boolean inValue = false;
      boolean inInlineString = false;
      while (xmlStreamReader.hasNext()) {
        xmlStreamReader.next();
        final int event = xmlStreamReader.getEventType();
        switch (event) {
          case START_ELEMENT: {
            final String name = xmlStreamReader.getLocalName();
            if (ExcelUtil.VALUE.equals(name) ||
                (inInlineString && ExcelUtil.INLINE_STRING_TEXT.equals(name))) {
              inValue = true;
            } else if (ExcelUtil.CELL.equals(name)) {
              handleCellStart();
            } else if (ExcelUtil.INLINE_STRING.equals(name)) {
              inInlineString = true;
            }
            break;
          }

          case CHARACTERS: {
            if (inValue) {
                handleValue(xmlStreamReader.getText());
              }
            break;
          }

          case END_ELEMENT: {
            final String name = xmlStreamReader.getLocalName();
            if (ExcelUtil.VALUE.equals(name) ||
                (inInlineString && ExcelUtil.INLINE_STRING_TEXT.equals(name))) {
              currentColumnIndex = -1;
              lookupNextValueInSST = false;
              valueTypeFromAttribute = null;
              valueTypeFromStyle = null;
              inValue = false;
            } else if (ExcelUtil.INLINE_STRING.equals(name)) {
              inInlineString = false;
            } else if (-1 != currentColumnIndex && ExcelUtil.CELL.equals(name)) {
              ensureNull();
              currentColumnIndex = -1;
              lookupNextValueInSST = false;
              valueTypeFromAttribute = null;
              valueTypeFromStyle = null;
              inValue = false;
            } else if (ExcelUtil.ROW.equals(name)) {
              if (mergeCells != null) {
                handleMergedCells();
              }

              runningRecordCount++;
              return State.READ_SUCCESSFUL;
            }
            break;
          }
        }
      }
    } finally {
      writer.end();
    }

    return State.END_OF_STREAM;
  }

  /**
   * Helper method to determine the output types based on the explicit type given as attribute in "cell" element
   * and the type through "style" definition.
   * @param type Value of attribute type {@link ExcelUtil#TYPE} on cell.
   * @param style Value of attribute style {@link ExcelUtil#STYLE} on cell.
   */
  private void determineOutputTypes(String type, String style) {
    // Determine the type from attribute
    valueTypeFromAttribute = MinorType.FLOAT8;
    if (type != null) {
      if (TYPE_MAP.containsKey(type)) {
        valueTypeFromAttribute = TYPE_MAP.get(type);
      }
    }

    valueTypeFromStyle = MinorType.FLOAT8;
    if (style != null) {
      if (styleToTypeCache.containsKey(style)) {
        valueTypeFromStyle = styleToTypeCache.get(style);
        return;
      }

      int styleId = Integer.valueOf(style);
      CTXf styleDef = styles.getCellXfAt(styleId);
      if (styleDef != null) {
        long numFmtId = styleDef.getNumFmtId();
        String format = styles.getNumberFormatAt((short) numFmtId);

        if (DateUtil.isADateFormat((int) numFmtId, format)) {
          valueTypeFromStyle = MinorType.TIMESTAMP;
        }
      }
    }
    styleToTypeCache.put(style, valueTypeFromStyle);
  }

  /**
   * Helper method to handle start of the "cell" element. It reads required attributes and set the workspace
   * variables.
   */
  private void handleCellStart() {
    final String typeValue = xmlStreamReader.getAttributeValue(/*namespaceURI=*/null, ExcelUtil.TYPE);
    lookupNextValueInSST = ExcelUtil.SST_STRING.equals(typeValue);

    determineOutputTypes(
        typeValue,
        xmlStreamReader.getAttributeValue(/*namespaceURI=*/null, ExcelUtil.STYLE));
    currentCellRef = xmlStreamReader.getAttributeValue(/*namespaceURI=*/null, ExcelUtil.CELL_REF);
    String columnName = ExcelUtil.getColumnName(currentCellRef);
    currentColumnIndex = CellReference.convertColStringToIndex(columnName);
  }

  /**
   * Handle string value read in "value" element. Based on the workspace variable, determine its type and
   * set it in vector container.
   * @param value
   */
  private void handleValue(String value) {
    if (value.length() != 0) {
      value = resolveValue(value, lookupNextValueInSST);

      assert currentColumnIndex != -1 : "Invalid currentColumnIndex";

      final String finalColumnName = columnNameHandler.getColumnName(currentColumnIndex);

      final boolean projectedAndNotSkipQuery = !skipQuery && (columnsToProject == null || columnsToProject.contains(finalColumnName));

      final MergeCellRegion mergeCellRegion = mergeCells != null ? mergeCells.get(currentCellRef) : null;

      switch (valueTypeFromAttribute) {
        case BIT:
          final int toWrite = "1".equalsIgnoreCase(value) ? 1 : 0;
          indexToLastTypeCache.put(currentColumnIndex, valueTypeFromAttribute);
          if (projectedAndNotSkipQuery) {
            writer.bit(finalColumnName).writeBit(toWrite);
          }
          if (mergeCellRegion != null) {
            mergeCellRegion.setValue(MinorType.BIT, toWrite);
          }
          break;

        case FLOAT8:
          double dValue = Double.valueOf(value);
          if (valueTypeFromStyle == MinorType.TIMESTAMP && DateUtil.isValidExcelDate(dValue)) {
            indexToLastTypeCache.put(currentColumnIndex, MinorType.TIMESTAMP);
            final long dateMillis = DateUtil.getJavaDate(dValue, false, LocaleUtil.TIMEZONE_UTC).getTime();
            if (projectedAndNotSkipQuery) {
              writer.timeStampMilli(finalColumnName).writeTimeStampMilli(dateMillis);
            }
            if (mergeCellRegion != null) {
              mergeCellRegion.setValue(MinorType.TIMESTAMP, dateMillis);
            }
          } else {
            indexToLastTypeCache.put(currentColumnIndex, valueTypeFromAttribute);
            if (projectedAndNotSkipQuery) {
              writer.float8(finalColumnName).writeFloat8(dValue);
            }
            if (mergeCellRegion != null) {
              mergeCellRegion.setValue(MinorType.FLOAT8, dValue);
            }
          }
          break;

        case VARCHAR:
          indexToLastTypeCache.put(currentColumnIndex, valueTypeFromAttribute);
          final byte[] b = value.getBytes(Charsets.UTF_8);
          FieldSizeLimitExceptionHelper.checkSizeLimit(b.length, maxCellSize, currentColumnIndex, logger);

          managedBuf = managedBuf.reallocIfNeeded(b.length);
          managedBuf.setBytes(0, b);
          if (projectedAndNotSkipQuery) {
            writer.varChar(finalColumnName).writeVarChar(0, b.length, managedBuf);
          }
          if (mergeCellRegion != null) {
            mergeCellRegion.setValue(MinorType.VARCHAR, b);
          }
          break;

        default:
          throw UserException.unsupportedError()
            .message("Unknown type received %s", valueTypeFromAttribute)
            .build(logger);
      }
    }
  }

  /**
   * Ensure a null value is set for the writer by retrieving the writer for the column but not writing a value.
   */
  private void ensureNull() {
    assert currentColumnIndex != -1 : "Invalid currentColumnIndex";

    final String finalColumnName = columnNameHandler.getColumnName(currentColumnIndex);

    if ((columnsToProject == null) || columnsToProject.contains(finalColumnName)) {
      MinorType type = indexToLastTypeCache.get(currentColumnIndex);
      if (type == null) {
        if (valueTypeFromAttribute == MinorType.FLOAT8 && valueTypeFromStyle == MinorType.TIMESTAMP) {
          type = MinorType.TIMESTAMP;
        } else {
          type = valueTypeFromAttribute;
        }
      }

      switch (type) {
        case BIT:
          writer.bit(finalColumnName);
          break;

        case FLOAT8:
          writer.float8(finalColumnName);
          break;

        case TIMESTAMP:
          writer.timeMilli(finalColumnName);
          break;

        case VARCHAR:
          writer.varChar(finalColumnName);
          break;

        default:
          throw UserException.unsupportedError()
            .message("Unknown type received %s", valueTypeFromAttribute)
            .build(logger);
      }
    }
  }

  /**
   * Resolve the given value with respect to whether it is a reference to element in shared strings table.
   * Also decode the final value.
   *
   * @param value Value read in element.
   * @param lookupNextValueInSST Whether the value is an index into the shared strings table.
   * @return
   */
  private String resolveValue(String value, boolean lookupNextValueInSST) {
    if(lookupNextValueInSST) {
      int idx = (int)Double.parseDouble(value);
      return new XSSFRichTextString(sst.getEntryAt(idx)).toString();
    }

    return new XSSFRichTextString(value).toString();
  }

  /**
   * Populate the values for merged cells in current row. Also delete the merged regions that are not valid from
   * current row onwards.
   */
  private void handleMergedCells() {
    if (mergeCellsSortedByRowEnd != null) {
      Iterator<MergeCellRegion> iter = mergeCellsSortedByRowEnd.iterator();
      while (iter.hasNext()) {
        final MergeCellRegion mcr = iter.next();
        if (mcr.hasValue() && mcr.containsRow(runningRecordCount)) {
          for (int colIndex = mcr.colStart; colIndex <= mcr.colEnd; colIndex++) {
            String colName = columnNameHandler.getColumnName(colIndex);
            if (columnsToProject == null || columnsToProject.contains(colName)) {
              mcr.write(managedBuf, writer, colName);
            }
          }
        }

        if (mcr.rowEnd < runningRecordCount) {
          iter.remove();
        }
      }
    }
  }

  @Override
  public void close() throws Exception {
    AutoCloseables.close(pkgInputStream, sheetInputStream);
  }
}
