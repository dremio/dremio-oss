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
package com.dremio.exec.store.easy.excel;

import static com.google.common.base.Preconditions.checkNotNull;
import static javax.xml.stream.XMLStreamConstants.START_ELEMENT;

import java.io.InputStream;

import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamReader;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;

public class ExcelUtil {
  /**
   * Constants for attribute, element names.
   */
  public static final String SHEET_DATA = "sheetData";
  public static final String ROW = "row";
  public static final String CELL = "c";
  public static final String VALUE = "v";
  public static final String SST_STRING = "s";
  public static final String MERGE_CELLS = "mergeCells";
  public static final String MERGE_CELL = "mergeCell";
  public static final String MERGE_CELL_REF = "ref";
  public static final String SHEET = "sheet";
  public static final String SHEET_NAME = "name";
  public static final String SHEET_ID = "id";
  public static final String SHEET_ID_NS = "http://schemas.openxmlformats.org/officeDocument/2006/relationships";
  public static final String TYPE = "t";
  public static final String STYLE = "s";
  public static final String CELL_REF = "r";

  public static final XMLInputFactory XML_INPUT_FACTORY = XMLInputFactory.newInstance();

  /**
   * Helper method to get the column name from cell reference. Ex. cell reference 'AB345' => column name is 'AB'.
   * @param cellRef Value of attribute cellRef {@link #CELL_REF} on cell.
   * @return
   */
  static String getColumnName(final String cellRef) {
    Preconditions.checkArgument(!Strings.isNullOrEmpty(cellRef), "Expected a non-empty cell reference value");
    int loc = getRowNumberStartIndex(cellRef);

    return cellRef.substring(0, loc);
  }

  /**
   * Helper method which returns the row start index in given cell reference value.
   * @param cellRef Value of attribute cellRef {@link #CELL_REF} on cell.
   * @return
   */
  public static int getRowNumberStartIndex(final String cellRef) {
    final int length = cellRef.length();
    // step over column name chars until first digit for row number.
    for (int loc = 0; loc < length; loc++) {
      char ch = cellRef.charAt(loc);
      if (Character.isDigit(ch)) {
        return loc;
      }
    }

    return -1;
  }

  /**
   * Helper method to read the "workbook.xml" in the document and find out the sheet id for
   * given sheet name, or the first sheet if sheetName is null or empty.
   * @param workbook InputStream of the workbook. Caller is responsible for closing it.
   * @param sheetName Sheet name to lookup.
   * @return Non-null value if found a sheet with given name. Otherwise null
   * @throws XMLStreamException
   */
  public static String getSheetId(final InputStream workbook, final String sheetName) throws XMLStreamException {
    checkNotNull(workbook);

    final XMLStreamReader wbReader  = XML_INPUT_FACTORY.createXMLStreamReader(workbook);
    final boolean retrieveFirstSheet = sheetName == null || sheetName.isEmpty();

    while (wbReader.hasNext()) {
      wbReader.next();
      final int event = wbReader.getEventType();
      switch (event) {
        case START_ELEMENT: {
          final String name = wbReader.getLocalName();
          if (SHEET.equals(name)) {
            String sheet = wbReader.getAttributeValue(/*namespaceURI=*/null, SHEET_NAME);
            // Sheet names are case insensitive
            if (retrieveFirstSheet || sheetName.equalsIgnoreCase(sheet)) {
              return wbReader.getAttributeValue(SHEET_ID_NS, SHEET_ID);
            }
          }
          break;
        }
      }
    }

    return null;
  }
}
