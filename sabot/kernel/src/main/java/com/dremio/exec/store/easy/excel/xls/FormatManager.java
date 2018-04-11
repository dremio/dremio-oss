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
package com.dremio.exec.store.easy.excel.xls;

import org.apache.poi.hssf.eventusermodel.FormatTrackingHSSFListener;
import org.apache.poi.hssf.record.CellValueRecordInterface;
import org.apache.poi.ss.usermodel.DateUtil;

/**
 * Keeps track of all Format/ExtendedFormat records encountered and exposes
 * helper methods to identify the formatting of a given cell
 */
public class FormatManager extends FormatTrackingHSSFListener {

  public FormatManager() {
    super(null);
  }

  @Override
  protected int getNumberOfCustomFormats() {
    return super.getNumberOfCustomFormats();
  }

  @Override
  protected int getNumberOfExtendedFormats() {
    return super.getNumberOfExtendedFormats();
  }

  boolean isDateFormat(CellValueRecordInterface cell) {
    final int formatIndex = getFormatIndex(cell);
    return DateUtil.isADateFormat(formatIndex, getFormatString(formatIndex));
  }
}
