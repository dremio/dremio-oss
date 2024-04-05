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
package com.dremio.exec.store.excel;

import com.dremio.BaseTestQuery;
import com.dremio.common.exceptions.UserRemoteException;
import com.dremio.common.util.TestTools;
import com.dremio.exec.proto.UserBitShared.DremioPBError.ErrorType;
import org.junit.Assert;
import org.junit.Test;

/** Tests for reading excel format files. */
public abstract class TestExcelFormatPluginBase extends BaseTestQuery {

  abstract ExcelTestHelper getHelper();

  String getExcelDir() {
    return TestTools.getWorkingPath() + "/src/test/resources/excel/";
  }

  @Test
  public void readingWithHeaderAndMergedCellsExpansion() throws Exception {
    testHelper("sheet 1", true, true);
  }

  @Test
  public void readingWithHeaderAndNoMergedCellsExpansion() throws Exception {
    testHelper("sheet 1", true, false);
  }

  @Test
  public void readingWithNoHeaderAndMergedCellsExpansion() throws Exception {
    testHelper("sheet 2", false, true);
  }

  @Test
  public void readingWithNoHeaderAndNoMergedCellsExpansion() throws Exception {
    testHelper("sheet 2", false, false);
  }

  @Test
  public void readingWithNoSheetNameExpansion() throws Exception {
    testHelper(null, true, true);
  }

  @Test
  public void readingWithEmptySheetNameExpansion() throws Exception {
    testHelper("", true, true);
  }

  private void testHelper(String sheetName, boolean header, boolean mergedCellExpansion)
      throws Exception {
    getHelper().test(testBuilder(), sheetName, header, mergedCellExpansion);
  }

  void testAndExpectUserException(
      final String query, ErrorType errorType, final String errorMessage) throws Exception {
    try {
      test(query);
      Assert.fail("Query should've failed");
    } catch (UserRemoteException uex) {
      Assert.assertEquals("Error Message: " + uex.getMessage(), errorType, uex.getErrorType());
      Assert.assertTrue(
          String.format(
              "Expected error message to contain: %s but was actually [%s].",
              errorMessage, uex.getMessage()),
          uex.getMessage().contains(errorMessage));
    }
  }
}
