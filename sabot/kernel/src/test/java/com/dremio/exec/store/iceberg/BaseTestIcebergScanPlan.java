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
package com.dremio.exec.store.iceberg;

import com.dremio.PlanTestBase;

public class BaseTestIcebergScanPlan extends PlanTestBase {

  protected static String dataFileScanWithRowCount(int rowCount) {
    return String.format("TableFunction.*DATA_FILE_SCAN.*rowcount = %d", rowCount);
  }

  protected static String dataFileScanWithFilter(String filterCol, String filterVal) {
    return String.format("TableFunction.*Filter on `%s`: equal\\(`%s`, %s\\)", filterCol, filterCol, filterVal);
  }

  protected static String deleteFileAggWithRowCount(int rowCount) {
    return String.format("IcebergDeleteFileAgg.*rowcount = %d", rowCount);
  }

  protected static String dataManifestScanWithRowCount(int rowCount) {
    return String.format("IcebergManifestScan.*manifestContent=\\[DATA\\].*rowcount = %d", rowCount);
  }

  protected static String dataManifestScanWithRowCountAndFilter(int rowCount, String filterCol, String filterVal) {
    return String.format("IcebergManifestScan.*" +
        "ManifestFile Filter.*ref\\(name=\"%s\"\\) == %s\\).*" +
        "manifestContent=\\[DATA\\].*rowcount = %d", filterCol, filterVal, rowCount);
  }

  protected static String dataManifestList() {
    return String.format("IcebergManifestList.*manifestContent=\\[DATA\\]");
  }

  protected static String dataManifestListWithFilter(String filterCol, String filterVal) {
    return String.format("IcebergManifestList.*" +
        "ManifestList Filter.*ref\\(name=\"%s\"\\) == %s\\).*" +
        "manifestContent=\\[DATA\\]", filterCol, filterVal);
  }

  protected static String deleteManifestScanWithRowCount(int rowCount) {
    return String.format("IcebergManifestScan.*manifestContent=\\[DELETES\\].*rowcount = %d", rowCount);
  }

  protected static String deleteManifestScanWithRowCountAndFilter(int rowCount, String filterCol, String filterVal) {
    return String.format("IcebergManifestScan.*" +
        "ManifestFile Filter.*ref\\(name=\"%s\"\\) == %s\\).*" +
        "manifestContent=\\[DELETES\\].*rowcount = %d", filterCol, filterVal, rowCount);
  }

  protected static String deleteManifestList() {
    return "IcebergManifestList.*manifestContent=\\[DELETES\\]";
  }

  protected static String deleteManifestListWithFilter(String filterCol, String filterVal) {
    return String.format("IcebergManifestList.*" +
        "ManifestList Filter.*ref\\(name=\"%s\"\\) == %s\\).*" +
        "manifestContent=\\[DELETES\\]", filterCol, filterVal);
  }

  protected static String splitGenManifestScanWithRowCount(int rowCount) {
    return String.format("TableFunction.*SPLIT_GEN_MANIFEST_SCAN.*rowcount = %d", rowCount);
  }

  protected static String deleteManifestContent() {
    return "manifestContent=\\[DELETES\\]";
  }
}
