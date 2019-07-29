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
package com.dremio.dac.resource;

import static org.junit.Assert.assertEquals;

import java.util.Arrays;

import javax.ws.rs.client.Entity;
import javax.ws.rs.client.Invocation;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import com.dremio.dac.explore.model.DatasetPath;
import com.dremio.dac.explore.model.DatasetUI;
import com.dremio.dac.explore.model.InitialDataPreviewResponse;
import com.dremio.dac.model.spaces.Space;
import com.dremio.dac.server.ApiErrorModel;
import com.dremio.dac.server.BaseTestServer;
import com.dremio.dac.server.FamilyExpectation;

/**
 * Tests to verify handling of dataset dependecy changes
 */
@RunWith(Parameterized.class)
public class TestDatasetDependencyChanges extends BaseTestServer {

  private static final int EXPECTED_COLUMN_COUNT = 50;
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(TestDatasetDependencyChanges.class);

  private final String datasetDefOne;
  private final String datasetDefTwo;
  private final String datasetDefThree;
  private final String datasetDefRedefined;
  private final int datasetToChange;
  private final boolean expectError;

  public TestDatasetDependencyChanges(String datasetDefOne, String datasetDefTwo,
                                      String datasetDefThree, String datasetDefRedefined,
                                      int datasetToChange, boolean expectError) {
    this.datasetDefOne = datasetDefOne;
    this.datasetDefTwo = datasetDefTwo;
    this.datasetDefThree = datasetDefThree;
    this.datasetDefRedefined = datasetDefRedefined;
    this.datasetToChange = datasetToChange;
    this.expectError = expectError;
  }

  @Before
  public void setup() throws Exception {
    clearAllDataExceptUser();
  }

  @Parameterized.Parameters(name = "{index}: {0}")
  public static Iterable<Object[]> data() {
    return Arrays.asList(
      // Decrease ds1 field list   *** Reproduces index issue
      new Object[] {
        "select s.s_name, s_phone, s_suppkey from cp.\"tpch/supplier.parquet\" s",
        "select * from spaceCreateDataset.ds1 ds1",
        "select * from spaceCreateDataset.ds2 ds2",
        "select s.s_name, s_phone from cp.\"tpch/supplier.parquet\" s",
        1,
        false},
      // Increase ds1 field list
      new Object[] {
        "select s.s_name, s_phone, s_suppkey from cp.\"tpch/supplier.parquet\" s",
        "select * from spaceCreateDataset.ds1 ds1",
        "select * from spaceCreateDataset.ds2 ds2",
        "select s.s_name, s_phone, s_suppkey, s_comment from cp.\"tpch/supplier.parquet\" s",
        1,
        false},
      // Use * in ds1
      new Object[] {
        "select s.s_name, s_phone, s_suppkey from cp.\"tpch/supplier.parquet\" s",
        "select * from spaceCreateDataset.ds1 ds1",
        "select * from spaceCreateDataset.ds2 ds2",
        "select s.s_name, s_phone, s_suppkey, s.* from cp.\"tpch/supplier.parquet\" s",
        1,
        false},
      // Remove * in ds1 - decreasing field list  *** reproduces index issue
      new Object[] {
        "select * from cp.\"tpch/supplier.parquet\" s",
        "select * from spaceCreateDataset.ds1 ds1",
        "select * from spaceCreateDataset.ds2 ds2",
        "select s.s_name, s_phone, s_suppkey from cp.\"tpch/supplier.parquet\" s",
        1,
        false},
      // Decrease ds1 field list - No * in ds2
      new Object[] {
        "select s.s_name, s_phone, s_suppkey from cp.\"tpch/supplier.parquet\" s",
        "select ds1.s_name, ds1.s_phone from spaceCreateDataset.ds1 ds1",
        "select * from spaceCreateDataset.ds2 ds2",
        "select s.s_name, s_phone from cp.\"tpch/supplier.parquet\" s",
        1,
        false},
      // Increase ds1 field list - No * in ds2
      new Object[] {
        "select s.s_name, s_phone, s_suppkey from cp.\"tpch/supplier.parquet\" s",
        "select ds1.s_name, ds1.s_phone from spaceCreateDataset.ds1 ds1",
        "select * from spaceCreateDataset.ds2 ds2",
        "select s.s_name, s_phone, s_suppkey, s_comment from cp.\"tpch/supplier.parquet\" s",
        1,
        false},
      // Use * in ds1 - No * in ds2
      new Object[] {
        "select s.s_name, s_phone, s_suppkey from cp.\"tpch/supplier.parquet\" s",
        "select ds1.s_name, ds1.s_phone from spaceCreateDataset.ds1 ds1",
        "select * from spaceCreateDataset.ds2 ds2",
        "select * from cp.\"tpch/supplier.parquet\" s",
        1,
        false},
      // Remove * in ds1 - No * in ds2
      new Object[] {
        "select * from cp.\"tpch/supplier.parquet\" s",
        "select ds1.s_name, ds1.s_phone from spaceCreateDataset.ds1 ds1",
        "select * from spaceCreateDataset.ds2 ds2",
        "select s.s_name, s_phone, s_suppkey from cp.\"tpch/supplier.parquet\" s",
        1,
        false},
      // Decrease ds2 field list - no * in ds2
      new Object[] {
        "select * from cp.\"tpch/supplier.parquet\" s",
        "select ds1.s_suppkey, ds1.s_name from spaceCreateDataset.ds1 ds1",
        "select * from spaceCreateDataset.ds2 ds2",
        "select ds1.s_suppkey from spaceCreateDataset.ds1 ds1",
        2,
        false},
      // Increase ds2 field list
      new Object[] {
        "select s_suppkey, s_name from cp.\"tpch/supplier.parquet\" s",
        "select s_suppkey from spaceCreateDataset.ds1 ds1",
        "select * from spaceCreateDataset.ds2 ds2",
        "select s_suppkey, s_name from spaceCreateDataset.ds1 ds1",
        2,
        false},
      // Use * in ds2 - increasing field list
      new Object[] {
        "select s_suppkey, s_name from cp.\"tpch/supplier.parquet\" s",
        "select s_suppkey from spaceCreateDataset.ds1 ds1",
        "select * from spaceCreateDataset.ds2 ds2",
        "select * from spaceCreateDataset.ds1 ds1",
        2,
        false},
      // Remove * in ds2 - decreasing field list
      new Object[] {
        "select * from cp.\"tpch/supplier.parquet\" s",
        "select * from spaceCreateDataset.ds1 ds1",
        "select * from spaceCreateDataset.ds2 ds2",
        "select s_suppkey from spaceCreateDataset.ds1 ds1",
        2,
        false},
      // Decrease ds2 field list - No * in ds1
      new Object[] {
        "select s.s_name, s_phone, s_suppkey from cp.\"tpch/supplier.parquet\" s",
        "select ds1.s_name, ds1.s_phone from spaceCreateDataset.ds1 ds1",
        "select * from spaceCreateDataset.ds2 ds2",
        "select ds1.s_name from spaceCreateDataset.ds1 ds1",
        2,
        false},
      // Increase ds2 field list - No * in ds1
      new Object[] {
        "select s.s_name, s_phone, s_suppkey from cp.\"tpch/supplier.parquet\" s",
        "select ds1.s_name, ds1.s_phone from spaceCreateDataset.ds1 ds1",
        "select * from spaceCreateDataset.ds2 ds2",
        "select ds1.s_name, ds1.s_phone, 'stringLiteral' from spaceCreateDataset.ds1 ds1",
        2,
        false}
    );
  }

  @Test
  public void testDatasetDependencyChange() {
    // Create initial dataset
    expectSuccess(getBuilder(getAPIv2().path("space/spaceCreateDataset"))
      .buildPut(Entity.json(new Space(null, "spaceCreateDataset", null, null, null, 0, null))), Space.class);
    DatasetUI ds1 = createDatasetFromSQLAndSave(new DatasetPath("spaceCreateDataset.ds1"),
      datasetDefOne, Arrays.asList("cp"));

    // Create second dataset dependent on the previous dataset
    DatasetUI ds2 = createDatasetFromSQLAndSave(new DatasetPath("spaceCreateDataset.ds2"),
      datasetDefTwo, Arrays.asList("cp"));

    // Create third dataset dependent on the previous dataset
    DatasetUI ds3 = createDatasetFromSQLAndSave(new DatasetPath("spaceCreateDataset.ds3"),
      datasetDefThree, Arrays.asList("cp"));

    // Rename the dependent dataset
    rename(getDatasetPath(datasetToChange == 1 ? ds1 : ds2), "old_dataset");

    // Recreate dataset with a different schema
    createDatasetFromSQLAndSave(new DatasetPath(String.format("spaceCreateDataset.ds%d",datasetToChange)),
      datasetDefRedefined, Arrays.asList("cp"));

    // Preview ds3 with changed dependency
    if (expectError) {
      final Invocation invocation =
        getBuilder(getAPIv2().path("dataset/" + getDatasetPath(ds3).toPathString() + "/preview")).buildGet();

      ApiErrorModel errorMessage =
        expectError(FamilyExpectation.CLIENT_ERROR, invocation,  ApiErrorModel.class);

      // Log the received error message
      logger.debug(errorMessage.toString());

      // Confirm that a human-readable error message is returned.
      assertEquals(
        "Definition of this dataset is out of date",
        errorMessage.getErrorMessage().split("\\.")[0]);
    } else {
      InitialDataPreviewResponse resp  = getPreview(getDatasetPath(ds3));
      assertEquals(EXPECTED_COLUMN_COUNT, resp.getData().getReturnedRowCount());
    }
  }
}
