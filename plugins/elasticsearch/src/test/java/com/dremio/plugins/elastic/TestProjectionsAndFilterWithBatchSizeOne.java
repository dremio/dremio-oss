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
package com.dremio.plugins.elastic;

import org.junit.After;
import org.junit.Before;

import com.dremio.exec.ExecConstants;

/**
 * Runs the same set of tests as TestAggregationsAndFilter, but with batch size set to 1 for the entire test.
 * This is important to make sure that batch size is handled properly in elasticsearch readers and that when next()
 * is called after the first iteration returns data properly, especially in ElasticsearchAggregatorReader.
 */
public class TestProjectionsAndFilterWithBatchSizeOne extends TestProjectionsAndFilter {
  @Override
  @Before
  public void loadTable() throws Exception {
    super.loadTable();
    setSessionOption(ExecConstants.TARGET_BATCH_RECORDS_MIN, "1");
    setSessionOption(ExecConstants.TARGET_BATCH_RECORDS_MAX, "1");
  }

  @After
  public void cleanUp() throws Exception {
    resetSessionOption(ExecConstants.TARGET_BATCH_RECORDS_MIN);
    resetSessionOption(ExecConstants.TARGET_BATCH_RECORDS_MAX);
  }
}
