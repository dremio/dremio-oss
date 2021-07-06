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

import static com.dremio.exec.store.iceberg.DataProcessorTestUtil.extractDataFilePath;
import static com.dremio.exec.store.iceberg.DataProcessorTestUtil.getBatchSchema;
import static com.dremio.exec.store.iceberg.DataProcessorTestUtil.getDataFileVec;
import static com.dremio.exec.store.iceberg.DataProcessorTestUtil.getDatafile;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.List;

import org.apache.arrow.vector.VarCharVector;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.dremio.BaseTestQuery;
import com.dremio.exec.record.VectorContainer;
import com.dremio.exec.server.SabotContext;
import com.dremio.sabot.exec.context.OperatorContext;
import com.dremio.sabot.exec.context.OperatorContextImpl;

public class TestPathGeneratingDatafileProcessor extends BaseTestQuery {
  private final int startIndex = 0;
  private final int maxOutputCount = 3;

  private VectorContainer outgoing;
  private VarCharVector dataFileVec;
  private PathGeneratingDatafileProcessor datafileProcessor;

  @Before
  public void initialisePathGenDatafileProcessor() throws Exception {
    outgoing = getOperatorContext().createOutputVectorContainer();
    outgoing.addSchema(getBatchSchema(DataProcessorTestUtil.DataProcessorType.DATAFILE_PATH_GEN));
    outgoing.buildSchema();
    dataFileVec = getDataFileVec(outgoing);
    datafileProcessor = new PathGeneratingDatafileProcessor();
    datafileProcessor.setup(null, outgoing);
  }

  @After
  public void close() throws Exception {
    outgoing.close();
    datafileProcessor.close();
  }

  @Test
  public void testPathGeneratingDataFileProcessorWithinMaxLimit() throws Exception {
    List<String> dataFilePaths = new ArrayList<>();
    dataFilePaths.add("/path/to/data-0.parquet");
    dataFilePaths.add("/path/to/data-1.parquet");
    dataFilePaths.add("/path/to/data-2.parquet");

    int totalRecords = 0, outputRecords;
    for (String datafilePath : dataFilePaths) {
      outputRecords = datafileProcessor.processDatafile(getDatafile(datafilePath, 1024l), startIndex + totalRecords, maxOutputCount - totalRecords);
      totalRecords += outputRecords;

      outputRecords = datafileProcessor.processDatafile(getDatafile(datafilePath, 1024l), startIndex + totalRecords, maxOutputCount - totalRecords);
      totalRecords += outputRecords;
      assertEquals(0, outputRecords); // Zero indicates: We are done with current file. ready for processing next
      datafileProcessor.closeDatafile();
    }

    String datafile0 = extractDataFilePath(dataFileVec, 0);
    String datafile2 = extractDataFilePath(dataFileVec, 2);
    assertEquals(datafile0, "/path/to/data-0.parquet");
    assertEquals(datafile2, "/path/to/data-2.parquet");
  }

  @Test
  public void testPathGeneratingDataFileProcessorOutOfMaxLimit() throws Exception {
    List<String> datafilePaths = new ArrayList<>();
    datafilePaths.add("/path/to/data-0.parquet");
    datafilePaths.add("/path/to/data-1.parquet");
    datafilePaths.add("/path/to/data-2.parquet");
    datafilePaths.add("/path/to/data-3.parquet");

    int totalRecords = 0, outputRecords;
    for (String datafilePath : datafilePaths) {
      outputRecords = datafileProcessor.processDatafile(getDatafile(datafilePath, 1024l), startIndex + totalRecords, maxOutputCount - totalRecords);
      totalRecords += outputRecords;
      outputRecords = datafileProcessor.processDatafile(getDatafile(datafilePath, 1024l), startIndex + totalRecords, maxOutputCount - totalRecords);
      totalRecords += outputRecords;
      assertEquals(0, outputRecords); // Zero indicates: We are done with current file. ready for processing next
      datafileProcessor.closeDatafile();
    }

    assertEquals(3, totalRecords);
    assertTrue(dataFileVec.isNull(3));
    assertEquals(dataFileVec.isSet(3), 0);
  }

  private OperatorContext getOperatorContext() {
    SabotContext sabotContext = getSabotContext();
    return new OperatorContextImpl(sabotContext.getConfig(), getAllocator(), sabotContext.getOptionManager(), 10);
  }
}
