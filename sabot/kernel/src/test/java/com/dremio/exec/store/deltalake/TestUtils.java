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

package com.dremio.exec.store.deltalake;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.column.page.PageReadStore;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroup;
import org.apache.parquet.example.data.simple.convert.GroupRecordConverter;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.apache.parquet.io.ColumnIOFactory;
import org.apache.parquet.io.MessageColumnIO;
import org.apache.parquet.io.RecordReader;
import org.apache.parquet.schema.MessageType;
import org.junit.Test;

import com.dremio.exec.hadoop.HadoopFileSystem;
import com.dremio.io.file.FileSystem;
import com.dremio.io.file.Path;

//Has a makeshift checkpoint reader used for integration testing of DeltaLakeTable until a
//checkpoint reader is ready. To be removed when the parquet reader is finished as part of DX-26745

public class TestUtils {

  public static boolean groupPresent(Group x, String s) {
    try {
      x.getGroup(s, 0);
      return true;
    } catch (Exception e) {
      return false;
    }
  };

  public static DeltaLogSnapshot makeShiftCheckpointReader(FileSystem fs, Path filePath) throws IOException {
    org.apache.hadoop.fs.Path hadoopPath = new org.apache.hadoop.fs.Path(filePath.toURI());
    ParquetFileReader reader = ParquetFileReader.open(HadoopInputFile.fromPath(hadoopPath, new Configuration()));
    MessageType schema = reader.getFooter().getFileMetaData().getSchema();
    PageReadStore pages;

    List<SimpleGroup> simpleGroups = new ArrayList<>();

    while ((pages = reader.readNextRowGroup()) != null) {
      long rows = pages.getRowCount();
      MessageColumnIO columnIO = new ColumnIOFactory().getColumnIO(schema);
      RecordReader recordReader = columnIO.getRecordReader(pages, new GroupRecordConverter(schema));

      for (int i = 0; i < rows; i++) {
        SimpleGroup simpleGroup = (SimpleGroup) recordReader.read();
        simpleGroups.add(simpleGroup);
      }
    }

    List<Path> addedFiles = new ArrayList<>();
    List<String> optionalGroups = Arrays.asList("metaData", "commitInfo");

    int numFilesAdded = 0;
    int numOutputBytes = 0;
    int numberOfOuptutRows = 0;
    String schemaString = "";

    for(Group g : simpleGroups) {
      if(groupPresent(g, "metaData")) {
        Group r = g.getGroup("metaData", 0);
        schemaString = r.getString("schemaString", 0);
      } else if (groupPresent(g, "add")) {
        Group r = g.getGroup("add", 0);
        addedFiles.add(Path.of(r.getString("path", 0)));
        numFilesAdded ++;
        numOutputBytes += r.getLong("size", 0);
      } else if (groupPresent(g, "commitInfo")) {
        Group r = g.getGroup("commitInfo", 0).getGroup("operationMetrics", 0);
        numFilesAdded += r.getInteger("numFiles", 0);
        numOutputBytes += r.getInteger("numOutputBytes", 0);
        numberOfOuptutRows += r.getInteger("numOutputRows", 0);
      }
    }

    DeltaLogSnapshot snap = new DeltaLogSnapshot("UNKNOWN", numFilesAdded, numOutputBytes, numberOfOuptutRows, numFilesAdded, System.currentTimeMillis(), true);
    snap.setSchema(schemaString, new ArrayList<>());
    return snap;
  }

  @Test
  public void makeShiftCheckpointReaderTest() throws IOException {
    FileSystem fs = HadoopFileSystem.getLocal(new Configuration());
    String path = "src/test/resources/deltalake/covid_cases/_delta_log/00000000000000000010.checkpoint.parquet";
    Path p = Path.of(path);
    makeShiftCheckpointReader(fs, p);
  }
}
