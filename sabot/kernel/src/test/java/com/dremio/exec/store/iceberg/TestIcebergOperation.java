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

import static org.junit.Assert.assertTrue;

import java.io.File;

import org.apache.hadoop.conf.Configuration;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import com.dremio.common.expression.CompleteType;
import com.dremio.exec.record.BatchSchema;
import com.dremio.io.file.Path;

public class TestIcebergOperation {
  @ClassRule
  public static final TemporaryFolder tempDir = new TemporaryFolder();

  @Test
  public void testCreateOperation() {
    // Instantiate iceberg operation, start it and store it in PhysicalPlan
    String tableName = "icebergtable";
    BatchSchema schema = BatchSchema.newBuilder().addField(CompleteType.INT.toField("int")).build();
    IcebergOpCommitter createTableCommitter = IcebergOperation.getCreateTableCommitter(
      Path.of(tempDir.getRoot().getPath()).resolve(tableName),
      schema,
      null,
      new Configuration());
    createTableCommitter.commit();

    File tableFolder = new File(tempDir.getRoot(), tableName);
    assertTrue(tableFolder.exists()); // table folder
    File metadataFolder = new File(tableFolder, "metadata");
    assertTrue(metadataFolder.exists()); // metadata folder
    assertTrue(new File(metadataFolder, "v1.metadata.json").exists()); // snapshot metadata
    assertTrue(new File(metadataFolder, "version-hint.text").exists()); // root pointer file
  }


}
