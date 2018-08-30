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
package com.dremio.dac.cmd.upgrade;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.Base64;
import java.util.List;
import java.util.Objects;

import org.apache.arrow.flatbuf.OldSchema;
import org.apache.arrow.vector.types.pojo.Field;
import org.junit.Test;

import com.dremio.exec.record.BatchSchema;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.protobuf.ByteString;

/**
 * To test conversion of old Arrow Schema (pre Dremio 2.1.0)
 * to Arrow format as of Dremio 2.1.0+
 */
public class TestDatasetConfigUpgrade {

  /**
   * This is Binary Schema from Dremio pre 2.1.0 Base64 encoded
   */
  private static final String OLD_SCHEMA =
   "EAAAAAAACgAMAAAACAAEAAoAAAAIAAAACAAAAAAAAAAEAAAAUAEAANAAAABsAAAABAAAANT" +
     "+//8YAAAAGAAAADAAAAAwAAAAAAACATQAAAAAAAAAAgAAABAAAAAEAAAAuP7//0AAAQDA/v//" +
     "AQACAAAAAAC4/v//AAAAAUAAAAATAAAAJF9kcmVtaW9fJF91cGRhdGVfJAA4////" +
     "GAAAABgAAAA8AAAAPAAAAAAABQE4AAAAAAAAAAMAAAAcAAAAEAAAAAQAAAAg////CAABAKD///" +
     "8AACAAMP///wEAAgAAAAAAmP///wkAAABSX0NPTU1FTlQAAACY////GAAAABgAAABEAAAASAAAAAAABQFEAAAAAAAAAAMAAAAkAAAAGAAAAAQAAACA" +
     "////CAABAAgACAAGAAAACAAAAAAAIACY////" +
     "AQACAAAAAAAEAAQABAAAAAYAAABSX05BTUUAABQAHAAYABcAFgAQAAAADAAIAAQAFAAAABgAAAAYAAAAOAAAAEAAAAAAAAIBRAAAAAAAAAACAAAAGAAAAAQAAAD4" +
     "////QAABAAgACAAEAAYACAAAAAEAAgAAAAAACAAMAAgABwAIAAAAAAAAAUAAAAALAAAAUl9SRUdJT05LRVkA";

  @Test
  public void testArrowSchemaConversion() {
    byte[] schema = Base64.getDecoder().decode(OLD_SCHEMA);

    // try with old schema - preconverted
    try {
      BatchSchema.deserialize(schema);
      fail("Should not be able to process old Schema");
    } catch (IndexOutOfBoundsException e) {
      // expected
    }
    ByteString schemaBytes = ByteString.copyFrom(schema);
    DatasetConfigUpgrade datasetConfigUpgrade = new DatasetConfigUpgrade();
    OldSchema oldSchema = OldSchema.getRootAsOldSchema(schemaBytes.asReadOnlyByteBuffer());
    byte[] newschemaBytes = datasetConfigUpgrade.convertFromOldSchema(oldSchema);

    BatchSchema batchSchema = BatchSchema.deserialize(newschemaBytes);
    int fieldCount = batchSchema.getFieldCount();
    assertEquals(4, fieldCount);
    List<String> expected = ImmutableList.of(
      "R_REGIONKEY",
      "R_NAME",
      "R_COMMENT",
      "$_dremio_$_update_$");
    List<String> actual = Lists.newArrayList();
    for (int i = 0; i < fieldCount; i++) {
      Field field = batchSchema.getColumn(i);
      actual.add(field.getName());
    }
    assertTrue(Objects.equals(expected, actual));
  }
}
