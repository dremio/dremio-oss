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
package com.dremio.exec.planner.fragment;

import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.List;

import org.junit.Test;

import com.dremio.exec.physical.base.OpProps;
import com.dremio.exec.proto.CoordExecRPC.HBaseSubScanSpec;
import com.dremio.exec.store.mock.MockStorePOP;

public class TestMinorDataReader {

  @Test
  public void multiAttrsInSamePOP() throws Exception {
    MinorDataSerDe serDe = new MinorDataSerDe(null, null);
    MinorDataWriter writer = new MinorDataWriter(null, serDe, null);

    MockStorePOP pop = new MockStorePOP(OpProps.prototype(1), null);
    List<HBaseSubScanSpec> specList = new ArrayList<>();
    for (int i = 0; i < 4; ++i) {
      HBaseSubScanSpec spec = HBaseSubScanSpec
        .newBuilder()
        .setTableName("testTable" + i)
        .build();
      specList.add(spec);

      writer.writeProtoEntry(pop, "testKey" + i, spec);
    }

    MinorAttrsMap minorAttrsMap = MinorAttrsMap.create(writer.getAttrList());
    MinorDataReader reader = new MinorDataReader(null, serDe, null, minorAttrsMap);
    for (int i = 0; i < 4; ++i) {
      HBaseSubScanSpec spec = HBaseSubScanSpec.parseFrom(reader.readProtoEntry(pop, "testKey" + i));
      assertEquals(spec, specList.get(i));
    }
  }

  @Test
  public void sameKeyMultiPOPs() throws Exception {
    MinorDataSerDe serDe = new MinorDataSerDe(null, null);
    MinorDataWriter writer = new MinorDataWriter(null, serDe, null);

    MockStorePOP pop1 = new MockStorePOP(OpProps.prototype(1), null);
    MockStorePOP pop2 = new MockStorePOP(OpProps.prototype(2), null);

    List<HBaseSubScanSpec> specList = new ArrayList<>();
    for (int i = 0; i < 2; ++i) {
      HBaseSubScanSpec spec = HBaseSubScanSpec
        .newBuilder()
        .setTableName("testTable" + i)
        .build();
      specList.add(spec);

      writer.writeProtoEntry(i == 0 ? pop1 : pop2, "testKey", spec);
    }

    MinorAttrsMap minorAttrsMap = MinorAttrsMap.create(writer.getAttrList());
    MinorDataReader reader = new MinorDataReader(null, serDe, null, minorAttrsMap);

    HBaseSubScanSpec spec1 = HBaseSubScanSpec.parseFrom(reader.readProtoEntry(pop1, "testKey"));
    assertEquals(spec1, specList.get(0));

    HBaseSubScanSpec spec2 = HBaseSubScanSpec.parseFrom(reader.readProtoEntry(pop2, "testKey"));
    assertEquals(spec2, specList.get(1));
  }
}
