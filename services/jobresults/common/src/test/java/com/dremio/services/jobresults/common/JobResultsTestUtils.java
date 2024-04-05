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
package com.dremio.services.jobresults.common;

import com.dremio.common.utils.protos.QueryWritableBatch;
import com.dremio.exec.enginemanagement.proto.EngineManagementProtos;
import com.dremio.exec.proto.CoordinationProtos;
import com.dremio.exec.proto.UserBitShared;
import com.dremio.exec.record.WritableBatch;
import java.util.Arrays;
import java.util.List;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.ValueVector;

/** Utils related to JobResults */
public class JobResultsTestUtils {
  public static CoordinationProtos.NodeEndpoint getSampleForeman() {
    return CoordinationProtos.NodeEndpoint.newBuilder()
        .setAddress("sample-foreman-address")
        .setConduitPort(9000)
        .setMaxDirectMemory(8096)
        .setEngineId(EngineManagementProtos.EngineId.newBuilder().setId("engine-id").build())
        .setSubEngineId(
            EngineManagementProtos.SubEngineId.newBuilder().setId("subengine-id").build())
        .build();
  }

  public static IntVector intVector(BufferAllocator allocator, String name, int count) {
    IntVector vec = new IntVector(name, allocator);
    vec.allocateNew(count);
    vec.set(0, 20);
    vec.set(1, 50);
    vec.set(2, -2000);
    vec.set(3, 327345);
    vec.setNull(4);

    vec.setValueCount(count);
    return vec;
  }

  public static BitVector bitVector(BufferAllocator allocator, String name, int count) {
    BitVector vec = new BitVector(name, allocator);
    vec.allocateNew(count);
    vec.set(0, 1);
    vec.set(1, 0);
    vec.setNull(2);
    vec.set(3, 1);
    vec.set(4, 1);

    vec.setValueCount(count);
    return vec;
  }

  public static QueryWritableBatch createQueryWritableBatch(BufferAllocator allocator, int count) {
    List<ValueVector> original =
        Arrays.<ValueVector>asList(
            bitVector(allocator, "field1", count), intVector(allocator, "field2", count));
    WritableBatch batch = WritableBatch.getBatchNoHV(count, original, false);
    UserBitShared.QueryData header =
        UserBitShared.QueryData.newBuilder().setRowCount(count).setDef(batch.getDef()).build();
    return new QueryWritableBatch(header, batch.getBuffers());
  }
}
