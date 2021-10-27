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
package com.dremio.exec.store;

import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.complex.StructVector;
import org.apache.arrow.vector.complex.impl.NullableStructWriter;
import org.apache.arrow.vector.complex.impl.UnionListWriter;
import org.apache.arrow.vector.complex.writer.BaseWriter;
import org.apache.arrow.vector.complex.writer.IntWriter;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.junit.Assert;
import org.junit.Test;

import com.dremio.exec.ExecTest;

public class ComplexTypeCopiersTest extends ExecTest {
  @Test
  public void testStructCopier() {
    try (final StructVector s1 = StructVector.empty("s1", allocator);
         final StructVector s2 = StructVector.empty("s2", allocator)) {
      int rowcount = 5000;
      s1.addOrGet("struct_child", FieldType.nullable(Types.MinorType.INT.getType()), IntVector.class);
      s2.addOrGet("struct_child", FieldType.nullable(Types.MinorType.INT.getType()), IntVector.class);
      s1.allocateNew();
      BaseWriter.StructWriter structWriter = new NullableStructWriter(s1);
      IntWriter intWriter = structWriter.integer("struct_child");
      for(int i = 0; i<rowcount; i++) {
        if (i % 10 == 0) {
          continue;
        }
        structWriter.setPosition(i);
        structWriter.start();
        intWriter.writeInt(i);
        structWriter.end();
      }
      s1.setValueCount(rowcount);
      ComplexTypeCopiers.StructCopier structCopier = new ComplexTypeCopiers.StructCopier(s1, s2);
      structCopier.copyNonDataBufferRefs(rowcount);
      Assert.assertEquals(rowcount, s2.getValueCount());
      Assert.assertEquals(500, s1.getNullCount());
      Assert.assertEquals(500, s2.getNullCount());
      for(int i=0; i<rowcount; ++i) {
        Assert.assertEquals( i % 10 == 0, s2.isNull(i));
      }
    }
  }

  @Test
  public void testListCopier() {
    int rowcount = 5000;
    try (final ListVector l1 = new ListVector("colList", allocator, null);
         final ListVector l2 = new ListVector("colList", allocator, null);
         final ArrowBuf tempBuf = allocator.buffer(rowcount * 16)) {
      l1.allocateNew();
      UnionListWriter listWriter = new UnionListWriter(l1);
      for (int i = 0; i < rowcount; i++) {
        if (i % 10 == 0) {
          continue;
        }
        listWriter.setPosition(i);
        listWriter.startList();
        for (int j = 0; j < i; j++) {
          byte[] varCharVal = String.format("%d", (i%10)).getBytes();
          tempBuf.setBytes(0, varCharVal);
          listWriter.writeVarChar(0, varCharVal.length, tempBuf);
        }
        listWriter.endList();
      }
      l1.setValueCount(rowcount);
      ComplexTypeCopiers.ListCopier listCopier = new ComplexTypeCopiers.ListCopier(l1, l2);
      listCopier.copyNonDataBufferRefs(rowcount);
      Assert.assertEquals(rowcount, l2.getValueCount());
      Assert.assertEquals(500, l1.getNullCount());
      Assert.assertEquals(500, l2.getNullCount());
      for(int i=0; i<rowcount; ++i) {
        Assert.assertEquals( i % 10 == 0, l2.isNull(i));
        Assert.assertEquals(i % 10 == 0 ? 0 : i,
          l2.getOffsetBuffer().getInt((i+1) * ListVector.OFFSET_WIDTH) -
            l2.getOffsetBuffer().getInt(i * ListVector.OFFSET_WIDTH));
      }
    }
  }
}
