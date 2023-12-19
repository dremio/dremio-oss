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
package io.airlift.tpch;

import java.util.Arrays;
import java.util.List;

import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.complex.impl.UnionListWriter;
import org.apache.arrow.vector.complex.writer.BaseWriter.StructWriter;
import org.apache.arrow.vector.types.pojo.Field;

import com.dremio.common.expression.CompleteType;
import com.dremio.test.AllocatorRule;

/**
 * Creates a struct type vector and generates data for it.
 */
public class ListStructGenerator extends TpchGenerator {

  public static final AllocatorRule allocatorRule = AllocatorRule.defaultAllocator();

  private static final int WORD_AVERAGE_LENGTH = 175;
  private static final int INT_MIN = -30;
  private static final int INT_MAX = 100;

  private final RandomText wordRandom = randomText(1335826707, TEXT_POOL, WORD_AVERAGE_LENGTH);
  private final RandomBoundedInt intRandom = randomBoundedInt(298370230, INT_MIN, INT_MAX);

  private static BufferAllocator allocator;


  private final ListVector list;

  /**
   * Struct generated in this class would have 1 varchar and 1 int field
   */


  public ListStructGenerator(final BufferAllocator allocator, final GenerationDefinition def, final int partitionIndex, final GenerationDefinition.TpchTable table, final String...includedColumns) {

    super(table, allocator, def, partitionIndex, includedColumns);
    this.allocator = allocator;

    List<Field> children = Arrays.asList(CompleteType.VARCHAR.toField("varchar", true), CompleteType.INT.toField("int", true));
    Field listOfStruct =  new CompleteType(CompleteType.LIST.getType(), CompleteType.struct(children).toField(ListVector.DATA_VECTOR_NAME, true)).toField("list", true);
    //create a struct vector
    this.list = (ListVector) complexType(listOfStruct);
    //this.list.initializeChildrenFromFields(Collections.singletonList(map));

    finalizeSetup();
  }

  @Override
  protected void generateRecord(final long globalRecordIndex, final int outputIndex) {

    final UnionListWriter listWriter = new UnionListWriter(list);

    try(final ArrowBuf tempBuf =  allocator.buffer(1024)){
      listWriter.setPosition(outputIndex);
      listWriter.startList();
      for (int j = 0; j < 5; j++) {
        final StructWriter structWriter = listWriter.struct();
        structWriter.start();
        final byte[] varCharVal = wordRandom.nextValue().getBytes();
        tempBuf.setBytes(0, varCharVal);
        if(j != 1 && j != 3 ){
          structWriter.integer("int").writeInt(intRandom.nextValue());
          structWriter.varChar("varchar").writeVarChar(0, varCharVal.length,tempBuf);
        }
        structWriter.end();
        intRandom.rowFinished();
        wordRandom.rowFinished();
        structWriter.end();
      }
      listWriter.endList();

    }
  }
}
