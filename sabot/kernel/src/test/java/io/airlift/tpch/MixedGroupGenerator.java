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

import static org.apache.arrow.vector.types.Types.MinorType.INT;
import static org.apache.arrow.vector.types.Types.MinorType.VARCHAR;
import static org.apache.arrow.vector.types.pojo.FieldType.nullable;

import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.complex.StructVector;
import org.apache.arrow.vector.complex.impl.NullableStructWriter;
import org.apache.arrow.vector.types.pojo.Field;

import com.dremio.common.expression.CompleteType;
import com.dremio.test.AllocatorRule;

/**
 * Creates a struct type vector and generates data for it.
 */
public class MixedGroupGenerator extends TpchGenerator {

  public static final AllocatorRule allocatorRule = AllocatorRule.defaultAllocator();

  private static final int WORD_AVERAGE_LENGTH = 175;
  private static final int INT_MIN = -30;
  private static final int INT_MAX = 100;

  private final RandomText wordRandom = randomText(1335826707, TEXT_POOL, WORD_AVERAGE_LENGTH);
  private final RandomBoundedInt intRandom = randomBoundedInt(298370230, INT_MIN, INT_MAX);

  private static BufferAllocator allocator;

  private final StructVector mixedGroups;

  /**
   * Struct generated in this class would have 1 varchar and 1 int field
   */
  private static final Field map = CompleteType.struct(
    CompleteType.VARCHAR.toField("varchar"),
    CompleteType.INT.toField("int")
  ).toField("mixed_group");

  public MixedGroupGenerator(final BufferAllocator allocator, final GenerationDefinition def, final int partitionIndex, final GenerationDefinition.TpchTable table, final String...includedColumns) {

    super(table, allocator, def, partitionIndex, includedColumns);
    this.allocator = allocator;

    //create a struct vector
    this.mixedGroups = (StructVector) complexType(map);

    //create field vectors of struct
    this.mixedGroups.addOrGet("varchar", nullable(VARCHAR.getType()), VarCharVector.class);
    this.mixedGroups.addOrGet("int", nullable(INT.getType()), IntVector.class);

    finalizeSetup();
  }

  @Override
  protected void generateRecord(final long globalRecordIndex, final int outputIndex) {

    final NullableStructWriter structWriter = mixedGroups.getWriter();

    try(final ArrowBuf tempBuf =  allocator.buffer(1024)){

      structWriter.setPosition(outputIndex);

      structWriter.start();
      final byte[] varCharVal = wordRandom.nextValue().getBytes();
      tempBuf.setBytes(0, varCharVal);
      structWriter.varChar("varchar").writeVarChar(0, varCharVal.length,tempBuf);
      structWriter.integer("int").writeInt(intRandom.nextValue());
      structWriter.end();
      intRandom.rowFinished();
      wordRandom.rowFinished();

      structWriter.end();
    }
  }
}
