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

import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.complex.impl.UnionListWriter;

import com.dremio.test.AllocatorRule;

/**
 * Creates a ListVector with varchar base data and generates records for it.
 */
public class WordGroupsGenerator extends TpchGenerator {

  private static final int WORD_AVERAGE_LENGTH = 7;

  public static final AllocatorRule allocatorRule = AllocatorRule.defaultAllocator();

  private static BufferAllocator allocator;

  private final RandomText wordRandom = randomText(1335826707, TEXT_POOL, WORD_AVERAGE_LENGTH);

  private final ListVector wordGroups;

  public WordGroupsGenerator(final BufferAllocator allocator, final GenerationDefinition def, final int partitionIndex, final GenerationDefinition.TpchTable table, final String...includedColumns) {

    super(table, allocator, def, partitionIndex, includedColumns);
    this.allocator = allocator;

    //creates the list vector
    this.wordGroups = variableSizedList("word_groups");

    finalizeSetup();

  }

  @Override
  protected void generateRecord(final long globalRecordIndex, final int outputIndex) {

    final UnionListWriter listWriter = new UnionListWriter(wordGroups);

    try(final ArrowBuf tempBuf =  allocator.buffer(1024)){

      listWriter.setPosition(outputIndex);

      listWriter.startList();
      for (int j = 0; j < 26; j++) {
        final byte[] varCharVal = wordRandom.nextValue().getBytes();
        tempBuf.setBytes(0, varCharVal);
        listWriter.writeVarChar(0, varCharVal.length, tempBuf);
        wordRandom.rowFinished();
      }
      listWriter.endList();
    }

  }

}
