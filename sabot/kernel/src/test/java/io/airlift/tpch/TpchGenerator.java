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

import static java.nio.charset.StandardCharsets.UTF_8;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.types.pojo.Field;

import com.dremio.common.AutoCloseables;
import com.dremio.common.types.TypeProtos.MajorType;
import com.dremio.common.types.TypeProtos.MinorType;
import com.dremio.common.types.Types;
import com.dremio.exec.record.BatchSchema.SelectionVectorMode;
import com.dremio.exec.record.VectorAccessible;
import com.dremio.exec.record.VectorContainer;
import com.dremio.exec.record.VectorWrapper;
import com.dremio.sabot.Generator;

import io.airlift.tpch.GenerationDefinition.TpchTable;

public abstract class TpchGenerator implements Generator {

  protected static final Distributions DISTRIBUTIONS = Distributions.getDefaultDistributions();
  protected static final TextPool TEXT_POOL = TextPool.getDefaultTestPool();


  private final VectorContainer returned;
  private final VectorContainer all;
  protected final GenerationDefinition def;
  private final long startIndex;
  protected final long rowCount;
  private long index;
  private Set<String> included;

  private List<AbstractRandomInt> randoms = new ArrayList<>();

  protected TpchGenerator(TpchTable table, BufferAllocator allocator, GenerationDefinition def, int partitionIndex, String...includedColumns){
    startIndex = def.getStartIndex(table, partitionIndex);
    rowCount =  def.getRowCount(table, partitionIndex);
    this.returned = new VectorContainer(allocator);
    this.all = new VectorContainer(allocator);

    this.included = new HashSet<>(Arrays.asList(includedColumns));
    this.def = def;
    index = startIndex;
  }

  protected void finalizeSetup(){
    for(AbstractRandomInt r : randoms){
      r.advanceRows(startIndex);
    }
    all.buildSchema(SelectionVectorMode.NONE);
    returned.buildSchema(SelectionVectorMode.NONE);
  }


  @Override
  public VectorAccessible getOutput() {
    return returned;
  }

  public int next(int desiredCount){
    final long termination = Math.min(startIndex + rowCount, index + desiredCount);
    final int recordsGenerated = (int) (termination - index);

//    System.out.println(String.format("[next] rowCount: %d, start: %d, termination: %d, records: %d", rowCount, index, termination, recordsGenerated));
    if(recordsGenerated < 1){
      return 0;
    }

    all.allocateNew();

    int vectorIndex = 0;
    for(long i = index; i < termination; i++, vectorIndex++){
      generateRecord(i, vectorIndex);
      for(AbstractRandomInt r : randoms){
        r.rowFinished();
      }
    }

    index += recordsGenerated;

    returned.setRecordCount(recordsGenerated);
    for(VectorWrapper<?> w : returned){
      w.getValueVector().setValueCount(recordsGenerated);
    }

    return recordsGenerated;
  }

  protected abstract void generateRecord(long globalRecordIndex, int outputIndex);

  public void close() throws Exception {
    AutoCloseables.close((AutoCloseable) all, returned);
  }

  private <T extends ValueVector> T addOrGet(Field field){
    T vector = all.addOrGet(field);
    if(included.contains(field.getName()) || included.isEmpty()){
      returned.add(vector);
    }
    return vector;
  }

  private <T extends ValueVector> T addOrGet(String name, MajorType type, Class<T> clazz){
    T vector = all.addOrGet(name, type, clazz);
    if(included.contains(name) || included.isEmpty()){
      returned.add(vector);
    }
    return vector;
  }

  protected BigIntVector int8(String name){
    return addOrGet(name, Types.optional(MinorType.BIGINT), BigIntVector.class);
  }

  protected FieldVector complexType(Field map){

    return addOrGet(map);
  }

  protected ListVector variableSizedList(String name){

    return addOrGet(name, Types.optional(MinorType.LIST), ListVector.class);

  }


  protected IntVector int4(String name){
    return addOrGet(name, Types.optional(MinorType.INT), IntVector.class);
  }

  protected VarCharVector varChar(String name){
    return addOrGet(name, Types.optional(MinorType.VARCHAR), VarCharVector.class);
  }

  protected void set(int index, VarCharVector v, String value){
    byte[] bytesValue = value.getBytes(UTF_8);
    v.setSafe(index, bytesValue, 0, bytesValue.length);
  }

  protected RandomBoundedInt randomBoundedInt(long seed, int lowValue, int highValue){
    RandomBoundedInt random = new RandomBoundedInt(seed, lowValue, highValue);
    randoms.add(random);
    return random;
  }

  protected RandomInt randomInt(long seed, int expectedUsagePerRow){
    RandomInt random = new RandomInt(seed, expectedUsagePerRow);
    randoms.add(random);
    return random;
  }

  protected RandomString randomString(long seed, Distribution distribution){
    RandomString random = new RandomString(seed, distribution);
    randoms.add(random);
    return random;
  }

  protected RandomText randomText(long seed, TextPool textPool, double averageTextLength) {
    RandomText random = new RandomText(seed, textPool, averageTextLength);
    randoms.add(random);
    return random;
  }

  protected RandomAlphaNumeric randomAlphaNumeric(long seed, int averageLength) {
    RandomAlphaNumeric random = new RandomAlphaNumeric(seed, averageLength);
    randoms.add(random);
    return random;
  }

  protected RandomPhoneNumber randomPhoneNumber(long seed) {
    RandomPhoneNumber random = new RandomPhoneNumber(seed);
    randoms.add(random);
    return random;
  }

  /**
   * Create a monolithic partition generator.
   * @param table
   * @param scale
   * @param allocator
   * @param includedColumns
   * @return
   */
  public static TpchGenerator singleGenerator(GenerationDefinition.TpchTable table, double scale, BufferAllocator allocator, String... includedColumns) {
    GenerationDefinition def = new GenerationDefinition(scale, Long.MAX_VALUE);
    switch(table){
      case CUSTOMER:
        return new CustomerGenerator(allocator, def, 1, TpchTable.CUSTOMER, includedColumns);
      case TEMPERATURE:
        return new TemperatureGenerator(allocator, def, 1, TpchTable.TEMPERATURE, includedColumns);
      case WORD_GROUPS:
        return new WordGroupsGenerator(allocator, def, 1, TpchTable.WORD_GROUPS, includedColumns);
      case MIXED_GROUPS:
        return new MixedGroupGenerator(allocator, def, 1, TpchTable.MIXED_GROUPS, includedColumns);
      case CUSTOMER_LIMITED:
        return new CustomerGenerator(allocator, def, 1, TpchTable.CUSTOMER_LIMITED, includedColumns);
      case REGION:
        return new RegionGenerator(allocator, def, includedColumns);
      case NATION:
        return new NationGenerator(allocator, def, includedColumns);
      default:
        throw new UnsupportedOperationException();
    }
  }
}
