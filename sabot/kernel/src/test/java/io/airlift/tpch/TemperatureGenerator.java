package io.airlift.tpch;
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
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.complex.impl.UnionListWriter;

/**
 * Creates a list type vector and generates data for it.
 */
public class TemperatureGenerator extends TpchGenerator {

  private static final int TEMPERATURE_MIN = -30;
  private static final int TEMPERATURE_MAX = 100;

  private final RandomBoundedInt temperatureRandom = randomBoundedInt(298370230, TEMPERATURE_MIN, TEMPERATURE_MAX);

  private final ListVector temperature;

  public TemperatureGenerator(final BufferAllocator allocator, final GenerationDefinition def, final int partitionIndex, final GenerationDefinition.TpchTable table, final String...includedColumns) {

    super(table, allocator, def, partitionIndex, includedColumns);

    this.temperature = variableSizedList("t_temperature");

    finalizeSetup();

  }

  @Override
  protected void generateRecord(final long globalRecordIndex, final int outputIndex){

    final UnionListWriter listWriter = new UnionListWriter(temperature);

    listWriter.setPosition(outputIndex);
    listWriter.startList();

    for(int j=0; j<45; j++) {
      listWriter.writeInt(temperatureRandom.nextValue());
      temperatureRandom.rowFinished();
    }

    listWriter.endList();
  }
}
