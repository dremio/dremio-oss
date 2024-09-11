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
package com.dremio.sabot;

import com.dremio.exec.record.BatchSchema;
import com.dremio.exec.record.SchemaBuilder;
import com.dremio.exec.record.VectorAccessible;
import com.dremio.exec.record.VectorContainer;
import com.dremio.sabot.TypeDataGenerator.RandomGenerator;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.types.pojo.ArrowType.ArrowTypeID;
import org.apache.commons.lang3.RandomStringUtils;

/**
 * Data will be generated in following way - User can specify following information - Arrow data
 * types to generate data for, total number of rows to be generated, batch size. This class
 * generates output container (VectorContainer with individual vectors for input types) to place the
 * output in. It also creates random data generators (TypeDataGenerator) for each of these data
 * types that can give us a batch of random values of that type. Whenever caller needs next batch of
 * data to feed to an operator, it calls next function of this class, this class will get a batch of
 * random values from respective TypeDataGenerator's and will combine all result in 'container'
 * where user can access the batch.
 */
public class SortDataGenerator implements Generator {
  private final List<FieldInfo> fields;
  private final List<TypeDataGenerator<? extends Comparable<?>>> dataGenerators = new ArrayList<>();
  private final BufferAllocator allocator;
  private final int numOfBatches;
  private VectorContainer container;
  private final int numOfRows;
  private final int batchSize;
  private final int varcharLen;
  private static Map<ArrowTypeID, RandomGenerator<?>> typeIDRandomGeneratorMap;
  int absoluteIndex;

  public SortDataGenerator(
      List<FieldInfo> fields,
      BufferAllocator allocator,
      int numOfRows,
      int batchSize,
      int varcharLen)
      throws Exception {
    this.fields = fields;
    this.allocator = allocator;
    this.numOfRows = numOfRows;
    this.varcharLen = varcharLen;
    this.batchSize = Math.min(numOfRows, batchSize);
    this.numOfBatches = numOfRows / this.batchSize;
    typeIDRandomGeneratorMap = getTypeIDRandomGeneratorMap(varcharLen);
    createVectorContainer();
    dataGenerators.addAll(getDataGenerators(fields, container, numOfBatches, numOfRows, batchSize));
  }

  @Override
  public int next(int records) {
    if (fields.isEmpty()) {
      return 0;
    }
    records = Math.min(Math.min(records, batchSize), getRecordsRemaining());
    for (TypeDataGenerator<?> tdg : dataGenerators) {
      if (tdg.next(records) == 0) {
        return 0;
      }
    }
    container.setAllCount(records);
    absoluteIndex += records;
    return records;
  }

  public int getRecordsRemaining() {
    return numOfRows - absoluteIndex;
  }

  @Override
  public VectorAccessible getOutput() {
    return container;
  }

  public void createVectorContainer() throws Exception {
    SchemaBuilder builder = BatchSchema.newBuilder();
    for (FieldInfo info : fields) {
      builder.addField(info.getField());
    }

    container = VectorContainer.create(allocator, builder.build());

    addVectorsToContainer(fields, container);
  }

  /*
  List of Apache Arrow Minor Types - https://arrow.apache.org/docs/java/reference/org/apache/arrow/vector/types/Types.MinorType.html
   */
  private static List<TypeDataGenerator<? extends Comparable<?>>> getDataGenerators(
      List<FieldInfo> fields,
      VectorContainer container,
      int numOfBatches,
      int numOfRows,
      int batchSize)
      throws Exception {
    List<TypeDataGenerator<?>> typeDataGenerators = new ArrayList<>();
    for (FieldInfo info : fields) {
      typeDataGenerators.add(
          new TypeDataGenerator<>(
              numOfBatches,
              numOfRows,
              getGenerator(info.getField().getType().getTypeID()),
              info,
              batchSize,
              container.addOrGet(info.getField())));
    }
    return typeDataGenerators;
  }

  private static void addVectorsToContainer(List<FieldInfo> fields, VectorContainer container) {
    for (FieldInfo info : fields) {
      container.addOrGet(info.getField());
    }
  }

  @Override
  public void close() {
    container.close();
  }

  private static Map<ArrowTypeID, RandomGenerator<?>> getTypeIDRandomGeneratorMap(int varcharLen) {
    Map<ArrowTypeID, RandomGenerator<?>> generators = new HashMap<>();

    generators.put(
        ArrowTypeID.Int,
        () -> {
          Random rand = new Random();
          return rand.nextInt();
        });
    generators.put(ArrowTypeID.Utf8, () -> RandomStringUtils.randomAlphabetic(varcharLen));
    generators.put(
        ArrowTypeID.Date,
        () -> {
          Random rnd = new Random();
          long ms = -946771200000L + (Math.abs(rnd.nextLong()) % (70L * 365 * 24 * 60 * 60 * 1000));
          return new Date(ms);
        });
    generators.put(
        ArrowTypeID.Bool,
        () -> {
          Random rand = new Random();
          return rand.nextBoolean();
        });

    return generators;
  }

  private static RandomGenerator getGenerator(ArrowTypeID typeId) throws Exception {
    if (!typeIDRandomGeneratorMap.containsKey(typeId)) {
      throw new Exception(
          String.format("Data generation for sort doesn't support type %s", typeId));
    }
    return typeIDRandomGeneratorMap.get(typeId);
  }
}
