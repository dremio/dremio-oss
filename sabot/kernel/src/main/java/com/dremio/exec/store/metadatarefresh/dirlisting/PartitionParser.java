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
package com.dremio.exec.store.metadatarefresh.dirlisting;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.arrow.vector.types.pojo.Field;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;

import com.dremio.common.expression.CompleteType;
import com.dremio.exec.record.BatchSchema;
import com.dremio.exec.store.iceberg.IcebergPartitionData;
import com.dremio.exec.store.iceberg.SchemaConverter;
import com.dremio.io.file.Path;

/**
 * Class responsible for parsing paths to valid partitions. When inferPartitions is false, implicit partition column
 * names like dir0, dir1 and so on are used in the partition data returned by this class.
*/
public class PartitionParser {

  protected final Path rootPath;

  public static PartitionParser getInstance(Path rootPath, boolean inferPartitions) {
    if(inferPartitions) {
      return new InferredPartitionParser(rootPath);
    }
    return new PartitionParser(rootPath);
  }

  protected PartitionParser(Path rootPath) {
    this.rootPath = rootPath;
  }

  public IcebergPartitionData parsePartitionToPath(Path path) {
    String[] dirs = Path.withoutSchemeAndAuthority(rootPath).relativize(Path.withoutSchemeAndAuthority(path)).toString().split(Path.SEPARATOR);
    return buildIcebergPartitionData(dirs, new ArrayList<>(), new ArrayList<>());
  }

  protected IcebergPartitionData buildIcebergPartitionData(String[] dirs, List<String> additionalNames, List<String> additionalValues) {
    PartitionSpec.Builder partitionSpecBuilder = PartitionSpec
      .builderFor(buildIcebergSchema(additionalNames, dirs.length - 1));

    additionalNames.stream().forEach(x -> partitionSpecBuilder.identity(x));
    for(int j = 0; j < dirs.length - 1; j++) {
      partitionSpecBuilder.identity("dir" + j);
    }
    IcebergPartitionData icebergPartitionData = new IcebergPartitionData(partitionSpecBuilder.build().partitionType());

    AtomicInteger i = new AtomicInteger();
    additionalValues.stream().forEach(x -> {
      icebergPartitionData.setString(i.getAndIncrement(), x);
    });
    for(int j = 0; j < dirs.length - 1; j++) {
      icebergPartitionData.setString(i.get(), dirs[j]);
      i.incrementAndGet();
    }

    return icebergPartitionData;
  }

  protected Schema buildIcebergSchema(List<String> names, int levels) {
    Field[] fields = new Field[names.size() + levels];

    int i = 0;
    for(String name : names) {
      fields[i] = CompleteType.VARCHAR.toField(name);
      i++;
    }

    for(int j = 0; j < levels; j++) {
      fields[names.size() + j] = CompleteType.VARCHAR.toField("dir" + j);
    }

    BatchSchema tableSchema = BatchSchema.of(fields);

    SchemaConverter schemaConverter = new SchemaConverter();
    return schemaConverter.toIcebergSchema(tableSchema);
  }
}
