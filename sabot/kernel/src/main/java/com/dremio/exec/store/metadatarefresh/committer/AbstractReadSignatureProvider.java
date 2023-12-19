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
package com.dremio.exec.store.metadatarefresh.committer;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import com.dremio.exec.store.iceberg.IcebergPartitionData;
import com.dremio.exec.util.PartitionUtils;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;


public abstract class AbstractReadSignatureProvider implements ReadSignatureProvider {

  protected final String tableRoot;
  protected final long queryStartTime;
  protected final Predicate<String> doesPartitionExist;
  protected final Function<IcebergPartitionData, List<String>> fileSystemPartitionToPathMapper;

  protected AbstractReadSignatureProvider(final String tableRoot, final long queryStartTime,
                                          Predicate<String> partitionExists) {
    this.tableRoot = tableRoot;
    this.queryStartTime = queryStartTime;
    fileSystemPartitionToPathMapper = ipd -> {
      String[] partitionValues = new String[ipd.size()];
      for (int i = 0; i < ipd.size(); i++) {
        Object partVal = ipd.get(i);
        if (partVal == null) {
          partitionValues[i] = "";
        } else {
          String partName = ipd.getPartitionType().fields().get(i).name();
          // Only use partition columns that match the standard "dirN" format when constructing paths.
          if(PartitionUtils.isPartitionName(partName, true )){
            partitionValues[i] = partVal.toString();
          } else {
            partitionValues[i] = "";
          }
        }
      }
      Path currentPath = Paths.get(tableRoot);

      Set<String> paths = new HashSet<>();
      paths.add(currentPath.toString());

      for(String subPath : partitionValues) {
        currentPath = currentPath.resolve(subPath);
        paths.add(currentPath.toString());
      }

      return paths.stream().collect(Collectors.toList());
    };
    doesPartitionExist = partitionExists;
  }

  protected static com.dremio.exec.store.file.proto.FileProtobuf.FileUpdateKey decodeReadSignatureByteString(ByteString readSig) {
    try {
      return com.dremio.exec.store.file.proto.FileProtobuf.FileUpdateKey.parseFrom(readSig);
    } catch (InvalidProtocolBufferException e) {
      // Wrap protobuf exception for consistency
      throw new RuntimeException(e);
    }
  }
}
