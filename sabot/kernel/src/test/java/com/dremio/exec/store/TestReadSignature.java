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

import static org.apache.iceberg.types.Types.NestedField.required;

import com.dremio.BaseTestQuery;
import com.dremio.exec.store.file.proto.FileProtobuf;
import com.dremio.exec.store.iceberg.IcebergPartitionData;
import com.dremio.exec.store.metadatarefresh.committer.FullRefreshReadSignatureProvider;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import java.util.HashSet;
import java.util.Set;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.types.Types;
import org.junit.Test;

public class TestReadSignature extends BaseTestQuery {

  /*
   Test the filtering of invalid partition columns names when generating the read signature.

   Columns "part_order_year", "part_number", "dirt0" and "dir1a" should be filtered out
   as they do not match the filter (ie: "dir([0-9])+") for valid partition column names
   and therefore will not end up in the read signature
  */

  @Test
  public void testReadSignatureNameFiltering() throws Exception {

    String dataTableRoot = "/root/random";
    long queryStartTime = 0;
    FullRefreshReadSignatureProvider readSigProvider =
        new FullRefreshReadSignatureProvider(dataTableRoot, queryStartTime);

    Set<IcebergPartitionData> addedPartitions = new HashSet<>(); // partitions with new files
    Set<IcebergPartitionData> deletedPartitions = new HashSet<>(); // partitions with deleted files

    // Valid Columns names:   "dir0", "dir476"
    // Invalid Columns names: "part_order_year", "part_number", "dirt0", "dir1a"

    String colName1 = "part_order_year";
    String colName2 = "dir0";
    String colName3 = "part_number";
    String colName4 = "dir476";
    String colName5 = "dirt0";
    String colName6 = "dir1a";

    Schema schema1 =
        new Schema(
            required(0, colName1, Types.StringType.get()),
            required(1, colName2, Types.StringType.get()),
            required(2, colName3, Types.StringType.get()),
            required(3, colName4, Types.StringType.get()),
            required(4, colName5, Types.StringType.get()),
            required(5, colName6, Types.StringType.get()));

    PartitionSpec partitionSpec1 =
        PartitionSpec.builderFor(schema1)
            .identity(colName1)
            .identity(colName2)
            .identity(colName3)
            .identity(colName4)
            .identity(colName5)
            .identity(colName6)
            .build();

    IcebergPartitionData icebergPartitionData11 =
        new IcebergPartitionData(partitionSpec1.partitionType());
    icebergPartitionData11.setString(0, "2267");
    icebergPartitionData11.setString(1, "part_order_year=2267");
    icebergPartitionData11.setString(2, "711558600");
    icebergPartitionData11.setString(3, "part_number=711558600");
    icebergPartitionData11.setString(4, "1112223333");
    icebergPartitionData11.setString(5, "abcdef");

    addedPartitions.add(icebergPartitionData11);

    ByteString newReadSignature = readSigProvider.compute(addedPartitions, deletedPartitions);

    // Verify the proper partition columns appear
    // Expected Paths:
    //     "/root/random"
    //     "/root/random/part_order_year=2267"
    //     "/root/random/part_order_year=2267/part_number=711558600"

    try {
      FileProtobuf.FileUpdateKey updateKey =
          com.dremio.exec.store.file.proto.FileProtobuf.FileUpdateKey.parseFrom(newReadSignature);
      if (updateKey.getCachedEntitiesCount() != 3) {
        throw new RuntimeException(
            "Invalid number of directories found, directory filtering failure");
      } else if (!updateKey.getCachedEntities(0).hasPath()
          || !updateKey.getCachedEntities(0).getPath().equals("/root/random")) {
        throw new RuntimeException("Failed to find directory '/root/random'");
      } else if (!updateKey.getCachedEntities(1).hasPath()
          || !updateKey
              .getCachedEntities(1)
              .getPath()
              .equals("/root/random/part_order_year=2267")) {
        throw new RuntimeException("Failed to find directory '/root/random/part_order_year=2267'");
      } else if (!updateKey.getCachedEntities(2).hasPath()
          || !updateKey
              .getCachedEntities(2)
              .getPath()
              .equals("/root/random/part_order_year=2267/part_number=711558600")) {
        throw new RuntimeException("Failed to find directory '/root/random/part_number=711558600'");
      }
    } catch (InvalidProtocolBufferException e) {
      // Wrap protobuf exception for consistency
      throw new RuntimeException(e);
    }
  }
}
