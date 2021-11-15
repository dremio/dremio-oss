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
package com.dremio.exec.expr.fn.impl;

import javax.inject.Inject;

import org.apache.arrow.vector.holders.NullableIntHolder;
import org.apache.arrow.vector.holders.NullableVarBinaryHolder;

import com.dremio.exec.expr.SimpleFunction;
import com.dremio.exec.expr.annotations.FunctionTemplate;
import com.dremio.exec.expr.annotations.Output;
import com.dremio.exec.expr.annotations.Param;
import com.dremio.exec.expr.annotations.Workspace;
import com.dremio.exec.expr.fn.FunctionErrorContext;

/**
 * functions used for load distribution
 */
public class LoadDistributionFunctions {
  /**
   * Distributes DataFiles based on the partitionData present in the DataFile
   * This is used by the HashToRandomExchange between FooterReadTableFunction and ManifestWriter in metadata refresh queries
   */
  @FunctionTemplate(names = {"icebergDistributeByPartition"}, scope = FunctionTemplate.FunctionScope.SIMPLE, nulls = FunctionTemplate.NullHandling.INTERNAL )
  public static class DataFileDistribute implements SimpleFunction {

    @Param
    NullableVarBinaryHolder in;
    @Output
    NullableIntHolder out;
    @Workspace
    int unpartitionedDataCounter;
    @Inject
    FunctionErrorContext errCtx;

    @Override
    public void setup() {
      unpartitionedDataCounter = 0;
    }

    @Override
    public void eval() {
      int len = in.end - in.start;
      byte[] dst = new byte[len];
      in.buffer.getBytes(in.start, dst);
      try {
        final com.dremio.exec.store.iceberg.IcebergMetadataInformation icebergMetadataInformation = (com.dremio.exec.store.iceberg.IcebergMetadataInformation) com.dremio.exec.store.iceberg.IcebergSerDe.deserializeFromByteArray(dst);
        org.apache.iceberg.DataFile dataFile = com.dremio.exec.store.iceberg.IcebergSerDe.deserializeDataFile(icebergMetadataInformation.getIcebergMetadataFileByte());
        out.isSet = 1;
        org.apache.iceberg.StructLike partition = dataFile.partition();
        if (partition.size() == 0) {
          out.value = unpartitionedDataCounter;
          unpartitionedDataCounter++;
        } else {
          unpartitionedDataCounter = 0;
          out.value = partition.hashCode();
        }
      } catch (Exception e) {
        throw errCtx.error()
          .message("Failed during DataFile distribution")
          .addContext("exception", e.getMessage())
          .build();
      }
    }
  }
}
