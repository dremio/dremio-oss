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
import org.apache.iceberg.DataFile;

import com.dremio.exec.expr.SimpleFunction;
import com.dremio.exec.expr.annotations.FunctionTemplate;
import com.dremio.exec.expr.annotations.Output;
import com.dremio.exec.expr.annotations.Param;
import com.dremio.exec.expr.annotations.Workspace;
import com.dremio.exec.expr.fn.FunctionErrorContext;
import com.dremio.exec.store.iceberg.IcebergMetadataInformation;
import com.dremio.exec.store.iceberg.IcebergSerDe;

/**
 * functions used for load distribution
 */
public class LoadDistributionFunctions {
    @FunctionTemplate(names = {"dremioSplitDistribute"}, scope = FunctionTemplate.FunctionScope.SIMPLE, nulls = FunctionTemplate.NullHandling.INTERNAL )
    public static class LoadDistribute implements SimpleFunction {

        @Param
        NullableVarBinaryHolder in;
        @Output
        NullableIntHolder out;
        @Workspace
        com.dremio.exec.util.SplitHasher hasher;
        @Inject
        com.dremio.exec.store.EndPointListProvider endPointListProvider;
        @Inject
        FunctionErrorContext errCtx;


        @Override
        public void setup() {
            hasher = new com.dremio.exec.util.SplitHasher(endPointListProvider.getDestinations());
        }

        @Override
        public void eval() {
            int len = in.end - in.start;
            byte[] dst = new byte[len];
            in.buffer.getBytes(in.start, dst);

            java.io.ByteArrayInputStream bis = new java.io.ByteArrayInputStream(dst);
            java.io.ObjectInput objectInput = null;
            try {
              objectInput = new java.io.ObjectInputStream(bis);
              Object obj = objectInput.readObject();
              org.apache.arrow.util.Preconditions.checkState(obj instanceof com.dremio.exec.store.SplitIdentity, "Unexpected Object type");
              com.dremio.exec.store.SplitIdentity splitIdentity = (com.dremio.exec.store.SplitIdentity) obj;
              out.isSet = 1;
              out.value = hasher.getMinorFragmentIndex(com.dremio.io.file.Path.of(splitIdentity.getPath()), splitIdentity.getBlockLocations(), splitIdentity.getOffset(), splitIdentity.getLength());
            } catch (Exception e) {
                throw errCtx.error()
                        .message("Failed during split distribution")
                        .addContext("exception", e.getMessage())
                        .build();
            } finally {
                try {
                    com.dremio.common.AutoCloseables.close(objectInput, bis);
                } catch (Exception ce) {
                    // ignore
                }
            }
        }
    }

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
    int numOfReceivers;
    @Workspace
    int unpartitionedDataCounter;
    @Inject
    com.dremio.exec.store.EndPointListProvider endPointListProvider;
    @Inject
    FunctionErrorContext errCtx;

    @Override
    public void setup() {
      numOfReceivers = endPointListProvider.getDestinations().size();
      unpartitionedDataCounter = 0;
    }

    @Override
    public void eval() {
      int len = in.end - in.start;
      byte[] dst = new byte[len];
      in.buffer.getBytes(in.start, dst);
      try {
        final IcebergMetadataInformation icebergMetadataInformation = IcebergSerDe.deserializeFromByteArray(dst);
        DataFile dataFile = IcebergSerDe.deserializeDataFile(icebergMetadataInformation.getIcebergMetadataFileByte());

        out.isSet = 1;
        org.apache.iceberg.StructLike partition = dataFile.partition();
        if (partition.size() == 0) {
          out.value = unpartitionedDataCounter % numOfReceivers;
          unpartitionedDataCounter++;
        } else {
          unpartitionedDataCounter = 0;
          out.value = partition.hashCode() % numOfReceivers;
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
