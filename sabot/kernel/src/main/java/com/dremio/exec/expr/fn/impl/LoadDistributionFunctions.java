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
    @FunctionTemplate(names = {"dremioSplitDistribute"}, scope = FunctionTemplate.FunctionScope.SIMPLE, nulls = FunctionTemplate.NullHandling.INTERNAL )
    public static class LoadDistribute implements SimpleFunction {

        @Param
        NullableVarBinaryHolder in;
        @Output
        NullableIntHolder out;
        @Workspace
        com.dremio.exec.util.RendezvousSplitHasher hasher;
        @Inject
        com.dremio.exec.store.EndPointListProvider endPointListProvider;
        @Inject
        FunctionErrorContext errCtx;


        @Override
        public void setup() {
            hasher = new com.dremio.exec.util.RendezvousSplitHasher(endPointListProvider.getDestinations());
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
                if (obj instanceof org.apache.iceberg.ManifestFile) {
                    org.apache.iceberg.ManifestFile manifestFile = (org.apache.iceberg.ManifestFile) obj;
                    out.isSet = 1;
                    out.value = hasher.getNodeIndex(com.dremio.io.file.Path.of(manifestFile.path()), 0);
                } else if (obj instanceof com.dremio.exec.store.SplitAndPartitionInfo) {
                    com.dremio.exec.store.SplitAndPartitionInfo splitAndPartitionInfo = ((com.dremio.exec.store.SplitAndPartitionInfo) obj);

                    com.dremio.sabot.exec.store.parquet.proto.ParquetProtobuf.ParquetBlockBasedSplitXAttr splitScanXAttr =
                            (com.dremio.sabot.exec.store.parquet.proto.ParquetProtobuf.ParquetBlockBasedSplitXAttr)com.dremio.datastore.LegacyProtobufSerializer
                            .parseFrom(com.dremio.sabot.exec.store.parquet.proto.ParquetProtobuf.ParquetBlockBasedSplitXAttr.PARSER,
                                    splitAndPartitionInfo.getDatasetSplitInfo().getExtendedProperty());
                    out.isSet = 1;
                    out.value = hasher.getNodeIndex(com.dremio.io.file.Path.of(splitScanXAttr.getPath()), splitScanXAttr.getStart());
                }
            } catch (Exception e) {
                throw errCtx.error()
                        .message("Failed during split distribution")
                        .addContext("exception", e.getMessage())
                        .build();
            } finally {
                try {
                    java.util.List<AutoCloseable> closeables = com.google.common.collect.Lists.newArrayList();
                    closeables.add(objectInput);
                    closeables.add(bis);
                    com.dremio.common.AutoCloseables.close(closeables);
                } catch (Exception ce) {
                    // ignore
                }
            }
        }
    }


}
