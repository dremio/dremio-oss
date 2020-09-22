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
package com.dremio.exec.store.hive.exec;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import com.dremio.exec.store.hive.metadata.HiveMetadataUtils;
import com.dremio.hive.proto.HiveReaderProto;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.io.orc.OrcSplit;
import org.apache.hadoop.security.UserGroupInformation;
import org.junit.Test;

import com.dremio.exec.physical.base.OpProps;
import com.dremio.exec.store.SplitAndPartitionInfo;
import com.dremio.service.namespace.dataset.proto.PartitionProtobuf;
import com.google.protobuf.ByteString;
import com.dremio.exec.store.hive.HiveStoragePlugin;
import com.dremio.sabot.exec.fragment.FragmentExecutionContext;

public class TestHiveScanBatchCreator {
  @Test
  public void ensureStoragePluginIsUsedForUsername() throws Exception {
    final String originalName = "Test";
    final String finalName = "Replaced";

    final HiveScanBatchCreator creator = new HiveScanBatchCreator();

    final HiveStoragePlugin plugin = mock(HiveStoragePlugin.class);
    when(plugin.getUsername(originalName)).thenReturn(finalName);

    final FragmentExecutionContext fragmentExecutionContext = mock(FragmentExecutionContext.class);
    when(fragmentExecutionContext.getStoragePlugin(any())).thenReturn(plugin);

    final OpProps props = mock(OpProps.class);
    final HiveProxyingSubScan hiveSubScan = mock(HiveProxyingSubScan.class);
    when(hiveSubScan.getProps()).thenReturn(props);
    when(hiveSubScan.getProps().getUserName()).thenReturn(originalName);

    final UserGroupInformation ugi = creator.getUGI(plugin, hiveSubScan);
    verify(plugin).getUsername(originalName);
    assertEquals(finalName, ugi.getUserName());
  }

  private List<SplitAndPartitionInfo> buildSplits(String scheme) {
    List<SplitAndPartitionInfo> splitAndPartitionInfos = new ArrayList<>();
    final int numPartitions = 2;
    final int numSplitsPerPartition = 2;

    OrcSplit orcSplit = new OrcSplit(new Path(scheme + "://test.bucket/file.orc"), null, 0, 0, new String[0],
      null, true, false, Collections.emptyList(), 0, 0);


    for (int i = 0; i < numPartitions; ++i) {
      String extendedProp = "partition_extended_" + String.valueOf(i);
      PartitionProtobuf.NormalizedPartitionInfo partitionInfo = PartitionProtobuf.NormalizedPartitionInfo
        .newBuilder()
        .setId(String.valueOf(i))
        .setSize(i * 1000)
        .setExtendedProperty(ByteString.copyFrom(extendedProp.getBytes()))
        .build();

      for (int j = 0; j < numSplitsPerPartition; ++j) {
        HiveReaderProto.HiveSplitXattr splitXattr = HiveMetadataUtils.buildHiveSplitXAttr(i, orcSplit);

        PartitionProtobuf.NormalizedDatasetSplitInfo splitInfo = PartitionProtobuf.NormalizedDatasetSplitInfo
          .newBuilder()
          .setPartitionId(String.valueOf(i))
          .setExtendedProperty(ByteString.copyFrom(splitXattr.toByteArray()))
          .build();

        splitAndPartitionInfos.add(new SplitAndPartitionInfo(partitionInfo, splitInfo));
      }
    }
    return splitAndPartitionInfos;
  }

  @Test
  public void testAllSplitsAreOnS3() {
    List<SplitAndPartitionInfo> s3Splits = buildSplits("s3");
    List<SplitAndPartitionInfo> hdfsplits = buildSplits("hdfs");
    HiveScanBatchCreator scanBatchCreator = new HiveScanBatchCreator();
    assertTrue(scanBatchCreator.allSplitsAreOnS3(s3Splits));
    assertFalse(scanBatchCreator.allSplitsAreOnS3(hdfsplits));
  }
}
