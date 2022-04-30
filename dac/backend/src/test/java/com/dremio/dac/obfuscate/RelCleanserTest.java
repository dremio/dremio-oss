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

package com.dremio.dac.obfuscate;

import static junit.framework.TestCase.assertEquals;

import org.junit.Test;

import com.dremio.context.RequestContext;
import com.dremio.context.SupportContext;

public class RelCleanserTest {

  @Test
  public void redactRelTree() throws Exception {
    String relTree = "00-00    Screen : rowType = RecordType(VARCHAR(65536) Fragment, BIGINT Records, VARCHAR(65536) Path, VARBINARY(65536) Metadata, INTEGER Partition, BIGINT FileSize, VARBINARY(65536) IcebergMetadata, VARBINARY(65536) fileschema, VARBINARY(65536) ARRAY PartitionData): rowcount = 10000.0, cumulative cost = {61100.0 rows, 67001.5 cpu, 6000.0 io, 6000.0 network, 0.0 memory}, id = 217\n" +
      "00-01      Project(Fragment=[$0], Records=[$1], Path=[$2], Metadata=[$3], Partition=[$4], FileSize=[$5], IcebergMetadata=[$6], fileschema=[$7], PartitionData=[$8]) : rowType = RecordType(VARCHAR(65536) Fragment, BIGINT Records, VARCHAR(65536) Path, VARBINARY(65536) Metadata, INTEGER Partition, BIGINT FileSize, VARBINARY(65536) IcebergMetadata, VARBINARY(65536) fileschema, VARBINARY(65536) ARRAY PartitionData): rowcount = 10000.0, cumulative cost = {60100.0 rows, 66001.5 cpu, 6000.0 io, 6000.0 network, 0.0 memory}, id = 216\n" +
      "00-02        WriterCommitter(final=[/Users/shabeer/git/dremio_copy_1/enterprise/distribution/server/target/dremio-enterprise-19.0.0-SNAPSHOT/dremio-enterprise-19.0.0-SNAPSHOT/data/pdfs/results/1ee182fe-c2eb-3830-c54e-e08363fd2200]) : rowType = RecordType(VARCHAR(65536) Fragment, BIGINT Records, VARCHAR(65536) Path, VARBINARY(65536) Metadata, INTEGER Partition, BIGINT FileSize, VARBINARY(65536) IcebergMetadata, VARBINARY(65536) fileschema, VARBINARY(65536) ARRAY PartitionData): rowcount = 10000.0, cumulative cost = {50100.0 rows, 66000.6 cpu, 6000.0 io, 6000.0 network, 0.0 memory}, id = 215\n" +
      "00-03          Writer : rowType = RecordType(VARCHAR(65536) Fragment, BIGINT Records, VARCHAR(65536) Path, VARBINARY(65536) Metadata, INTEGER Partition, BIGINT FileSize, VARBINARY(65536) IcebergMetadata, VARBINARY(65536) fileschema, VARBINARY(65536) ARRAY PartitionData): rowcount = 10000.0, cumulative cost = {40100.0 rows, 56000.600000000006 cpu, 6000.0 io, 6000.0 network, 0.0 memory}, id = 214\n" +
      "00-04            Project(role_name=[$0], source=[$1], role_type=[$2]) : rowType = RecordType(VARCHAR(65536) role_name, VARCHAR(65536) source, VARCHAR(65536) role_type): rowcount = 10000.0, cumulative cost = {30100.0 rows, 46000.600000000006 cpu, 6000.0 io, 6000.0 network, 0.0 memory}, id = 213\n" +
      "00-05              Project(role_name=[$0], source=[$1], role_type=[$2]) : rowType = RecordType(VARCHAR(65536) role_name, VARCHAR(65536) source, VARCHAR(65536) role_type): rowcount = 10000.0, cumulative cost = {20100.0 rows, 46000.3 cpu, 6000.0 io, 6000.0 network, 0.0 memory}, id = 212\n" +
      "00-06                Limit(offset=[0], fetch=[10000]) : rowType = RecordType(VARCHAR(65536) role_name, VARCHAR(65536) source, VARCHAR(65536) role_type): rowcount = 10000.0, cumulative cost = {10100.0 rows, 46000.0 cpu, 6000.0 io, 6000.0 network, 0.0 memory}, id = 211\n" +
      "00-07                  SystemScan(table=[sys.roles], columns=[`role_name`, `source`, `role_type`], splits=[1]) : rowType = RecordType(VARCHAR(65536) role_name, VARCHAR(65536) source, VARCHAR(65536) role_type): rowcount = 100.0, cumulative cost = {100.0 rows, 6000.0 cpu, 6000.0 io, 6000.0 network, 0.0 memory}, id = 210";

    String expectedRelTree = "00-00    Screen : rowType = RecordType(VARCHAR(65536) field_9da2e250, BIGINT field_40871e42, VARCHAR(65536) field_346425, VARBINARY(65536) field_e52d7b2f, INTEGER field_94b912ea, BIGINT field_d436b77d, VARBINARY(65536) field_531b76f2, VARBINARY(65536) field_a10a1e1d, VARBINARY(65536) field_29ce940d): rowcount = 10000.0, cumulative cost = {61100.0 rows, 67001.5 cpu, 6000.0 io, 6000.0 network, 0.0 memory}, id = 217\n" +
      "00-01      Project(field_9da2e250=[$0], field_40871e42=[$1], field_346425=[$2], field_e52d7b2f=[$3], field_94b912ea=[$4], field_d436b77d=[$5], field_531b76f2=[$6], field_a10a1e1d=[$7], field_9af07d94=[$8]): rowcount = 10000.0, cumulative cost = {60100.0 rows, 66001.5 cpu, 6000.0 io, 6000.0 network, 0.0 memory}, id = 216\n" +
      "00-02        WriterCommitter(final=[filepath_d56b4fd3]) : rowType = RecordType(VARCHAR(65536) field_9da2e250, BIGINT field_40871e42, VARCHAR(65536) field_346425, VARBINARY(65536) field_e52d7b2f, INTEGER field_94b912ea, BIGINT field_d436b77d, VARBINARY(65536) field_531b76f2, VARBINARY(65536) field_a10a1e1d, VARBINARY(65536) field_29ce940d): rowcount = 10000.0, cumulative cost = {50100.0 rows, 66000.6 cpu, 6000.0 io, 6000.0 network, 0.0 memory}, id = 215\n" +
      "00-03          Writer : rowType = RecordType(VARCHAR(65536) field_9da2e250, BIGINT field_40871e42, VARCHAR(65536) field_346425, VARBINARY(65536) field_e52d7b2f, INTEGER field_94b912ea, BIGINT field_d436b77d, VARBINARY(65536) field_531b76f2, VARBINARY(65536) field_a10a1e1d, VARBINARY(65536) field_29ce940d): rowcount = 10000.0, cumulative cost = {40100.0 rows, 56000.600000000006 cpu, 6000.0 io, 6000.0 network, 0.0 memory}, id = 214\n" +
      "00-04            Project(field_14048cb4=[$0], field_ca90681b=[$1], field_1407a163=[$2]): rowcount = 10000.0, cumulative cost = {30100.0 rows, 46000.600000000006 cpu, 6000.0 io, 6000.0 network, 0.0 memory}, id = 213\n" +
      "00-05              Project(field_14048cb4=[$0], field_ca90681b=[$1], field_1407a163=[$2]): rowcount = 10000.0, cumulative cost = {20100.0 rows, 46000.3 cpu, 6000.0 io, 6000.0 network, 0.0 memory}, id = 212\n" +
      "00-06                Limit(offset=[0], fetch=[10000]) : rowType = RecordType(VARCHAR(65536) field_14048cb4, VARCHAR(65536) field_ca90681b, VARCHAR(65536) field_1407a163): rowcount = 10000.0, cumulative cost = {10100.0 rows, 46000.0 cpu, 6000.0 io, 6000.0 network, 0.0 memory}, id = 211\n" +
      "00-07                  SystemScan(table=[\"1becd\".\"67a8ebd\"], columns=[`field_14048cb4`, `field_ca90681b`, `field_1407a163`], splits=[1]) : rowType = RecordType(VARCHAR(65536) field_14048cb4, VARCHAR(65536) field_ca90681b, VARCHAR(65536) field_1407a163): rowcount = 100.0, cumulative cost = {100.0 rows, 6000.0 cpu, 6000.0 io, 6000.0 network, 0.0 memory}, id = 210\n";

    ObfuscationUtils.setFullObfuscation(true);

    String observedRelTree =
      RequestContext.current()
      .with(SupportContext.CTX_KEY, new SupportContext("dummy", "dummy", new String[]{ SupportContext.SupportRole.BASIC_SUPPORT_ROLE.getValue() }))
      .call(() -> RelCleanser.redactRelTree(relTree));
      assertEquals("Obfuscating of relTree failed.", expectedRelTree, observedRelTree);
  }
}
