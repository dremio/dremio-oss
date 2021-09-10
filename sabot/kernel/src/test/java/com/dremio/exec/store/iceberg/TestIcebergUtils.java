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
package com.dremio.exec.store.iceberg;

import static com.dremio.exec.store.iceberg.IcebergUtils.resolvePath;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.junit.Assert;
import org.junit.Test;

import com.dremio.common.types.TypeProtos;
import com.dremio.common.types.Types;
import com.dremio.common.util.MajorTypeHelper;
import com.dremio.exec.record.BatchSchema;

/**
Test class for IcebergUtils.java class
 */
public class TestIcebergUtils {

  @Test
  public void getValidIcebergPathTest() {
    String testUrl = "/testdir/Automation/regression/iceberg/alltypes/metadata/snap-6325739561998439041.avro";

    Configuration azureConf = new Configuration();
    azureConf.set("dremio.azure.account", "azurev1databricks2");
    String modifiedFileLocation = IcebergUtils.getValidIcebergPath(new Path(testUrl), azureConf, "dremioAzureStorage://");
    Assert.assertEquals("wasbs://testdir@azurev1databricks2.blob.core.windows.net/Automation/regression/iceberg/alltypes/metadata/snap-6325739561998439041.avro", modifiedFileLocation);

    Configuration conf = new Configuration();

    modifiedFileLocation = IcebergUtils.getValidIcebergPath(new Path(testUrl), conf, "dremioS3");
    Assert.assertEquals("s3://testdir/Automation/regression/iceberg/alltypes/metadata/snap-6325739561998439041.avro", modifiedFileLocation);

    modifiedFileLocation = IcebergUtils.getValidIcebergPath(new Path(testUrl), conf, "dremiogcs");
    Assert.assertEquals("gs://testdir/Automation/regression/iceberg/alltypes/metadata/snap-6325739561998439041.avro", modifiedFileLocation);

    Configuration hdfsConf = new Configuration();
    hdfsConf.set("fs.defaultFS", "hdfs://172.25.0.39:8020/");
    modifiedFileLocation = IcebergUtils.getValidIcebergPath(new Path(testUrl), hdfsConf, "hdfs");
    Assert.assertEquals("hdfs://172.25.0.39:8020/testdir/Automation/regression/iceberg/alltypes/metadata/snap-6325739561998439041.avro", modifiedFileLocation);

    modifiedFileLocation = IcebergUtils.getValidIcebergPath(new Path(testUrl), conf, "hdfs");
    Assert.assertEquals("hdfs:///testdir/Automation/regression/iceberg/alltypes/metadata/snap-6325739561998439041.avro", modifiedFileLocation);

    String urlWithScheme = "wasbs://testdir@azurev1databricks2.blob.core.windows.net/Automation/regression/iceberg/alltypes/metadata/snap-6325739561998439041.avro";
    modifiedFileLocation = IcebergUtils.getValidIcebergPath(new Path(urlWithScheme), conf, "dremioAzureStorage://");
    Assert.assertEquals(urlWithScheme, modifiedFileLocation);


    urlWithScheme = "s3://testdir/Automation/regression/iceberg/alltypes/metadata/snap-6325739561998439041.avro";
    modifiedFileLocation = IcebergUtils.getValidIcebergPath(new Path(urlWithScheme), conf, "dremioS3");
    Assert.assertEquals(urlWithScheme, modifiedFileLocation);

    urlWithScheme = "hdfs://172.25.0.39:8020/testdir/Automation/regression/iceberg/alltypes/metadata/snap-6325739561998439041.avro";
    modifiedFileLocation = IcebergUtils.getValidIcebergPath(new Path(urlWithScheme), conf, "hdfs");
    Assert.assertEquals(urlWithScheme, modifiedFileLocation);

    urlWithScheme = "file:/testdir/Automation/regression/iceberg/alltypes/metadata/snap-6325739561998439041.avro";
    modifiedFileLocation = IcebergUtils.getValidIcebergPath(new Path(urlWithScheme), conf, "file");
    Assert.assertEquals(urlWithScheme, modifiedFileLocation);

    Configuration adlsConf = new Configuration();
    adlsConf.set("fs.defaultFS", "dremioAdl://accountname.azuredatalakestore.net/");
    modifiedFileLocation = IcebergUtils.getValidIcebergPath(new Path(testUrl), adlsConf, "dremioAdl");
    Assert.assertEquals("adl://accountname.azuredatalakestore.net/testdir/Automation/regression/iceberg/alltypes/metadata/snap-6325739561998439041.avro", modifiedFileLocation);

    Configuration emptyAdlsConf = new Configuration();
    modifiedFileLocation = IcebergUtils.getValidIcebergPath(new Path(testUrl), emptyAdlsConf, "dremioAdl");
    Assert.assertEquals("adl:///testdir/Automation/regression/iceberg/alltypes/metadata/snap-6325739561998439041.avro", modifiedFileLocation);

    Configuration onlyScheme = new Configuration();
    onlyScheme.set("fs.defaultFS", "dremioAdl://");
    modifiedFileLocation = IcebergUtils.getValidIcebergPath(new Path(testUrl), onlyScheme, "dremioAdl");
    Assert.assertEquals("adl:///testdir/Automation/regression/iceberg/alltypes/metadata/snap-6325739561998439041.avro", modifiedFileLocation);
  }

  @Test
  public void testPartitionStatsPathForInputWithNoScheme() {
    String rootPointer = "/tmp/metadata/v1.metadata.json";
    String metadata = "metadata-12345.json";
    Assert.assertEquals("/tmp/metadata/metadata-12345.json", resolvePath(rootPointer, metadata));
  }

  @Test
  public void testPartitionStatsPathForInputWithNoSchemeAndWhitespace() {
    String rootPointer = "/tmp/new metadata/v1.metadata.json";
    String metadata = "metadata-12345.json";
    Assert.assertEquals("/tmp/new metadata/metadata-12345.json", resolvePath(rootPointer, metadata));
  }

  @Test
  public void testPartitionStatsPathForInputWithNoSchemeAndRelativePath() {
    String rootPointer = "tmp/metadata/v1.metadata.json";
    String metadata = "metadata-12345.json";
    Assert.assertEquals("tmp/metadata/metadata-12345.json", resolvePath(rootPointer, metadata));
  }

  @Test
  public void testPartitionStatsPathForInputWithNoSchemeAndRelativePathAndWhitespace() {
    String rootPointer = "tmp/new metadata/v1.metadata.json";
    String metadata = "metadata-12345.json";
    Assert.assertEquals("tmp/new metadata/metadata-12345.json", resolvePath(rootPointer, metadata));
  }

  @Test
  public void testPartitionStatsPathForInputWithNoSchemeAndUnnormalizedPath() {
    String rootPointer = "../../tmp/metadata/v1.metadata.json";
    String metadata = "metadata-12345.json";
    Assert.assertEquals("../../tmp/metadata/metadata-12345.json", resolvePath(rootPointer, metadata));
  }

  @Test
  public void testPartitionStatsPathForInputWithNoSchemeAndUnnormalizedPathAndWhitespace() {
    String rootPointer = "../../tmp/new metadata/v1.metadata.json";
    String metadata = "metadata-12345.json";
    Assert.assertEquals("../../tmp/new metadata/metadata-12345.json", resolvePath(rootPointer, metadata));
  }

  @Test
  public void testPartitionStatsPathForInputWithFileScheme() {
    String rootPointer = "file:///tmp/metadata/v1.metadata.json";
    String metadata = "metadata-12345.json";
    Assert.assertEquals("file:///tmp/metadata/metadata-12345.json", resolvePath(rootPointer, metadata));
  }

  @Test
  public void testPartitionStatsPathForInputWithFileSchemeAndWhitespace() {
    String rootPointer = "file:///tmp/new metadata/v1.metadata.json";
    String metadata = "metadata-12345.json";
    Assert.assertEquals("file:///tmp/new metadata/metadata-12345.json", resolvePath(rootPointer, metadata));
  }

  @Test
  public void testPartitionStatsPathForInputWithHdfsScheme() {
    String rootPointer = "hdfs:///tmp/metadata/v1.metadata.json";
    String metadata = "metadata-12345.json";
    Assert.assertEquals("hdfs:///tmp/metadata/metadata-12345.json", resolvePath(rootPointer, metadata));
  }

  @Test
  public void testPartitionStatsPathForInputWithHdfsSchemeAndWhitespace() {
    String rootPointer = "hdfs:///tmp/new metadata/v1.metadata.json";
    String metadata = "metadata-12345.json";
    Assert.assertEquals("hdfs:///tmp/new metadata/metadata-12345.json", resolvePath(rootPointer, metadata));
  }

  @Test
  public void testPartitionStatsPathForInputWithHdfsSchemeAndRelativePath() {
    String rootPointer = "hdfs://tmp/metadata/v1.metadata.json";
    String metadata = "metadata-12345.json";
    Assert.assertEquals("hdfs://tmp/metadata/metadata-12345.json", resolvePath(rootPointer, metadata));
  }

  @Test
  public void testPartitionStatsPathForInputWithHdfsSchemeAndRelativePathWithWhitespace() {
    String rootPointer = "hdfs://tmp/new metadata/v1.metadata.json";
    String metadata = "metadata-12345.json";
    Assert.assertEquals("hdfs://tmp/new metadata/metadata-12345.json", resolvePath(rootPointer, metadata));
  }

  @Test
  public void testPartitionStatsPathForInputWithHdfsSchemeAndHostPort() {
    String rootPointer = "hdfs://some-host:1234/tmp/metadata/v1.metadata.json";
    String metadata = "metadata-12345.json";
    Assert.assertEquals("hdfs://some-host:1234/tmp/metadata/metadata-12345.json", resolvePath(rootPointer, metadata));
  }

  @Test
  public void testPartitionStatsPathForInputWithHdfsSchemeAndHostPortWithWhitespace() {
    String rootPointer = "hdfs://some-host:1234/tmp/new metadata/v1.metadata.json";
    String metadata = "metadata-12345.json";
    Assert.assertEquals("hdfs://some-host:1234/tmp/new metadata/metadata-12345.json", resolvePath(rootPointer, metadata));
  }

  @Test
  public void testConvertSchemaMilliToMicro() {
    BatchSchema schema = BatchSchema.newBuilder()
      .addField(MajorTypeHelper.getFieldForNameAndMajorType("f0", Types.optional(TypeProtos.MinorType.INT)))
      .addField(MajorTypeHelper.getFieldForNameAndMajorType("f1", Types.optional(TypeProtos.MinorType.TIME)))
      .addField(MajorTypeHelper.getFieldForNameAndMajorType("f2", Types.optional(TypeProtos.MinorType.TIMESTAMP)))
      .build();

    Assert.assertEquals(schema.getColumn(0).getType(), org.apache.arrow.vector.types.Types.MinorType.INT.getType());
    Assert.assertEquals(schema.getColumn(1).getType(), org.apache.arrow.vector.types.Types.MinorType.TIMEMILLI.getType());
    Assert.assertEquals(schema.getColumn(2).getType(), org.apache.arrow.vector.types.Types.MinorType.TIMESTAMPMILLI.getType());

    // convert
    List<Field> fields = IcebergUtils.convertSchemaMilliToMicro(schema.getFields());
    Assert.assertEquals(fields.get(0).getType(), org.apache.arrow.vector.types.Types.MinorType.INT.getType());
    Assert.assertEquals(fields.get(1).getType(), org.apache.arrow.vector.types.Types.MinorType.TIMEMICRO.getType());
    Assert.assertEquals(fields.get(2).getType(), org.apache.arrow.vector.types.Types.MinorType.TIMESTAMPMICRO.getType());

    schema = BatchSchema.newBuilder()
      .addField(new Field("f0", FieldType.nullable(org.apache.arrow.vector.types.Types.MinorType.LIST.getType()), Collections.singletonList(
        Field.nullable("data", org.apache.arrow.vector.types.Types.MinorType.TIMEMILLI.getType())
      )))
      .addField(new Field("f1", FieldType.nullable(org.apache.arrow.vector.types.Types.MinorType.LIST.getType()), Collections.singletonList(
        Field.nullable("data", org.apache.arrow.vector.types.Types.MinorType.TIMESTAMPMILLI.getType())
      )))
      .build();

    // convert
    fields = IcebergUtils.convertSchemaMilliToMicro(schema.getFields());
    Assert.assertEquals(fields.get(0).getChildren().get(0).getType(), org.apache.arrow.vector.types.Types.MinorType.TIMEMICRO.getType());
    Assert.assertEquals(fields.get(1).getChildren().get(0).getType(), org.apache.arrow.vector.types.Types.MinorType.TIMESTAMPMICRO.getType());

    schema = BatchSchema.newBuilder()
      .addField(new Field("f0", FieldType.nullable(org.apache.arrow.vector.types.Types.MinorType.STRUCT.getType()), Arrays.asList(
        Field.nullable("c0", org.apache.arrow.vector.types.Types.MinorType.TIMEMILLI.getType()),
        Field.nullable("c1", org.apache.arrow.vector.types.Types.MinorType.TIMESTAMPMILLI.getType())
      )))
      .build();

    // convert
    fields = IcebergUtils.convertSchemaMilliToMicro(schema.getFields());
    Assert.assertEquals(fields.get(0).getChildren().get(0).getType(), org.apache.arrow.vector.types.Types.MinorType.TIMEMICRO.getType());
    Assert.assertEquals(fields.get(0).getChildren().get(1).getType(), org.apache.arrow.vector.types.Types.MinorType.TIMESTAMPMICRO.getType());
  }
}
