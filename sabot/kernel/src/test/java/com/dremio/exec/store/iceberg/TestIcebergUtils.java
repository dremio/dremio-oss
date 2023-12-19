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

import static com.dremio.exec.ExecConstants.ICEBERG_CATALOG_TYPE_KEY;
import static com.dremio.exec.store.iceberg.IcebergUtils.getColumnName;
import static com.dremio.exec.store.iceberg.IcebergUtils.resolvePath;
import static com.dremio.exec.store.iceberg.IcebergUtils.toIcebergValue;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import com.dremio.common.exceptions.UserException;
import com.dremio.common.types.TypeProtos;
import com.dremio.common.types.Types;
import com.dremio.common.util.MajorTypeHelper;
import com.dremio.exec.hadoop.HadoopFileSystem;
import com.dremio.exec.planner.sql.PartitionTransform;
import com.dremio.exec.record.BatchSchema;
import com.dremio.exec.store.iceberg.model.IcebergCatalogType;
import com.dremio.io.file.FileSystem;
import com.google.common.collect.ImmutableList;

/**
Test class for IcebergUtils.java class
 */
public class TestIcebergUtils {
  private static final Configuration CONF = new Configuration();
  private static FileSystem fs;

  @BeforeClass
  public static void initStatics() throws Exception {
    CONF.set(ICEBERG_CATALOG_TYPE_KEY, IcebergCatalogType.HADOOP.name());
    fs = HadoopFileSystem.get(com.dremio.io.file.Path.of("/"), CONF);
  }

  private static final BatchSchema TEST_SCHEMA = BatchSchema.newBuilder()
    .addField(MajorTypeHelper.getFieldForNameAndMajorType("w", Types.optional(TypeProtos.MinorType.TIME)))
    .addField(MajorTypeHelper.getFieldForNameAndMajorType("x", Types.optional(TypeProtos.MinorType.INT)))
    .addField(MajorTypeHelper.getFieldForNameAndMajorType("y", Types.optional(TypeProtos.MinorType.VARCHAR)))
    .addField(MajorTypeHelper.getFieldForNameAndMajorType("z", Types.optional(TypeProtos.MinorType.TIMESTAMP)))
    .addField(MajorTypeHelper.getFieldForNameAndMajorType("_", Types.optional(TypeProtos.MinorType.TIMESTAMP)))
    .addField(MajorTypeHelper.getFieldForNameAndMajorType("test_", Types.optional(TypeProtos.MinorType.TIMESTAMP)))
    .addField(MajorTypeHelper.getFieldForNameAndMajorType("_identity", Types.optional(TypeProtos.MinorType.TIMESTAMP)))
    .addField(MajorTypeHelper.getFieldForNameAndMajorType("_hour", Types.optional(TypeProtos.MinorType.TIMESTAMP)))
    .build();

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

  @Test
  public void testGetIcebergPartitionSpecFromIdentityTransform() {
    List<PartitionTransform> transforms = ImmutableList.of(new PartitionTransform("x"));
    PartitionSpec spec = IcebergUtils.getIcebergPartitionSpecFromTransforms(TEST_SCHEMA, transforms, null);
    Assert.assertEquals(1, spec.fields().size());
    Assert.assertEquals("x", spec.fields().get(0).name());
    Assert.assertTrue(spec.fields().get(0).transform().isIdentity());
  }

  @Test
  public void testGetIcebergPartitionSpecFromYearTransform() {
    List<PartitionTransform> transforms = ImmutableList.of(new PartitionTransform("z", PartitionTransform.Type.YEAR));
    PartitionSpec spec = IcebergUtils.getIcebergPartitionSpecFromTransforms(TEST_SCHEMA, transforms, null);
    Assert.assertEquals(1, spec.fields().size());
    Assert.assertEquals("z_year", spec.fields().get(0).name());
    Assert.assertEquals("year", spec.fields().get(0).transform().toString());
  }

  @Test
  public void testGetIcebergPartitionSpecFromMonthTransform() {
    List<PartitionTransform> transforms = ImmutableList.of(new PartitionTransform("z", PartitionTransform.Type.MONTH));
    PartitionSpec spec = IcebergUtils.getIcebergPartitionSpecFromTransforms(TEST_SCHEMA, transforms, null);
    Assert.assertEquals(1, spec.fields().size());
    Assert.assertEquals("z_month", spec.fields().get(0).name());
    Assert.assertEquals("month", spec.fields().get(0).transform().toString());
  }

  @Test
  public void testGetIcebergPartitionSpecFromDayTransform() {
    List<PartitionTransform> transforms = ImmutableList.of(new PartitionTransform("z", PartitionTransform.Type.DAY));
    PartitionSpec spec = IcebergUtils.getIcebergPartitionSpecFromTransforms(TEST_SCHEMA, transforms, null);
    Assert.assertEquals(1, spec.fields().size());
    Assert.assertEquals("z_day", spec.fields().get(0).name());
    Assert.assertEquals("day", spec.fields().get(0).transform().toString());
  }

  @Test
  public void testGetIcebergPartitionSpecFromHourTransform() {
    List<PartitionTransform> transforms = ImmutableList.of(new PartitionTransform("z", PartitionTransform.Type.HOUR));
    PartitionSpec spec = IcebergUtils.getIcebergPartitionSpecFromTransforms(TEST_SCHEMA, transforms, null);
    Assert.assertEquals(1, spec.fields().size());
    Assert.assertEquals("z_hour", spec.fields().get(0).name());
    Assert.assertEquals("hour", spec.fields().get(0).transform().toString());
  }

  @Test
  public void testGetIcebergPartitionSpecFromBucketTransform() {
    List<PartitionTransform> transforms = ImmutableList.of(new PartitionTransform("x",
      PartitionTransform.Type.BUCKET, ImmutableList.of(10)));
    PartitionSpec spec = IcebergUtils.getIcebergPartitionSpecFromTransforms(TEST_SCHEMA, transforms, null);
    Assert.assertEquals(1, spec.fields().size());
    Assert.assertEquals("x_bucket", spec.fields().get(0).name());
    Assert.assertEquals("bucket[10]", spec.fields().get(0).transform().toString());
  }

  @Test
  public void testGetIcebergPartitionSpecFromTruncateTransform() {
    List<PartitionTransform> transforms = ImmutableList.of(new PartitionTransform("x",
      PartitionTransform.Type.TRUNCATE, ImmutableList.of(10)));
    PartitionSpec spec = IcebergUtils.getIcebergPartitionSpecFromTransforms(TEST_SCHEMA, transforms, null);
    Assert.assertEquals(1, spec.fields().size());
    Assert.assertEquals("x_trunc", spec.fields().get(0).name());
    Assert.assertEquals("truncate[10]", spec.fields().get(0).transform().toString());
  }

  @Test
  public void testGetIcebergPartitionSpecFromMultiTransforms() {
    List<PartitionTransform> transforms = ImmutableList.of(
      new PartitionTransform("x", PartitionTransform.Type.TRUNCATE, ImmutableList.of(10)),
      new PartitionTransform("y"),
      new PartitionTransform("z", PartitionTransform.Type.YEAR)
    );
    PartitionSpec spec = IcebergUtils.getIcebergPartitionSpecFromTransforms(TEST_SCHEMA, transforms, null);
    Assert.assertEquals(3, spec.fields().size());
    Assert.assertEquals("x_trunc", spec.fields().get(0).name());
    Assert.assertEquals("truncate[10]", spec.fields().get(0).transform().toString());
    Assert.assertEquals("y", spec.fields().get(1).name());
    Assert.assertEquals("z_year", spec.fields().get(2).name());
    Assert.assertEquals("year", spec.fields().get(2).transform().toString());
  }

  @Test
  public void testGetIcebergPartitionSpecFromTransformWithInvalidFields() {
    List<PartitionTransform> transforms = ImmutableList.of(new PartitionTransform("badcol"));
    assertThatThrownBy(() -> IcebergUtils.getIcebergPartitionSpecFromTransforms(TEST_SCHEMA, transforms, null))
      .isInstanceOf(UserException.class);
  }

  @Test
  public void testGetIcebergPartitionSpecFromTransformWithTimeTypeFails() {
    List<PartitionTransform> transforms = ImmutableList.of(new PartitionTransform("w"));
    assertThatThrownBy(() -> IcebergUtils.getIcebergPartitionSpecFromTransforms(TEST_SCHEMA, transforms, null))
      .isInstanceOf(UserException.class);
  }

  @Test
  public void testGetInvalidColumnsForPruning() {
    /*
    If a partition column has identity transformation in all the partition specs, that column can be pruned
    Scenarios:
    1. When a column ("y") has identity transform in all partition specs -> column not included in o/p
    2. When a column ("z") doesn't have identity transform in all partition specs -> column included in o/p
    3. When a column ("x") has identity transform in some partition specs, but not all -> column included in o/p
    4. When a column ("w") has only identity transform, but not in all the partition specs -> column included in o/p
    */
    SchemaConverter schemaConverter = SchemaConverter.getBuilder().build();
    Schema schema = schemaConverter.toIcebergSchema(TEST_SCHEMA);

    PartitionSpec.Builder builder = PartitionSpec.builderFor(schema);
    PartitionSpec spec1 = builder.withSpecId(0).identity("y").bucket("x", 10).month("z").build();

    builder = PartitionSpec.builderFor(schema);
    PartitionSpec spec2 = builder.withSpecId(1).identity("y").bucket("x", 10).identity("x").build();

    builder = PartitionSpec.builderFor(schema);
    PartitionSpec spec3 = builder.withSpecId(2).identity("y").bucket("x", 10).identity("x").day("z").identity("w").build();

    Map<Integer, PartitionSpec> partitionSpecMap = new HashMap<>();
    partitionSpecMap.put(0, spec1);
    partitionSpecMap.put(1, spec2);
    partitionSpecMap.put(2, spec3);

    Set<String> op = IcebergUtils.getInvalidColumnsForPruning(partitionSpecMap);
    Assert.assertTrue(op.contains("z"));
    Assert.assertFalse(op.contains("y"));
    Assert.assertTrue(op.contains("x"));
    Assert.assertTrue(op.contains("w"));
  }
  @Test
  public void testGetColumnNameIdentity() {
    final List<PartitionTransform> transforms = ImmutableList.of(new PartitionTransform("x"));
    final PartitionSpec spec = IcebergUtils.getIcebergPartitionSpecFromTransforms(TEST_SCHEMA, transforms, null);
    Assert.assertEquals("x",getColumnName(spec.fields().get(0), spec.schema()));
  }

  @Test
  public void testGetColumnNameYear() {
    final List<PartitionTransform> transforms = ImmutableList.of(new PartitionTransform("z", PartitionTransform.Type.YEAR));
    final PartitionSpec spec = IcebergUtils.getIcebergPartitionSpecFromTransforms(TEST_SCHEMA, transforms, null);
    Assert.assertEquals("z",getColumnName(spec.fields().get(0), spec.schema()));
  }

  @Test
  public void testGetColumnNameMonth() {
    final List<PartitionTransform> transforms = ImmutableList.of(new PartitionTransform("z", PartitionTransform.Type.MONTH));
    final PartitionSpec spec = IcebergUtils.getIcebergPartitionSpecFromTransforms(TEST_SCHEMA, transforms, null);
    Assert.assertEquals("z",getColumnName(spec.fields().get(0), spec.schema()));
  }

  @Test
  public void testGetColumnNameDay() {
    final List<PartitionTransform> transforms = ImmutableList.of(new PartitionTransform("z", PartitionTransform.Type.DAY));
    final PartitionSpec spec = IcebergUtils.getIcebergPartitionSpecFromTransforms(TEST_SCHEMA, transforms, null);
    Assert.assertEquals("z",getColumnName(spec.fields().get(0), spec.schema()));
  }

  @Test
  public void testGetColumnNameHour() {
    final List<PartitionTransform> transforms = ImmutableList.of(new PartitionTransform("z", PartitionTransform.Type.HOUR));
    final PartitionSpec spec = IcebergUtils.getIcebergPartitionSpecFromTransforms(TEST_SCHEMA, transforms, null);
    Assert.assertEquals("z",getColumnName(spec.fields().get(0), spec.schema()));
  }

  @Test
  public void testGetColumnNameBucket() {
    final List<PartitionTransform> transforms = ImmutableList.of(new PartitionTransform("x",
      PartitionTransform.Type.BUCKET, ImmutableList.of(10)));
    final PartitionSpec spec = IcebergUtils.getIcebergPartitionSpecFromTransforms(TEST_SCHEMA, transforms, null);
    Assert.assertEquals("x",getColumnName(spec.fields().get(0), spec.schema()));
  }

  @Test
  public void testGetColumnNameTruncate() {
    final List<PartitionTransform> transforms = ImmutableList.of(new PartitionTransform("x",
      PartitionTransform.Type.TRUNCATE, ImmutableList.of(10)));
    final PartitionSpec spec = IcebergUtils.getIcebergPartitionSpecFromTransforms(TEST_SCHEMA, transforms, null);
    Assert.assertEquals("x",getColumnName(spec.fields().get(0), spec.schema()));
  }

  @Test
  public void testGetColumnNameUnderscoreHour() {
    final List<PartitionTransform> transforms = ImmutableList.of(new PartitionTransform("_", PartitionTransform.Type.HOUR));
    final PartitionSpec spec = IcebergUtils.getIcebergPartitionSpecFromTransforms(TEST_SCHEMA, transforms, null);
    Assert.assertEquals("_",getColumnName(spec.fields().get(0), spec.schema()));
  }

@Test
  public void testGetColumnNameTestUnderscoreHour() {
    final List<PartitionTransform> transforms = ImmutableList.of(new PartitionTransform("test_", PartitionTransform.Type.HOUR));
    final PartitionSpec spec = IcebergUtils.getIcebergPartitionSpecFromTransforms(TEST_SCHEMA, transforms, null);
    Assert.assertEquals("test_",getColumnName(spec.fields().get(0), spec.schema()));
  }

  @Test
  public void testGetColumnNameUnderscoreHourHour() {
    final List<PartitionTransform> transforms = ImmutableList.of(new PartitionTransform("_hour", PartitionTransform.Type.HOUR));
    final PartitionSpec spec = IcebergUtils.getIcebergPartitionSpecFromTransforms(TEST_SCHEMA, transforms, null);
    Assert.assertEquals("_hour",getColumnName(spec.fields().get(0), spec.schema()));
  }

  @Test
  public void testGetColumnNameTestUnderscoreIdenitytyHour() {
    final List<PartitionTransform> transforms = ImmutableList.of(new PartitionTransform("_identity", PartitionTransform.Type.HOUR));
    final PartitionSpec spec = IcebergUtils.getIcebergPartitionSpecFromTransforms(TEST_SCHEMA, transforms, null);
    Assert.assertEquals("_identity",getColumnName(spec.fields().get(0), spec.schema()));
  }

  @Test
  public void testGetColumnNameUnderscoreIdentity() {
    final List<PartitionTransform> transforms = ImmutableList.of(new PartitionTransform("_"));
    final PartitionSpec spec = IcebergUtils.getIcebergPartitionSpecFromTransforms(TEST_SCHEMA, transforms, null);
    Assert.assertEquals("_",getColumnName(spec.fields().get(0), spec.schema()));
  }

  @Test
  public void testGetColumnNameTestUnderscoreIdentity() {
    final List<PartitionTransform> transforms = ImmutableList.of(new PartitionTransform("test_"));
    final PartitionSpec spec = IcebergUtils.getIcebergPartitionSpecFromTransforms(TEST_SCHEMA, transforms, null);
    Assert.assertEquals("test_",getColumnName(spec.fields().get(0), spec.schema()));
  }

  @Test
  public void testGetColumnNameUnderscoreHourIdentity() {
    final List<PartitionTransform> transforms = ImmutableList.of(new PartitionTransform("_hour"));
    final PartitionSpec spec = IcebergUtils.getIcebergPartitionSpecFromTransforms(TEST_SCHEMA, transforms, null);
    Assert.assertEquals("_hour",getColumnName(spec.fields().get(0), spec.schema()));
  }

  @Test
  public void testGetColumnNameTestUnderscoreIdentityIdentity() {
    final List<PartitionTransform> transforms = ImmutableList.of(new PartitionTransform("_identity"));
    final PartitionSpec spec = IcebergUtils.getIcebergPartitionSpecFromTransforms(TEST_SCHEMA, transforms, null);
    Assert.assertEquals("_identity",getColumnName(spec.fields().get(0), spec.schema()));
  }
  @Test
  public void testToIcebergValueLong(){
    final Long valueLong = 1_000L;
    //for timestamp conversion happens
    Assert.assertEquals(1_000_000L,toIcebergValue(valueLong,
      TypeProtos.MajorType.newBuilder().setMinorType(TypeProtos.MinorType.TIMESTAMP).build()));

    //no conversion for the rest of the types
    Assert.assertEquals(1_000L,toIcebergValue(valueLong,
      TypeProtos.MajorType.newBuilder().setMinorType(TypeProtos.MinorType.BIGINT).build()));
   final  Integer valueInt = 1_000;
    Assert.assertEquals(1_000,toIcebergValue(valueInt,
      TypeProtos.MajorType.newBuilder().setMinorType(TypeProtos.MinorType.INT).build()));
  }
}
