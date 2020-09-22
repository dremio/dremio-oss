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
package com.dremio.exec.store.hive;

import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;

import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.hadoop.hive.conf.HiveConf;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import com.dremio.common.config.SabotConfig;
import com.dremio.config.DremioConfig;
import com.dremio.connector.metadata.BytesOutput;
import com.dremio.connector.metadata.DatasetHandle;
import com.dremio.connector.metadata.DatasetMetadata;
import com.dremio.connector.metadata.DatasetStats;
import com.dremio.connector.metadata.EntityPath;
import com.dremio.connector.metadata.extensions.SupportsReadSignature;
import com.dremio.exec.ExecConstants;
import com.dremio.exec.record.BatchSchema;
import com.dremio.exec.server.SabotContext;
import com.dremio.hive.proto.HiveReaderProto;
import com.dremio.options.OptionManager;
import com.google.common.collect.Lists;
import com.google.common.math.LongMath;

/**
 * Hive 2.x storage plugin configuration.
 */
public class HiveStoragePluginTest {

  @Test
  public void testValidateMetadataTimeoutCombinations() throws Exception{
    testValidateMetadataTimeout(2_000l, 2l);
    testValidateMetadataTimeout(1_500l, 4l);

    // No. of partitions less than parallelism. Effective parallelism should be adjusted to no. of partitions (4).
    testValidateMetadataTimeout(100l, 32l);
  }


  private void testValidateMetadataTimeout(Long timePerCheck, Long parallelism) throws Exception {
    // File and partition specifics are encoded in the metadata and the signature. Listing here for explanation.
    int noOfPartitions = 4;
    int noOfFilesPerPartition = 2;
    int totalChecks = noOfPartitions * (1 + noOfFilesPerPartition); // one check for parent folder
    Long effectiveParallelism = Math.min(parallelism, noOfPartitions);
    long expectedTotalTimeout = LongMath.checkedMultiply((long) Math.ceil(totalChecks / effectiveParallelism), timePerCheck);

    // Initialize plugin with mocks
    HiveStoragePlugin storagePlugin = testPlugin(parallelism, timePerCheck);

    ArgumentCaptor<Long> expectedTotalTimeoutArgument = ArgumentCaptor.forClass(Long.class);
    ArgumentCaptor<Integer> effectiveParallelismArgument = ArgumentCaptor.forClass(Integer.class);
    // Hit the validation method.
    SupportsReadSignature.MetadataValidity validity = storagePlugin.validateMetadata(getReadSignature(),
      getTestDatasetHandle(Lists.newArrayList("t2")), getMockMetadata());
    verify(storagePlugin).runValidations(any(DatasetHandle.class), any(List.class),
      effectiveParallelismArgument.capture(), expectedTotalTimeoutArgument.capture());
    assertEquals(Long.valueOf(expectedTotalTimeout), expectedTotalTimeoutArgument.getValue());
    assertEquals(Integer.valueOf(effectiveParallelism.intValue()), effectiveParallelismArgument.getValue());
    assertEquals(SupportsReadSignature.MetadataValidity.VALID, validity);
  }


  public HiveStoragePlugin testPlugin(long parallelism, long timeoutMS) throws Exception {
    SabotContext ctx = mock(SabotContext.class);
    when(ctx.isCoordinator()).thenReturn(true);
    SabotConfig sabotConfig = mock(SabotConfig.class);
    when(ctx.getConfig()).thenReturn(sabotConfig);
    OptionManager optionManager = mock(OptionManager.class);
    when(ctx.getOptionManager()).thenReturn(optionManager);
    DremioConfig dremioConfig = mock(DremioConfig.class);
    when(ctx.getDremioConfig()).thenReturn(dremioConfig);

    HiveConf conf = mock(HiveConf.class);
    when(conf.getBoolVar(HiveConf.ConfVars.HIVE_SERVER2_ENABLE_DOAS)).thenReturn(true);
    when(conf.getBoolVar(HiveConf.ConfVars.HIVE_AUTHORIZATION_ENABLED)).thenReturn(false);
    when(conf.getBoolVar(HiveConf.ConfVars.METASTORE_EXECUTE_SET_UGI)).thenReturn(false);
    when(conf.getBoolVar(HiveConf.ConfVars.METASTORE_USE_THRIFT_SASL)).thenReturn(false);

    when(optionManager.getOption(eq(ExecConstants.HIVE_SIGNATURE_VALIDATION_PARALLELISM))).thenReturn(parallelism);
    when(optionManager.getOption(eq(ExecConstants.HIVE_SIGNATURE_VALIDATION_TIMEOUT_MS))).thenReturn(timeoutMS);

    HiveStoragePlugin storagePlugin = spy(new HiveStoragePlugin(conf, ctx, "testplugin"));
    Mockito.doReturn(SupportsReadSignature.MetadataValidity.VALID).when(storagePlugin).checkHiveMetadata(any(HiveReaderProto.HiveTableXattr.class), any(EntityPath.class), any(BatchSchema.class));
    Mockito.doReturn(Lists.newArrayList(false, false, false, false))
      .when(storagePlugin)
      .runValidations(any(DatasetHandle.class), any(List.class), any(Integer.class), any(Long.class));
    return storagePlugin;
  }

  private DatasetHandle getTestDatasetHandle(final List<String> path) {
    return () -> new EntityPath(path);
  }

  private DatasetMetadata getMockMetadata() {
    // (stats, schema,  extraInfo)
    DatasetStats stats = mock(DatasetStats.class);
    Schema schema = mock(Schema.class);
    List<Field> fields = new ArrayList<>();
    fields.add(Field.nullablePrimitive("id", new ArrowType.Utf8()));
    fields.add(Field.nullablePrimitive("p1", new ArrowType.Utf8()));
    fields.add(Field.nullablePrimitive("p2", new ArrowType.Utf8()));
    when(schema.getFields()).thenReturn(fields);

    BytesOutput extraInfo = getMetaExtraInfo();
    return DatasetMetadata.of(stats, schema, extraInfo);
  }

  private BytesOutput getReadSignature() {
    return new Base64ToOutput("CAESlAESCwoAEI205tKuLhgBEhMKCDAwMDAwMF8wEO6o5dKuLhgAEhoKDzAwMDAwMF8wX2NvcHlfMRD/pObSri4YABpSaGRmczovL2hhcnNoLWhpdmUtMC5jLmRyZW1pby0xMDkzLmludGVybmFsOjgwMjAvZGF0YS9kcmVtaW8vdGVzdGRhdGEvcDE9cDExL3AyPXAyMSAAEpQBEgsKABDCuujSri4YARITCggwMDAwMDBfMBDeoufSri4YABIaCg8wMDAwMDBfMF9jb3B5XzEQy6bo0q4uGAAaUmhkZnM6Ly9oYXJzaC1oaXZlLTAuYy5kcmVtaW8tMTA5My5pbnRlcm5hbDo4MDIwL2RhdGEvZHJlbWlvL3Rlc3RkYXRhL3AxPXAxMS9wMj1wMjIgARKUARILCgAQibfq0q4uGAESEwoIMDAwMDAwXzAQq6np0q4uGAASGgoPMDAwMDAwXzBfY29weV8xEPmm6tKuLhgAGlJoZGZzOi8vaGFyc2gtaGl2ZS0wLmMuZHJlbWlvLTEwOTMuaW50ZXJuYWw6ODAyMC9kYXRhL2RyZW1pby90ZXN0ZGF0YS9wMT1wMTIvcDI9cDIxIAISlAESCwoAEJm27dKuLhgBEhMKCDAwMDAwMF8wEJmm69KuLhgAEhoKDzAwMDAwMF8wX2NvcHlfMRCVo+3Sri4YABpSaGRmczovL2hhcnNoLWhpdmUtMC5jLmRyZW1pby0xMDkzLmludGVybmFsOjgwMjAvZGF0YS9kcmVtaW8vdGVzdGRhdGEvcDE9cDEyL3AyPXAyMiAD");
  }

  private BytesOutput getMetaExtraInfo() {
    return new Base64ToOutput("KAI45dT9ngdAsPiT8ANIAVIXCg1jb2x1bW5zLnR5cGVzEgZzdHJpbmdSXgoIbG9jYXRpb24SUmhkZnM6Ly9oYXJzaC1oaXZlLTAuYy5kcmVtaW8tMTA5My5pbnRlcm5hbDo4MDIwL2RhdGEvZHJlbWlvL3Rlc3RkYXRhL3AxPXAxMS9wMj1wMjFSDQoHY29sdW1ucxICaWRSKAoXcGFydGl0aW9uX2NvbHVtbnMudHlwZXMSDXN0cmluZzpzdHJpbmdSLwoVQ09MVU1OX1NUQVRTX0FDQ1VSQVRFEhZ7IkJBU0lDX1NUQVRTIjoidHJ1ZSJ9UhkKFHNlcmlhbGl6YXRpb24uZm9ybWF0EgExUgwKB251bVJvd3MSATJSDQoIbnVtRmlsZXMSATJSKwoRc2VyaWFsaXphdGlvbi5kZGwSFnN0cnVjdCB0MiB7IHN0cmluZyBpZH1SIwoVdHJhbnNpZW50X2xhc3REZGxUaW1lEgoxNTkzMDY5NTM5UhAKC3Jhd0RhdGFTaXplEgEyUhQKEGNvbHVtbnMuY29tbWVudHMSAFIQCgl0b3RhbFNpemUSAzQ3MFISCgxidWNrZXRfY291bnQSAi0xUlMKEWZpbGUub3V0cHV0Zm9ybWF0Ej5vcmcuYXBhY2hlLmhhZG9vcC5oaXZlLnFsLmlvLnBhcnF1ZXQuTWFwcmVkUGFycXVldE91dHB1dEZvcm1hdFJQChFzZXJpYWxpemF0aW9uLmxpYhI7b3JnLmFwYWNoZS5oYWRvb3AuaGl2ZS5xbC5pby5wYXJxdWV0LnNlcmRlLlBhcnF1ZXRIaXZlU2VyRGVSGgoRcGFydGl0aW9uX2NvbHVtbnMSBXAxL3AyUlEKEGZpbGUuaW5wdXRmb3JtYXQSPW9yZy5hcGFjaGUuaGFkb29wLmhpdmUucWwuaW8ucGFycXVldC5NYXByZWRQYXJxdWV0SW5wdXRGb3JtYXRSDwoEbmFtZRIHbXlkYi50MlJeCghsb2NhdGlvbhJSaGRmczovL2hhcnNoLWhpdmUtMC5jLmRyZW1pby0xMDkzLmludGVybmFsOjgwMjAvZGF0YS9kcmVtaW8vdGVzdGRhdGEvcDE9cDExL3AyPXAyMlJeCghsb2NhdGlvbhJSaGRmczovL2hhcnNoLWhpdmUtMC5jLmRyZW1pby0xMDkzLmludGVybmFsOjgwMjAvZGF0YS9kcmVtaW8vdGVzdGRhdGEvcDE9cDEyL3AyPXAyMVJeCghsb2NhdGlvbhJSaGRmczovL2hhcnNoLWhpdmUtMC5jLmRyZW1pby0xMDkzLmludGVybmFsOjgwMjAvZGF0YS9kcmVtaW8vdGVzdGRhdGEvcDE9cDEyL3AyPXAyMlJQCghsb2NhdGlvbhJEaGRmczovL2hhcnNoLWhpdmUtMC5jLmRyZW1pby0xMDkzLmludGVybmFsOjgwMjAvZGF0YS9kcmVtaW8vdGVzdGRhdGFaPW9yZy5hcGFjaGUuaGFkb29wLmhpdmUucWwuaW8ucGFycXVldC5NYXByZWRQYXJxdWV0SW5wdXRGb3JtYXRqO29yZy5hcGFjaGUuaGFkb29wLmhpdmUucWwuaW8ucGFycXVldC5zZXJkZS5QYXJxdWV0SGl2ZVNlckRlcBJwAHACcAVwCHALcANwEHANcA9wEXAOcBZwCXgAiAEAkgEICAsQABgAIAGaAR8KGWVuYWJsZV92YXJjaGFyX3RydW5jYXRpb24SAhgA");
  }

  class Base64ToOutput implements BytesOutput {

    String base64Encoded;
    Base64ToOutput(String base64Encoded) {
      this.base64Encoded = base64Encoded;
    }

    @Override
    public void writeTo(OutputStream os) throws IOException {
      os.write(Base64.getDecoder().decode(base64Encoded));
    }
  }
}
