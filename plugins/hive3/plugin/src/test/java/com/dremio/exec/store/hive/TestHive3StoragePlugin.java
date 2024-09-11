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

import static org.apache.hadoop.hive.conf.HiveConf.ConfVars.HIVE_SERVER2_ENABLE_DOAS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.dremio.common.config.SabotConfig;
import com.dremio.common.exceptions.UserException;
import com.dremio.config.DremioConfig;
import com.dremio.connector.metadata.BytesOutput;
import com.dremio.connector.metadata.DatasetHandle;
import com.dremio.connector.metadata.DatasetMetadata;
import com.dremio.connector.metadata.DatasetStats;
import com.dremio.connector.metadata.EntityPath;
import com.dremio.connector.metadata.extensions.SupportsReadSignature;
import com.dremio.exec.ExecConstants;
import com.dremio.exec.catalog.CatalogOptions;
import com.dremio.exec.record.BatchSchema;
import com.dremio.exec.server.SabotContext;
import com.dremio.hive.proto.HiveReaderProto;
import com.dremio.options.OptionManager;
import com.dremio.service.users.SystemUser;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Streams;
import com.google.common.math.LongMath;
import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hive.common.util.Ref;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

/** Tests for Hive3StoragePlugin */
public class TestHive3StoragePlugin {
  private static final String TEST_USER_NAME = "testUser";

  @Test
  public void impersonationDisabledShouldReturnSystemUser() {
    final HiveConf hiveConf = new HiveConf();
    hiveConf.setBoolVar(HIVE_SERVER2_ENABLE_DOAS, false);
    final SabotContext context = mock(SabotContext.class);

    final Hive3StoragePlugin plugin = createHiveStoragePlugin(hiveConf, context);

    final String userName = plugin.getUsername(TEST_USER_NAME);
    assertEquals(SystemUser.SYSTEM_USERNAME, userName);
  }

  @Test
  public void impersonationEnabledShouldReturnUser() {
    final HiveConf hiveConf = new HiveConf();
    hiveConf.setBoolVar(HIVE_SERVER2_ENABLE_DOAS, true);
    final SabotContext context = mock(SabotContext.class);

    final Hive3StoragePlugin plugin = new Hive3StoragePlugin(hiveConf, context, "foo");

    final String userName = plugin.getUsername(TEST_USER_NAME);
    assertEquals(TEST_USER_NAME, userName);
  }

  protected Hive3StoragePlugin createHiveStoragePlugin(HiveConf hiveConf, SabotContext context) {
    return new Hive3StoragePlugin(hiveConf, context, "foo");
  }

  @Test
  public void testValidateMetadataTimeoutCombinations() throws Exception {
    testValidateMetadataTimeout(2_000L, 2L);
    testValidateMetadataTimeout(1_500L, 4L);

    // No. of partitions less than parallelism. Effective parallelism should be adjusted to no. of
    // partitions (4).
    testValidateMetadataTimeout(100L, 32L);
  }

  @Test
  public void testClientPoolOff() throws Exception {
    // tests the legacy setting, poolsize = 0
    // 5: {1 processUserMetastoreClient used by the 5 threads in a serial way: 5}
    // after a retry on failure, + 1 processUserMetastoreClient: 1
    testClientPool(0, ImmutableList.of(5), ImmutableList.of(1));
  }

  @Test
  public void testClientPoolOn() throws Exception {
    // 2, 2, 1, 0 = { 3 clients in the pool used by the 5 threads: 2, 2, 1 times,
    // 1 processUserMetastoreClient unused: 0 }
    // after a retry on failure, + 1 processUserMetastoreClient, + 1 client from a cleaned pool
    testClientPool(3, ImmutableList.of(2, 2, 1, 0), ImmutableList.of(1, 1));
  }

  private void testClientPool(
      int clientPoolSize,
      Collection<Integer> expectedCalls,
      Collection<Integer> expectedCallsAfterError)
      throws Exception {
    int threadCount = 5;
    // mocking client operation time
    long waitTime = 500;
    Ref<Boolean> shouldFailOnetime = new Ref<>(false);

    Hive3StoragePlugin storagePlugin = testPlugin(clientPoolSize);

    Map<HiveClient, AtomicInteger> hiveClients = new ConcurrentHashMap<>();
    Mockito.doAnswer(
            invocationOnStoragePlugin -> {
              HiveClient client = mock(HiveClient.class);
              Mockito.doAnswer(
                      invocationOnClient -> {
                        if (shouldFailOnetime.value) {
                          shouldFailOnetime.value = false;
                          throw new IOException();
                        } else {
                          hiveClients.get(invocationOnClient.getMock()).incrementAndGet();
                          Thread.sleep(waitTime);
                          return null;
                        }
                      })
                  .when(client)
                  .checkState(anyBoolean());
              hiveClients.put(client, new AtomicInteger(0));
              return client;
            })
        .when(storagePlugin)
        .createConnectedClient();

    storagePlugin.start();

    // run checkClientState() with more multiple threads than objects available in the pool
    Callable<Void> c =
        () -> {
          storagePlugin.checkClientState();
          return null;
        };

    Future<Void>[] futures = new Future[threadCount];
    ExecutorService executor = null;
    try {
      executor = Executors.newFixedThreadPool(threadCount);
      for (int i = 0; i < threadCount; ++i) {
        futures[i] = executor.submit(c);
      }

      // wait for all calls to finish
      for (int i = 0; i < threadCount; ++i) {
        futures[i].get();
      }
    } finally {
      executor.shutdown();
    }

    Assert.assertEquals(expectedCalls.size(), hiveClients.size());
    Assert.assertEquals(
        0,
        CollectionUtils.subtract(
                hiveClients.values().stream().map(i -> i.get()).collect(Collectors.toList()),
                expectedCalls)
            .size());

    // upon failure, should discard all objects from pool, then create and add a new one
    shouldFailOnetime.value = true;
    storagePlugin.checkClientState();

    Assert.assertEquals(expectedCalls.size() + expectedCallsAfterError.size(), hiveClients.size());
    Assert.assertEquals(
        0,
        CollectionUtils.subtract(
                hiveClients.values().stream().map(i -> i.get()).collect(Collectors.toList()),
                Streams.concat(expectedCalls.stream(), expectedCallsAfterError.stream())
                    .collect(Collectors.toList()))
            .size());
  }

  @Test
  public void testValidateDatabaseExists() throws Exception {
    final HiveConf hiveConf = new HiveConf();
    final SabotContext context = mock(SabotContext.class);
    final Hive3StoragePlugin plugin = createHiveStoragePlugin(hiveConf, context);
    final HiveClient hiveClient = mock(HiveClient.class);
    when(hiveClient.databaseExists("existing")).thenReturn(true);
    when(hiveClient.databaseExists("not_existing")).thenReturn(false);
    assertDoesNotThrow(() -> plugin.validateDatabaseExists(hiveClient, "existing"));
    UserException ex =
        assertThrows(
            UserException.class, () -> plugin.validateDatabaseExists(hiveClient, "not_existing"));
    assertEquals("Database does not exist: [not_existing]", ex.getOriginalMessage());
  }

  private void testValidateMetadataTimeout(Long timePerCheck, Long parallelism) throws Exception {
    // File and partition specifics are encoded in the metadata and the signature. Listing here for
    // explanation.
    int noOfPartitions = 4;
    int noOfFilesPerPartition = 2;
    int totalChecks = noOfPartitions * (1 + noOfFilesPerPartition); // one check for parent folder
    Long effectiveParallelism = Math.min(parallelism, noOfPartitions);
    long expectedTotalTimeout =
        LongMath.checkedMultiply(
            (long) Math.ceil(totalChecks / effectiveParallelism), timePerCheck);

    // Initialize plugin with mocks
    Hive3StoragePlugin storagePlugin = testPlugin(parallelism, timePerCheck);

    ArgumentCaptor<Long> expectedTotalTimeoutArgument = ArgumentCaptor.forClass(Long.class);
    ArgumentCaptor<Integer> effectiveParallelismArgument = ArgumentCaptor.forClass(Integer.class);
    // Hit the validation method.
    SupportsReadSignature.MetadataValidity validity =
        storagePlugin.validateMetadata(
            getReadSignature(), getTestDatasetHandle(Lists.newArrayList("t2")), getMockMetadata());
    verify(storagePlugin)
        .runValidations(
            any(DatasetHandle.class),
            any(List.class),
            effectiveParallelismArgument.capture(),
            expectedTotalTimeoutArgument.capture());
    assertEquals(Long.valueOf(expectedTotalTimeout), expectedTotalTimeoutArgument.getValue());
    assertEquals(
        Integer.valueOf(effectiveParallelism.intValue()), effectiveParallelismArgument.getValue());
    assertEquals(SupportsReadSignature.MetadataValidity.VALID, validity);
  }

  public Hive3StoragePlugin testPlugin(long parallelism, long timeoutMS) throws Exception {
    return testPlugin(parallelism, timeoutMS, 0);
  }

  public Hive3StoragePlugin testPlugin(long clientPoolSize) throws Exception {
    return testPlugin(0, 0, clientPoolSize);
  }

  private Hive3StoragePlugin testPlugin(long parallelism, long timeoutMS, long clientPoolSize)
      throws Exception {
    SabotContext ctx = mock(SabotContext.class);
    when(ctx.isCoordinator()).thenReturn(true);
    SabotConfig sabotConfig = mock(SabotConfig.class);
    when(ctx.getConfig()).thenReturn(sabotConfig);
    OptionManager optionManager = mock(OptionManager.class);
    Mockito.doReturn(clientPoolSize)
        .when(optionManager)
        .getOption(Hive3PluginOptions.HIVE_CLIENT_POOL_SIZE);
    Mockito.doReturn(true)
        .when(optionManager)
        .getOption(CatalogOptions.RETRY_CONNECTION_ON_FAILURE);

    when(ctx.getOptionManager()).thenReturn(optionManager);
    DremioConfig dremioConfig = mock(DremioConfig.class);
    when(ctx.getDremioConfig()).thenReturn(dremioConfig);

    HiveConf conf = new HiveConf();
    conf.setBoolVar(HiveConf.ConfVars.HIVE_SERVER2_ENABLE_DOAS, true);
    conf.setBoolVar(HiveConf.ConfVars.HIVE_AUTHORIZATION_ENABLED, false);
    conf.setBoolVar(HiveConf.ConfVars.METASTORE_EXECUTE_SET_UGI, false);
    conf.setBoolVar(HiveConf.ConfVars.METASTORE_USE_THRIFT_SASL, false);

    when(optionManager.getOption(ExecConstants.HIVE_SIGNATURE_VALIDATION_PARALLELISM))
        .thenReturn(parallelism);
    when(optionManager.getOption(ExecConstants.HIVE_SIGNATURE_VALIDATION_TIMEOUT_MS))
        .thenReturn(timeoutMS);

    Hive3StoragePlugin storagePlugin = spy(new Hive3StoragePlugin(conf, ctx, "testplugin"));
    Mockito.doReturn(SupportsReadSignature.MetadataValidity.VALID)
        .when(storagePlugin)
        .checkHiveMetadata(
            any(HiveReaderProto.HiveTableXattr.class),
            any(EntityPath.class),
            any(BatchSchema.class),
            any(HiveReaderProto.HiveReadSignature.class));
    Mockito.doReturn(Lists.newArrayList(false, false, false, false))
        .when(storagePlugin)
        .runValidations(
            any(DatasetHandle.class), any(List.class), any(Integer.class), any(Long.class));
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
    return new Base64ToOutput(
        "CAESlAESCwoAEI205tKuLhgBEhMKCDAwMDAwMF8wEO6o5dKuLhgAEhoKDzAwMDAwMF8wX2NvcHlfMRD/pObSri4YABpSaGRmczovL2hhcnNoLWhpdmUtMC5jLmRyZW1pby0xMDkzLmludGVybmFsOjgwMjAvZGF0YS9kcmVtaW8vdGVzdGRhdGEvcDE9cDExL3AyPXAyMSAAEpQBEgsKABDCuujSri4YARITCggwMDAwMDBfMBDeoufSri4YABIaCg8wMDAwMDBfMF9jb3B5XzEQy6bo0q4uGAAaUmhkZnM6Ly9oYXJzaC1oaXZlLTAuYy5kcmVtaW8tMTA5My5pbnRlcm5hbDo4MDIwL2RhdGEvZHJlbWlvL3Rlc3RkYXRhL3AxPXAxMS9wMj1wMjIgARKUARILCgAQibfq0q4uGAESEwoIMDAwMDAwXzAQq6np0q4uGAASGgoPMDAwMDAwXzBfY29weV8xEPmm6tKuLhgAGlJoZGZzOi8vaGFyc2gtaGl2ZS0wLmMuZHJlbWlvLTEwOTMuaW50ZXJuYWw6ODAyMC9kYXRhL2RyZW1pby90ZXN0ZGF0YS9wMT1wMTIvcDI9cDIxIAISlAESCwoAEJm27dKuLhgBEhMKCDAwMDAwMF8wEJmm69KuLhgAEhoKDzAwMDAwMF8wX2NvcHlfMRCVo+3Sri4YABpSaGRmczovL2hhcnNoLWhpdmUtMC5jLmRyZW1pby0xMDkzLmludGVybmFsOjgwMjAvZGF0YS9kcmVtaW8vdGVzdGRhdGEvcDE9cDEyL3AyPXAyMiAD");
  }

  private BytesOutput getMetaExtraInfo() {
    return new Base64ToOutput(
        "KAI45dT9ngdAsPiT8ANIAVIXCg1jb2x1bW5zLnR5cGVzEgZzdHJpbmdSXgoIbG9jYXRpb24SUmhkZnM6Ly9oYXJzaC1oaXZlLTAuYy5kcmVtaW8tMTA5My5pbnRlcm5hbDo4MDIwL2RhdGEvZHJlbWlvL3Rlc3RkYXRhL3AxPXAxMS9wMj1wMjFSDQoHY29sdW1ucxICaWRSKAoXcGFydGl0aW9uX2NvbHVtbnMudHlwZXMSDXN0cmluZzpzdHJpbmdSLwoVQ09MVU1OX1NUQVRTX0FDQ1VSQVRFEhZ7IkJBU0lDX1NUQVRTIjoidHJ1ZSJ9UhkKFHNlcmlhbGl6YXRpb24uZm9ybWF0EgExUgwKB251bVJvd3MSATJSDQoIbnVtRmlsZXMSATJSKwoRc2VyaWFsaXphdGlvbi5kZGwSFnN0cnVjdCB0MiB7IHN0cmluZyBpZH1SIwoVdHJhbnNpZW50X2xhc3REZGxUaW1lEgoxNTkzMDY5NTM5UhAKC3Jhd0RhdGFTaXplEgEyUhQKEGNvbHVtbnMuY29tbWVudHMSAFIQCgl0b3RhbFNpemUSAzQ3MFISCgxidWNrZXRfY291bnQSAi0xUlMKEWZpbGUub3V0cHV0Zm9ybWF0Ej5vcmcuYXBhY2hlLmhhZG9vcC5oaXZlLnFsLmlvLnBhcnF1ZXQuTWFwcmVkUGFycXVldE91dHB1dEZvcm1hdFJQChFzZXJpYWxpemF0aW9uLmxpYhI7b3JnLmFwYWNoZS5oYWRvb3AuaGl2ZS5xbC5pby5wYXJxdWV0LnNlcmRlLlBhcnF1ZXRIaXZlU2VyRGVSGgoRcGFydGl0aW9uX2NvbHVtbnMSBXAxL3AyUlEKEGZpbGUuaW5wdXRmb3JtYXQSPW9yZy5hcGFjaGUuaGFkb29wLmhpdmUucWwuaW8ucGFycXVldC5NYXByZWRQYXJxdWV0SW5wdXRGb3JtYXRSDwoEbmFtZRIHbXlkYi50MlJeCghsb2NhdGlvbhJSaGRmczovL2hhcnNoLWhpdmUtMC5jLmRyZW1pby0xMDkzLmludGVybmFsOjgwMjAvZGF0YS9kcmVtaW8vdGVzdGRhdGEvcDE9cDExL3AyPXAyMlJeCghsb2NhdGlvbhJSaGRmczovL2hhcnNoLWhpdmUtMC5jLmRyZW1pby0xMDkzLmludGVybmFsOjgwMjAvZGF0YS9kcmVtaW8vdGVzdGRhdGEvcDE9cDEyL3AyPXAyMVJeCghsb2NhdGlvbhJSaGRmczovL2hhcnNoLWhpdmUtMC5jLmRyZW1pby0xMDkzLmludGVybmFsOjgwMjAvZGF0YS9kcmVtaW8vdGVzdGRhdGEvcDE9cDEyL3AyPXAyMlJQCghsb2NhdGlvbhJEaGRmczovL2hhcnNoLWhpdmUtMC5jLmRyZW1pby0xMDkzLmludGVybmFsOjgwMjAvZGF0YS9kcmVtaW8vdGVzdGRhdGFaPW9yZy5hcGFjaGUuaGFkb29wLmhpdmUucWwuaW8ucGFycXVldC5NYXByZWRQYXJxdWV0SW5wdXRGb3JtYXRqO29yZy5hcGFjaGUuaGFkb29wLmhpdmUucWwuaW8ucGFycXVldC5zZXJkZS5QYXJxdWV0SGl2ZVNlckRlcBJwAHACcAVwCHALcANwEHANcA9wEXAOcBZwCXgAiAEAkgEICAsQABgAIAGaAR8KGWVuYWJsZV92YXJjaGFyX3RydW5jYXRpb24SAhgA");
  }

  private static final class Base64ToOutput implements BytesOutput {

    private final String base64Encoded;

    Base64ToOutput(String base64Encoded) {
      this.base64Encoded = base64Encoded;
    }

    @Override
    public void writeTo(OutputStream os) throws IOException {
      os.write(Base64.getDecoder().decode(base64Encoded));
    }
  }
}
