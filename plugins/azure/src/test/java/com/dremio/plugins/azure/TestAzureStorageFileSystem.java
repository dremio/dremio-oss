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

package com.dremio.plugins.azure;

import static junit.framework.TestCase.assertEquals;
import static junit.framework.TestCase.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.powermock.api.mockito.PowerMockito.when;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.hadoop.conf.Configuration;
import org.asynchttpclient.AsyncHttpClient;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import com.dremio.plugins.azure.utils.AzureAsyncHttpClientUtils;

import io.netty.util.HashedWheelTimer;

/**
 * Tests for {@link AzureStorageFileSystem}
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest(AzureAsyncHttpClientUtils.class)
public class TestAzureStorageFileSystem {
  private static final String ACCOUNT = "testaccount";

  @Test
  public void testResourceClosures() throws IOException {
    AsyncHttpClient asyncHttpClient = mock(AsyncHttpClient.class);
    PowerMockito.mockStatic(AzureAsyncHttpClientUtils.class);
    AtomicReference<HashedWheelTimer> timerInstance = new AtomicReference<>();
    when(AzureAsyncHttpClientUtils.newClient(eq(ACCOUNT), eq(true), any(HashedWheelTimer.class))).then(invocationOnMock -> {
      timerInstance.set(invocationOnMock.getArgument(2, HashedWheelTimer.class));
      return asyncHttpClient;
    });

    AzureStorageFileSystem azureStorageFileSystem = new AzureStorageFileSystem();
    azureStorageFileSystem.setup(getMockHadoopConf());

    // Close
    azureStorageFileSystem.close();

    verify(asyncHttpClient, times(1)).close();
    try {
      timerInstance.get().start();
      fail("Timer cannot be started if it was stopped properly at resource closure");
    } catch (IllegalStateException e) {
      assertEquals("cannot be started once stopped", e.getMessage());
    }
  }

  private Configuration getMockHadoopConf() {
    Configuration conf = mock(Configuration.class);
    when(conf.get(eq("dremio.azure.mode"))).thenReturn("STORAGE_V2");
    when(conf.get(eq("fs.azure.endpoint"))).thenReturn("");
    when(conf.get(eq("dremio.azure.account"))).thenReturn(ACCOUNT);
    when(conf.get(eq("dremio.azure.key"))).thenReturn("accesskey");
    when(conf.get(eq("dremio.azure.credentialsType"))).thenReturn("ACCESS_KEY");
    when(conf.getBoolean(eq("dremio.azure.secure"), eq(true))).thenReturn(true);
    return conf;
  }
}
