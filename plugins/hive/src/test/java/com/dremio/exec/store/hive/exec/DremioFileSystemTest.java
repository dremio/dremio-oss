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

import static org.apache.hadoop.fs.azurebfs.constants.ConfigurationKeys.*;

import org.apache.hadoop.conf.Configuration;
import org.junit.Assert;
import org.junit.Test;

public class DremioFileSystemTest {

  public static final String ENDPOINT = "endpoint";
  public static final String CLIEN_ID = "clienId";
  public static final String PASSWORD = "password";

  @Test
  public void testAzureOauthCopy() {
    DremioFileSystem fileSystem = new DremioFileSystem();
    Configuration conf = new Configuration();
    conf.set(FS_AZURE_ACCOUNT_OAUTH_CLIENT_ENDPOINT, ENDPOINT);
    conf.set(FS_AZURE_ACCOUNT_OAUTH_CLIENT_ID, CLIEN_ID);
    conf.set(FS_AZURE_ACCOUNT_OAUTH_CLIENT_SECRET, PASSWORD);


    fileSystem.updateOAuthConfig(conf, "testAccount.1234", "testAccount");
    Assert.assertEquals(ENDPOINT, conf.get("dremio.azure.tokenEndpoint"));
    Assert.assertEquals(CLIEN_ID, conf.get("dremio.azure.clientId"));
    Assert.assertEquals(PASSWORD, conf.get("dremio.azure.clientSecret"));

  }

  @Test
  public void testAzureOauthCopyWithAccountProperties() {
    DremioFileSystem fileSystem = new DremioFileSystem();
    Configuration conf = new Configuration();
    conf.set(FS_AZURE_ACCOUNT_OAUTH_CLIENT_ENDPOINT + ".testAccount", ENDPOINT);
    conf.set(FS_AZURE_ACCOUNT_OAUTH_CLIENT_ID + ".testAccount", CLIEN_ID);
    conf.set(FS_AZURE_ACCOUNT_OAUTH_CLIENT_SECRET + ".testAccount", PASSWORD);


    fileSystem.updateOAuthConfig(conf, "testAccount.1234", "testAccount");
    Assert.assertEquals(ENDPOINT, conf.get("dremio.azure.tokenEndpoint"));
    Assert.assertEquals(CLIEN_ID, conf.get("dremio.azure.clientId"));
    Assert.assertEquals(PASSWORD, conf.get("dremio.azure.clientSecret"));

  }
}
