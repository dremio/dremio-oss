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
package com.dremio.exec.catalog.conf;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.dremio.exec.catalog.StoragePluginId;
import com.dremio.exec.server.SabotContext;
import com.dremio.exec.store.StoragePlugin;
import com.dremio.services.credentials.CredentialsService;
import io.protostuff.Tag;
import java.util.LinkedList;
import java.util.List;
import javax.inject.Provider;
import org.junit.Assert;
import org.junit.Test;

public class TestConnectionConf {
  private static final class TestingConnectionConf
      extends ConnectionConf<TestingConnectionConf, StoragePlugin> {
    @Override
    public StoragePlugin newPlugin(
        SabotContext context, String name, Provider<StoragePluginId> pluginIdProvider) {
      return null;
    }

    @Tag(1)
    public String someField;

    @Tag(2)
    @Secret
    public String someSecretField;

    @Tag(3)
    @Secret
    List<Property> someSecretPropertyListField;
  }

  @Test
  public void testResolveSecrets() throws Exception {
    final TestingConnectionConf connectionConf = new TestingConnectionConf();
    connectionConf.someField = "someFieldString";
    connectionConf.someSecretField = "someSecretString";

    final CredentialsService credentialsService = mock(CredentialsService.class);
    when(credentialsService.lookup(connectionConf.someSecretField))
        .thenReturn("resolvedSecretString");

    final TestingConnectionConf resolvedConf =
        connectionConf.resolveSecrets(credentialsService, null);

    Assert.assertEquals("resolvedSecretString", resolvedConf.someSecretField);
  }

  @Test
  public void testResolveSecretsOnPropertyList() throws Exception {
    final TestingConnectionConf connectionConf = new TestingConnectionConf();

    Property prop1 = new Property("someSecretPropertyName0", "resolvedSecretPropertyVal0");
    Property prop2 = new Property("someSecretPropertyName1", "resolvedSecretPropertyVal1");
    List<Property> secretPropertyList = new LinkedList<>();
    secretPropertyList.add(prop1);
    secretPropertyList.add(prop2);
    connectionConf.someSecretPropertyListField = secretPropertyList;

    final CredentialsService credentialsService = mock(CredentialsService.class);

    when(credentialsService.lookup(connectionConf.someSecretPropertyListField.get(0).value))
        .thenReturn("resolvedSecretPropertyVal0");
    when(credentialsService.lookup(connectionConf.someSecretPropertyListField.get(1).value))
        .thenReturn("resolvedSecretPropertyVal1");

    final TestingConnectionConf resolvedConf =
        connectionConf.resolveSecrets(credentialsService, null);

    Assert.assertEquals(
        "resolvedSecretPropertyVal0", resolvedConf.someSecretPropertyListField.get(0).value);
    Assert.assertEquals(
        "resolvedSecretPropertyVal1", resolvedConf.someSecretPropertyListField.get(1).value);
  }

  @Test
  public void testResolveSecretsBadURI() throws Exception {
    final TestingConnectionConf connectionConf = new TestingConnectionConf();
    connectionConf.someField = "someFieldString";
    connectionConf.someSecretField = "someSecret%.String";

    final CredentialsService credentialsService = mock(CredentialsService.class);
    when(credentialsService.lookup(connectionConf.someSecretField))
        .thenThrow(new IllegalArgumentException("Bad URI"));

    final TestingConnectionConf resolvedConf =
        connectionConf.resolveSecrets(credentialsService, null);

    Assert.assertEquals(connectionConf.someSecretField, resolvedConf.someSecretField);
  }

  @Test
  public void testResolvedSecretsOnPropertyListBadURI() throws Exception {
    final TestingConnectionConf connectionConf = new TestingConnectionConf();

    Property prop1 = new Property("someSecretPropertyName0", "resolvedSecret%.PropertyVal0");
    Property prop2 = new Property("someSecretPropertyName1", "resolvedSecret%.PropertyVal1");
    List<Property> secretPropertyList = new LinkedList<>();
    secretPropertyList.add(prop1);
    secretPropertyList.add(prop2);
    connectionConf.someSecretPropertyListField = secretPropertyList;

    final CredentialsService credentialsService = mock(CredentialsService.class);

    when(credentialsService.lookup(connectionConf.someSecretPropertyListField.get(0).value))
        .thenThrow(new IllegalArgumentException("Bad URI"));
    when(credentialsService.lookup(connectionConf.someSecretPropertyListField.get(1).value))
        .thenThrow(new IllegalArgumentException("Bad URI"));

    final TestingConnectionConf resolvedConf =
        connectionConf.resolveSecrets(credentialsService, null);

    Assert.assertEquals(
        connectionConf.someSecretPropertyListField.get(0).value,
        resolvedConf.someSecretPropertyListField.get(0).value);
    Assert.assertEquals(
        connectionConf.someSecretPropertyListField.get(1).value,
        resolvedConf.someSecretPropertyListField.get(1).value);
  }

  @Test
  public void testResolveSecretsOnComboOfFieldTypes() throws Exception {
    final TestingConnectionConf connectionConf = new TestingConnectionConf();
    connectionConf.someField = "someFieldString";
    connectionConf.someSecretField = "someSecretString";

    Property prop1 = new Property("someSecretPropertyName0", "resolvedSecretPropertyVal0");
    Property prop2 = new Property("someSecretPropertyName1", "resolvedSecretPropertyVal1");
    List<Property> secretPropertyList = new LinkedList<>();
    secretPropertyList.add(prop1);
    secretPropertyList.add(prop2);
    connectionConf.someSecretPropertyListField = secretPropertyList;

    final CredentialsService credentialsService = mock(CredentialsService.class);
    when(credentialsService.lookup(connectionConf.someSecretField))
        .thenReturn("resolvedSecretString");

    when(credentialsService.lookup(connectionConf.someSecretPropertyListField.get(0).value))
        .thenReturn("resolvedSecretPropertyVal0");
    when(credentialsService.lookup(connectionConf.someSecretPropertyListField.get(1).value))
        .thenReturn("resolvedSecretPropertyVal1");

    final TestingConnectionConf resolvedConf =
        connectionConf.resolveSecrets(credentialsService, null);

    Assert.assertEquals("resolvedSecretString", resolvedConf.someSecretField);

    Assert.assertEquals(
        "resolvedSecretPropertyVal0", resolvedConf.someSecretPropertyListField.get(0).value);
    Assert.assertEquals(
        "resolvedSecretPropertyVal1", resolvedConf.someSecretPropertyListField.get(1).value);
  }

  @Test
  public void testResolveSecretsOnComboOfFieldTypesBadURI() throws Exception {
    final TestingConnectionConf connectionConf = new TestingConnectionConf();
    connectionConf.someField = "someFieldString";
    connectionConf.someSecretField = "someSecret%.String";

    Property prop1 = new Property("someSecretPropertyName0", "resolvedSecret%.PropertyVal0");
    Property prop2 = new Property("someSecretPropertyName1", "resolvedSecret%.PropertyVal1");
    List<Property> secretPropertyList = new LinkedList<>();
    secretPropertyList.add(prop1);
    secretPropertyList.add(prop2);
    connectionConf.someSecretPropertyListField = secretPropertyList;

    final CredentialsService credentialsService = mock(CredentialsService.class);
    when(credentialsService.lookup(connectionConf.someSecretField))
        .thenThrow(new IllegalArgumentException("Bad URI"));
    when(credentialsService.lookup(connectionConf.someSecretPropertyListField.get(0).value))
        .thenThrow(new IllegalArgumentException("Bad URI"));
    when(credentialsService.lookup(connectionConf.someSecretPropertyListField.get(1).value))
        .thenThrow(new IllegalArgumentException("Bad URI"));

    final TestingConnectionConf resolvedConf =
        connectionConf.resolveSecrets(credentialsService, null);

    Assert.assertEquals(connectionConf.someSecretField, resolvedConf.someSecretField);
    Assert.assertEquals(
        connectionConf.someSecretPropertyListField.get(0).value,
        resolvedConf.someSecretPropertyListField.get(0).value);
    Assert.assertEquals(
        connectionConf.someSecretPropertyListField.get(1).value,
        resolvedConf.someSecretPropertyListField.get(1).value);
  }
}
