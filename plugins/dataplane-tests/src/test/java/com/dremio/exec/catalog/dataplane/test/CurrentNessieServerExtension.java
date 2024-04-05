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
package com.dremio.exec.catalog.dataplane.test;

import static org.projectnessie.tools.compatibility.internal.NessieTestApiBridge.nessieServer;
import static org.projectnessie.tools.compatibility.internal.NessieTestApiBridge.populateAnnotatedFields;
import static org.projectnessie.tools.compatibility.internal.NessieTestApiBridge.versionsFromResource;

import com.google.common.base.Preconditions;
import java.net.URI;
import java.util.SortedSet;
import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.BeforeEachCallback;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.projectnessie.client.rest.v1.HttpApiV1;
import org.projectnessie.junit.engine.MultiEnvTestExtension;
import org.projectnessie.tools.compatibility.api.NessieBaseUri;
import org.projectnessie.tools.compatibility.api.Version;
import org.projectnessie.tools.compatibility.internal.OlderNessieServersExtension;

/**
 * Instantiates a Nessie server at current version and populates fields annotated with {@link
 * NessieBaseUri} with the configured URI of the provided Nessie server.
 *
 * <p>Very similar to {@link OlderNessieServersExtension}, but necessary to limit usage to a single
 * version. Meant for use in combination with something like {@link
 * MultipleDataplaneStorageExtension} that is a {@link MultiEnvTestExtension}. Cartesian products of
 * multiple dimensions of environments are not supported yet, so we need to limit dimensions to just
 * one.
 */
public class CurrentNessieServerExtension implements BeforeAllCallback, BeforeEachCallback {

  @Override
  public void beforeAll(ExtensionContext extensionContext) {
    populateFields(extensionContext, null);
  }

  @Override
  public void beforeEach(ExtensionContext extensionContext) throws Exception {
    populateFields(extensionContext, extensionContext.getRequiredTestInstance());
  }

  private void populateFields(ExtensionContext extensionContext, Object instance) {
    URI serverUri = nessieServer(extensionContext, getNessieVersion(), HttpApiV1.class);
    URI baseUri = serverUri.resolve("/"); // remove possible API version suffixes
    populateAnnotatedFields(
        extensionContext, instance, NessieBaseUri.class, a -> true, f -> baseUri);
  }

  private Version getNessieVersion() {
    SortedSet<Version> nessieVersions = versionsFromResource();
    Preconditions.checkArgument(
        nessieVersions.size() == 1,
        "CurrentNessieServerExtension expects a single Nessie version. Use OlderNessieServersExtension instead.");
    return nessieVersions.first();
  }
}
