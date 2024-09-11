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
package com.dremio.plugins.dataplane.store;

import com.dremio.exec.catalog.conf.SecretRef;
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import java.util.concurrent.TimeUnit;
import org.projectnessie.client.http.HttpAuthentication;
import org.projectnessie.client.http.HttpClient;
import org.projectnessie.client.http.RequestContext;

/** HttpAuthentication implementation that uses SecretRef to keep token fresh. */
public class SecureBearerAuthentication implements HttpAuthentication {

  private final Supplier<String> accessToken;

  public SecureBearerAuthentication(SecretRef accessToken) {
    this.accessToken = Suppliers.memoizeWithExpiration(accessToken::get, 5, TimeUnit.MINUTES);
  }

  @Override
  public void applyToHttpClient(HttpClient.Builder client) {
    client.addRequestFilter(this::applyToHttpRequest);
  }

  @Override
  public void applyToHttpRequest(RequestContext context) {
    context.putHeader("Authorization", "Bearer " + accessToken.get());
  }
}
