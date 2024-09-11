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
package com.dremio.services.credentials;

import com.dremio.telemetry.api.metrics.MeterProviders;
import com.dremio.telemetry.api.metrics.TimerUtils;
import com.google.common.base.Stopwatch;
import com.google.inject.Inject;
import com.google.inject.Provider;
import io.micrometer.core.instrument.Timer.ResourceSample;
import java.net.URI;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A credentials provider for System Secrets exposing system secret decryption allowing encrypted
 * secrets to be decrypted on lookup.
 */
public class SystemSecretCredentialsProvider extends AbstractSimpleCredentialsProvider
    implements CredentialsProvider {

  private static final Logger logger =
      LoggerFactory.getLogger(SystemSecretCredentialsProvider.class);

  private static final Supplier<ResourceSample> LOOKUP_TIMER =
      MeterProviders.newTimerResourceSampleSupplier(
          "credentials_service.credentials_provider.system_secrets.lookup",
          "Time taken to decrypt system secrets");

  static final String SECRET_PROVIDER_SCHEME = "system";

  private final Provider<Cipher> systemCipher;
  private final CredentialsProviderContext context;

  @Override
  public boolean isSupported(URI uri) {
    return isSystemEncrypted(uri);
  }

  @Inject
  public SystemSecretCredentialsProvider(
      Provider<Cipher> systemCipher, CredentialsProviderContext context) {
    super(SECRET_PROVIDER_SCHEME);
    this.systemCipher = systemCipher;
    this.context = context;
  }

  @Override
  protected String doLookup(URI uri) throws CredentialsException {
    final Stopwatch watch = Stopwatch.createUnstarted();
    if (logger.isDebugEnabled()) {
      watch.start();
    }

    final ResourceSample sample = LOOKUP_TIMER.get();
    final boolean isRemoteLookup = !context.isLeader();
    sample.tag("is_remote_lookup", "" + isRemoteLookup);

    if (isRemoteLookup) { // always go to leader, do not depend on isRemoteEnabled
      return TimerUtils.timedExceptionThrowingOperation(
          sample,
          () -> {
            final String resolved = context.lookupOnLeader(uri.toString());
            if (logger.isDebugEnabled()) {
              logger.debug(
                  "Remote lookup for system secret took {} ms",
                  watch.elapsed(TimeUnit.MILLISECONDS));
            }
            return resolved;
          });
    } else {
      return TimerUtils.timedExceptionThrowingOperation(
          sample,
          () -> {
            final String resolved = systemCipher.get().decrypt(uri.getSchemeSpecificPart());
            if (logger.isDebugEnabled()) {
              logger.debug(
                  "Local lookup for system secret took {} ms",
                  watch.elapsed(TimeUnit.MILLISECONDS));
            }
            return resolved;
          });
    }
  }

  public static boolean isSystemEncrypted(URI uri) {
    return SECRET_PROVIDER_SCHEME.equalsIgnoreCase(uri.getScheme());
  }
}
