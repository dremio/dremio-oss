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
package com.dremio.services.pubsub.inprocess;

import com.dremio.options.OptionManager;
import com.dremio.services.pubsub.PubSubClient;
import io.opentelemetry.api.OpenTelemetry;
import javax.annotation.Nullable;
import javax.inject.Inject;
import javax.inject.Provider;
import javax.inject.Singleton;

@Singleton
public class InProcessPubSubClientProvider {
  private final Provider<OptionManager> optionManagerProvider;
  private final OpenTelemetry openTelemetry;
  @Nullable private final Provider<InProcessPubSubEventListener> eventListenerProvider;

  private InProcessPubSubClient pubSubClient;

  @Inject
  public InProcessPubSubClientProvider(
      Provider<OptionManager> optionManagerProvider,
      OpenTelemetry openTelemetry,
      @Nullable Provider<InProcessPubSubEventListener> eventListenerProvider) {
    this.optionManagerProvider = optionManagerProvider;
    this.openTelemetry = openTelemetry;
    this.eventListenerProvider = eventListenerProvider;
  }

  public synchronized PubSubClient get() {
    if (pubSubClient == null) {
      pubSubClient =
          new InProcessPubSubClient(
              optionManagerProvider.get(),
              openTelemetry,
              eventListenerProvider != null ? eventListenerProvider.get() : null);
    }
    return pubSubClient;
  }
}
