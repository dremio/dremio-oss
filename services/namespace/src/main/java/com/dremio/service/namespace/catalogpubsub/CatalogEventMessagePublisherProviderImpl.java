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
package com.dremio.service.namespace.catalogpubsub;

import com.dremio.options.OptionManager;
import com.dremio.search.SearchOptions;
import com.dremio.service.namespace.CatalogEventProto;
import com.dremio.services.pubsub.ImmutableMessagePublisherOptions;
import com.dremio.services.pubsub.MessagePublisher;
import com.dremio.services.pubsub.inprocess.InProcessPubSubClientProvider;
import javax.inject.Inject;
import javax.inject.Provider;
import javax.inject.Singleton;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Singleton
public final class CatalogEventMessagePublisherProviderImpl
    implements CatalogEventMessagePublisherProvider {
  private static final Logger logger =
      LoggerFactory.getLogger(CatalogEventMessagePublisherProviderImpl.class);

  private final InProcessPubSubClientProvider pubSubClientProvider;
  private final Provider<OptionManager> optionManagerProvider;

  private MessagePublisher<CatalogEventProto.CatalogEventMessage> messagePublisher;
  private boolean isEnabled;

  @Inject
  public CatalogEventMessagePublisherProviderImpl(
      InProcessPubSubClientProvider pubSubClientProvider,
      Provider<OptionManager> optionManagerProvider) {
    this.pubSubClientProvider = pubSubClientProvider;
    this.optionManagerProvider = optionManagerProvider;
  }

  @Override
  public synchronized MessagePublisher<CatalogEventProto.CatalogEventMessage> get() {
    if (messagePublisher == null) {
      // Perform one-time setup
      initMessagePublisher();
      optionManagerProvider
          .get()
          .addOptionChangeListener(
              () -> {
                // If the option value has changed, re-initialize the message publisher
                if (isEnabled ^ isFeatureEnabled()) {
                  logger.info("Support key value has changed.");
                  initMessagePublisher();
                }
              });
    }
    return messagePublisher;
  }

  private void initMessagePublisher() {
    boolean featureEnabled = isFeatureEnabled();
    logger.info("{} CatalogEventMessagePublisher.", featureEnabled ? "Enabling" : "Disabling");
    isEnabled = featureEnabled;
    if (messagePublisher != null) {
      // Unsubscribe the existing one we will be replacing
      logger.debug("Closing existing message publisher.");
      messagePublisher.close();
    }
    messagePublisher =
        isEnabled
            ? pubSubClientProvider
                .get()
                .getPublisher(
                    CatalogEventsTopic.class,
                    new ImmutableMessagePublisherOptions.Builder().build())
            : NO_OP.get();
  }

  private boolean isFeatureEnabled() {
    return optionManagerProvider.get().getOption(SearchOptions.SEARCH_V2);
  }
}
