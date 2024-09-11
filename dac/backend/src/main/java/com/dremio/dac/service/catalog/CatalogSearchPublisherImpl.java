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
package com.dremio.dac.service.catalog;

import com.dremio.common.util.Closeable;
import com.dremio.dac.model.common.NamespacePath;
import com.dremio.dac.service.collaboration.CollaborationTagStore;
import com.dremio.dac.service.collaboration.CollaborationWikiStore;
import com.dremio.datastore.api.LegacyKVStoreProvider;
import com.dremio.exec.planner.sql.CalciteArrowHelper;
import com.dremio.search.pubsub.SearchDocumentTopic;
import com.dremio.service.namespace.CatalogEventProto;
import com.dremio.service.namespace.NamespaceException;
import com.dremio.service.namespace.NamespaceInvalidStateException;
import com.dremio.service.namespace.NamespaceKey;
import com.dremio.service.namespace.NamespaceNotFoundException;
import com.dremio.service.namespace.NamespaceService;
import com.dremio.service.namespace.catalogpubsub.CatalogEventsSearchSubscription;
import com.dremio.service.namespace.dataset.proto.DatasetConfig;
import com.dremio.service.namespace.function.proto.FunctionConfig;
import com.dremio.service.namespace.proto.NameSpaceContainer;
import com.dremio.service.namespace.source.proto.SourceConfig;
import com.dremio.service.search.SearchDocumentIdProto;
import com.dremio.service.search.SearchDocumentMessageProto;
import com.dremio.service.search.SearchDocumentProto;
import com.dremio.services.pubsub.ImmutableMessagePublisherOptions;
import com.dremio.services.pubsub.ImmutableMessageSubscriberOptions;
import com.dremio.services.pubsub.MessageContainerBase;
import com.dremio.services.pubsub.MessagePublisher;
import com.dremio.services.pubsub.MessageSubscriber;
import com.dremio.services.pubsub.PubSubClient;
import com.dremio.services.pubsub.inprocess.InProcessPubSubClientProvider;
import com.google.common.collect.ImmutableList;
import com.google.protobuf.Timestamp;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;
import javax.inject.Inject;
import javax.inject.Provider;
import javax.inject.Singleton;
import org.apache.arrow.vector.types.pojo.Field;

/**
 * Listens to catalog events, converts them to search documents and publishes the documents to
 * search index subscriber.
 */
@Singleton
public class CatalogSearchPublisherImpl implements CatalogSearchPublisher, Closeable {
  private static final org.slf4j.Logger logger =
      org.slf4j.LoggerFactory.getLogger(CatalogSearchPublisherImpl.class);

  private final Provider<NamespaceService> namespaceServiceProvider;
  private final LegacyKVStoreProvider legacyKVStoreProvider;
  private final InProcessPubSubClientProvider pubSubClientProvider;
  private CollaborationWikiStore wikiStore;
  private CollaborationTagStore tagStore;

  private MessageSubscriber<CatalogEventProto.CatalogEventMessage> catalogEventSubscriber;
  private MessagePublisher<SearchDocumentMessageProto.SearchDocumentMessage>
      searchDocumentPublisher;

  @Inject
  public CatalogSearchPublisherImpl(
      Provider<NamespaceService> namespaceServiceProvider,
      LegacyKVStoreProvider legacyKVStoreProvider,
      InProcessPubSubClientProvider pubSubClientProvider) {
    this.namespaceServiceProvider = namespaceServiceProvider;
    this.legacyKVStoreProvider = legacyKVStoreProvider;
    this.pubSubClientProvider = pubSubClientProvider;
  }

  @Override
  public void start() {
    logger.info("Starting");

    this.wikiStore = new CollaborationWikiStore(legacyKVStoreProvider);
    this.tagStore = new CollaborationTagStore(legacyKVStoreProvider);

    PubSubClient pubSubClient = pubSubClientProvider.get();
    catalogEventSubscriber =
        pubSubClient.getSubscriber(
            CatalogEventsSearchSubscription.class,
            this::processCatalogEvent,
            new ImmutableMessageSubscriberOptions.Builder().build());
    catalogEventSubscriber.start();
    searchDocumentPublisher =
        pubSubClient.getPublisher(
            SearchDocumentTopic.class,
            // Batch search docs before sending them to the search doc subscriber.
            new ImmutableMessagePublisherOptions.Builder()
                .setEnableBatching(true)
                .setBatchSize(1000)
                .setBatchDelay(Duration.ofMillis(500))
                .build());
  }

  private void processCatalogEvent(
      MessageContainerBase<CatalogEventProto.CatalogEventMessage> message) {
    try {
      // Convert catalog events to search documents and publish them to search index.
      for (SearchDocumentMessageProto.SearchDocumentMessage documentMessage :
          catalogEventToDocumentMessages(message)) {
        searchDocumentPublisher.publish(documentMessage).get();
      }

      // Mark message as processed successfully.
      message.ack().get();
    } catch (NamespaceException e) {
      // Acknowledge not to redeliver: either the namespace entity is not found or is invalid.
      logger.error("Acking catalog event message {}", message, e);
      try {
        message.ack().get();
      } catch (ExecutionException | InterruptedException ex) {
        logger.warn("Ignoring exception while acking", ex);
      }
    } catch (Exception e) {
      // Ask pubsub to redeliver the message.
      logger.warn("Nacking catalog event message {}", message, e);
      try {
        message.nack().get();
      } catch (ExecutionException | InterruptedException ex) {
        logger.warn("Ignoring exception while nacking", ex);
      }
    }
  }

  /** Converts catalog event(s) to search document message(s). */
  private List<SearchDocumentMessageProto.SearchDocumentMessage> catalogEventToDocumentMessages(
      MessageContainerBase<CatalogEventProto.CatalogEventMessage> message)
      throws NamespaceException {
    CatalogEventProto.CatalogEventMessage catalogEventMessage = message.getMessage();
    ImmutableList.Builder<SearchDocumentMessageProto.SearchDocumentMessage> resultListBuilder =
        ImmutableList.builder();
    for (CatalogEventProto.CatalogEventMessage.CatalogEvent event :
        catalogEventMessage.getEventsList()) {
      // Document id contains only path for non-versioned catalog entities.
      String path = NamespacePath.defaultImpl(event.getPathList()).toPathString();
      SearchDocumentIdProto.SearchDocumentId documentId =
          SearchDocumentIdProto.SearchDocumentId.newBuilder().setPath(path).build();
      SearchDocumentMessageProto.SearchDocumentMessage searchDocumentMessage;
      switch (event.getEventType()) {
        case CATALOG_EVENT_TYPE_DELETED:
          // Message to delete document.
          searchDocumentMessage =
              SearchDocumentMessageProto.SearchDocumentMessage.newBuilder()
                  .setDocumentId(documentId)
                  .setEventType(
                      SearchDocumentMessageProto.SearchDocumentMessage.SearchDocumentEventType
                          .SEARCH_DOCUMENT_EVENT_TYPE_DELETED)
                  .build();
          break;
        case CATALOG_EVENT_TYPE_CREATED:
        case CATALOG_EVENT_TYPE_UPDATED:
          searchDocumentMessage =
              SearchDocumentMessageProto.SearchDocumentMessage.newBuilder()
                  .setEventType(
                      event.getEventType()
                              == CatalogEventProto.CatalogEventMessage.CatalogEventType
                                  .CATALOG_EVENT_TYPE_CREATED
                          ? SearchDocumentMessageProto.SearchDocumentMessage.SearchDocumentEventType
                              .SEARCH_DOCUMENT_EVENT_TYPE_CREATED
                          : SearchDocumentMessageProto.SearchDocumentMessage.SearchDocumentEventType
                              .SEARCH_DOCUMENT_EVENT_TYPE_UPDATED)
                  .setDocumentId(documentId)
                  .setDocument(convertEventToDocument(message, event))
                  .build();
          break;
        default:
          throw new RuntimeException(
              String.format("Unexpected catalog event type %s for %s", event.getEventType(), path));
      }
      resultListBuilder.add(searchDocumentMessage);
    }
    return resultListBuilder.build();
  }

  /**
   * Convert catalog event to search document. Allow derived classes to override to augment the doc.
   */
  protected SearchDocumentProto.SearchDocument convertEventToDocument(
      MessageContainerBase<CatalogEventProto.CatalogEventMessage> message,
      CatalogEventProto.CatalogEventMessage.CatalogEvent event)
      throws NamespaceException {
    NameSpaceContainer container =
        namespaceServiceProvider.get().getEntityByPath(new NamespaceKey(event.getPathList()));
    if (container == null) {
      throw new NamespaceNotFoundException(
          new NamespaceKey(event.getPathList()), "No entity to convert to search doc");
    }

    SearchDocumentProto.CatalogObject.Builder catalogObjectBuilder =
        SearchDocumentProto.CatalogObject.newBuilder()
            .setPath(NamespacePath.defaultImpl(event.getPathList()).toPathString());

    // Get entity id, timestamps, and columns.
    String entityId;
    Long createdAt = null;
    Long lastModified = null;
    switch (container.getType()) {
      case FUNCTION:
        FunctionConfig functionConfig = container.getFunction();
        entityId = functionConfig.getId().getId();
        createdAt = functionConfig.getCreatedAt();
        lastModified = functionConfig.getLastModified();
        if (!functionConfig.getFunctionDefinitionsList().isEmpty()) {
          // For search include function body only, int the example:
          //   CREATE FUNCTION multiply(INT x, INT y) RETURNS SELECT x*y
          // the text after RETURNS.
          catalogObjectBuilder.setUdfSql(
              functionConfig.getFunctionDefinitionsList().get(0).getFunctionBody().getRawBody());
        }
        catalogObjectBuilder.setType("UDF");
        break;
      case DATASET:
        DatasetConfig datasetConfig = container.getDataset();
        entityId = datasetConfig.getId().getId();
        createdAt = datasetConfig.getCreatedAt();
        lastModified = datasetConfig.getLastModified();

        // Column names from schema.
        catalogObjectBuilder.addAllColumns(
            CalciteArrowHelper.fromDataset(datasetConfig).getFields().stream()
                .map(Field::getName)
                .collect(Collectors.toUnmodifiableList()));

        catalogObjectBuilder.setType(datasetConfig.getVirtualDataset() != null ? "VIEW" : "TABLE");
        break;
      case SOURCE:
        SourceConfig sourceConfig = container.getSource();
        entityId = sourceConfig.getId().getId();
        createdAt = sourceConfig.getCtime();
        lastModified = sourceConfig.getLastModifiedAt();
        catalogObjectBuilder.setType("SOURCE");
        break;
      case FOLDER:
        entityId = container.getFolder().getId().getId();
        catalogObjectBuilder.setType("FOLDER");
        break;
      case SPACE:
        entityId = container.getSpace().getId().getId();
        catalogObjectBuilder.setType("SPACE");
        break;
      case HOME:
        entityId = container.getHome().getId().getId();
        catalogObjectBuilder.setType("SPACE");
        break;
      default:
        // Throw NamespaceException to ack catalog event.
        throw new NamespaceInvalidStateException(
            String.format(
                "Container %s has invalid type: %s", event.getPathList(), container.getType()));
    }

    // Timestamps.
    if (createdAt != null) {
      catalogObjectBuilder.setCreated(timestampFromMillis(createdAt));
    }
    if (lastModified != null) {
      catalogObjectBuilder.setLastModified(timestampFromMillis(lastModified));
    }

    // Wiki and labels.
    wikiStore
        .getLatestWikiForEntityId(entityId)
        .ifPresent(wiki -> catalogObjectBuilder.setWiki(wiki.getText()));
    tagStore
        .getTagsForEntityId(entityId)
        .ifPresent(labels -> catalogObjectBuilder.addAllLabels(labels.getTagsList()));

    return SearchDocumentProto.SearchDocument.newBuilder()
        .setCatalogObject(catalogObjectBuilder.build())
        .build();
  }

  private static Timestamp timestampFromMillis(long epochMillis) {
    return Timestamp.newBuilder()
        .setSeconds(epochMillis / 1000)
        .setNanos((int) ((epochMillis % 1000) * 1000_000))
        .build();
  }

  @Override
  public void close() {
    if (catalogEventSubscriber != null) {
      catalogEventSubscriber.close();
    }
  }
}
