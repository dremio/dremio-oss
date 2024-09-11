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
package com.dremio.search.pubsub;

import com.dremio.service.search.SearchDocumentMessageProto;
import com.dremio.services.pubsub.Subscription;
import com.dremio.services.pubsub.Topic;
import com.google.protobuf.Parser;

/** Subscription to update search index. */
public final class SearchDocumentSubscription
    implements Subscription<SearchDocumentMessageProto.SearchDocumentMessage> {
  @Override
  public String getName() {
    return "search-document-index";
  }

  @Override
  public Parser<SearchDocumentMessageProto.SearchDocumentMessage> getMessageParser() {
    return SearchDocumentMessageProto.SearchDocumentMessage.parser();
  }

  @Override
  public Class<? extends Topic<SearchDocumentMessageProto.SearchDocumentMessage>> getTopicClass() {
    return SearchDocumentTopic.class;
  }
}
