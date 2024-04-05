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
package com.dremio.dac.service.collaboration;

import com.dremio.dac.proto.model.collaboration.CollaborationWiki;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

/** Collaboration wiki */
public class Wiki {
  private final Long version;
  private final String text;

  @JsonCreator
  public Wiki(@JsonProperty("text") String text, @JsonProperty("version") Long version) {
    this.text = text;
    this.version = version;
  }

  public Long getVersion() {
    return version;
  }

  public String getText() {
    return text;
  }

  public static Wiki fromCollaborationWiki(CollaborationWiki wiki) {
    return new Wiki(wiki.getText(), wiki.getVersion());
  }
}
