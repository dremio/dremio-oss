/*
 * Copyright (C) 2017-2018 Dremio Corporation
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

import java.util.Collections;
import java.util.List;

import com.dremio.dac.proto.model.collaboration.CollaborationTag;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Collaboration tags
 */
public class Tags {
  private final List<String> tags;
  private final String version;

  @JsonCreator
  public Tags(
    @JsonProperty("tags") List<String> tags,
    @JsonProperty("version") String version) {
    this.tags = (tags == null) ? Collections.emptyList() : tags;
    this.version = version;
  }

  public List<String> getTags() {
    return tags;
  }

  public String getVersion() {
    return version;
  }

  public static Tags fromCollaborationTag(CollaborationTag collaborationTag) {
    return new Tags(collaborationTag.getTagsList(), collaborationTag.getTag());
  }
}
