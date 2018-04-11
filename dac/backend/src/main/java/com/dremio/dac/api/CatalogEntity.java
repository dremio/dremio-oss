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
package com.dremio.dac.api;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;

/**
 * Represents a entity in the Dremio catalog
 */
@JsonIgnoreProperties(ignoreUnknown = true)
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.EXTERNAL_PROPERTY, property = "entityType", visible = true)
@JsonSubTypes({
  @JsonSubTypes.Type(value = Dataset.class, name = "dataset"),
  @JsonSubTypes.Type(value = Space.class, name = "space"),
  @JsonSubTypes.Type(value = Source.class, name = "source"),
  @JsonSubTypes.Type(value = Folder.class, name = "folder"),
  @JsonSubTypes.Type(value = File.class, name = "file"),
  @JsonSubTypes.Type(value = Home.class, name = "home")
})
public interface CatalogEntity {
}
