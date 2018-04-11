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
package com.dremio.dac.model.namespace;

import com.dremio.dac.model.sources.SourceUI;
import com.dremio.dac.model.spaces.Home;
import com.dremio.dac.model.spaces.Space;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;

/**
 * Interface for models that can contain datasets.
 *
 * Currently:
 * - Spaces can contain virtual datasets
 * - Home Spaces can contain virtual and uploaded physical datasets
 * - Sources can contain only physical datasets
 */
@JsonTypeInfo(
    use = JsonTypeInfo.Id.NAME,
    include = JsonTypeInfo.As.PROPERTY,
    // not named type to avoid collision with source types which also use this strategy
    // to serialize into different concrete types
    property = "datasetContainerType")
@JsonSubTypes({
    @JsonSubTypes.Type(value = Space.class, name = "space"),
    @JsonSubTypes.Type(value = SourceUI.class, name = "source"),
    @JsonSubTypes.Type(value = Home.class, name = "home"),
})
public interface DatasetContainer {
  String getName();
  Long getCtime();
  String getDescription();
}
