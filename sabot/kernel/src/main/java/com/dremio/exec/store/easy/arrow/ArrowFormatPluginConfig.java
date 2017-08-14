/*
 * Copyright (C) 2017 Dremio Corporation
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
package com.dremio.exec.store.easy.arrow;

import java.util.List;

import com.dremio.common.logical.FormatPluginConfig;
import com.dremio.exec.store.RecordWriter;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.google.common.base.Objects;
import com.google.common.collect.ImmutableList;

/**
 * {@link FormatPluginConfig} for Arrow format files.
 */
@JsonTypeName("arrow") @JsonInclude(JsonInclude.Include.NON_DEFAULT)
public class ArrowFormatPluginConfig implements FormatPluginConfig {

  public static final List<String> DEFAULT_EXTENSIONS = ImmutableList.of("dremarrow1");

  /**
   * Extensions of files written using the {@link RecordWriter} implementation of this plugin.
   * This needs to be public in order for the query with options to work.
   */
  public String outputExtension = "dremarrow1";

  /**
   * @return List of default extensions of Arrow format files.
   */
  public List<String> getDefaultExtensions() {
    return DEFAULT_EXTENSIONS;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }

    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    final ArrowFormatPluginConfig that = (ArrowFormatPluginConfig) o;

    return Objects.equal(outputExtension, that.outputExtension);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(outputExtension);
  }
}