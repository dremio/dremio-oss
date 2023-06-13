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
package com.dremio.dac.service.admin;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.annotation.JsonTypeName;

/**
 * Generic implementation of setting across multiple setting types.
 */
@JsonPropertyOrder({ "@id" })
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY, property = "type")
@JsonSubTypes({
    @JsonSubTypes.Type(value=Setting.IntegerSetting.class),
    @JsonSubTypes.Type(value=Setting.FloatSetting.class),
    @JsonSubTypes.Type(value=Setting.BooleanSetting.class),
    @JsonSubTypes.Type(value=Setting.TextSetting.class)
})
public abstract class Setting {

  private final String id;

  public Setting(String id) {
    super();
    this.id = id;
  }

  public String getId() {
    return id;
  }

  public abstract Object getValue();

  /**
   * Setting with long value.
   */
  @JsonTypeName("INTEGER")
  public static class IntegerSetting extends Setting {

    private final long value;

    @JsonCreator
    public IntegerSetting(
        @JsonProperty("id") String id,
        @JsonProperty("value") long value
        ) {
      super(id);
      this.value = value;
    }

    @Override
    public Long getValue(){
      return value;
    }
  }

  /**
   * Setting with floating point value.
   */
  @JsonTypeName("FLOAT")
  public static class FloatSetting extends Setting {

    private final double value;

    @JsonCreator
    public FloatSetting(
        @JsonProperty("id") String id,
        @JsonProperty("value") double value
        ) {
      super(id);
      this.value = value;
    }

    @Override
    public Double getValue(){
      return value;
    }
  }

  /**
   * Setting with boolean value.
   */
  @JsonTypeName("BOOLEAN")
  public static class BooleanSetting extends Setting {

    private final boolean value;

    @JsonCreator
    public BooleanSetting(
        @JsonProperty("id") String id,
        @JsonProperty("value") boolean value
        ) {
      super(id);
      this.value = value;
    }

    @Override
    public Boolean getValue(){
      return value;
    }
  }

  /**
   * Setting with text value.
   */
  @JsonTypeName("TEXT")
  public static class TextSetting extends Setting {

    private final String value;

    @JsonCreator
    public TextSetting(
        @JsonProperty("id") String id,
        @JsonProperty("value") String value
        ) {
      super(id);
      this.value = value;
    }

    @Override
    public String getValue(){
      return value;
    }
  }

}
