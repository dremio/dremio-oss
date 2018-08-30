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
package com.dremio.options;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;

/**
 * An {@link OptionValue option value} is used by an {@link OptionManager} to store a run-time setting. This setting,
 * for example, could affect a query in execution stage. Instances of this class are JSON serializable and can be stored
 * in a {@link PersistentStore persistent store} (see {@link SystemOptionManager#options}), or
 * in memory (see {@link InMemoryOptionManager#options}).
 */
@JsonInclude(Include.NON_NULL)
public final class OptionValue implements Comparable<OptionValue> {

  /**
   * Option Type
   */
  public enum OptionType {
    BOOT, SYSTEM, SESSION, QUERY
  }

  /**
   * Option kind
   */
  public enum Kind {
    BOOLEAN, LONG, STRING, DOUBLE
  }

  private final String name;
  private final Kind kind;
  private final OptionType type;

  private final Long numVal;
  private final String stringVal;
  private final Boolean boolVal;
  private final Double floatVal;

  public static OptionValue createLong(OptionType type, String name, long val) {
    return new OptionValue(Kind.LONG, type, name, val, null, null, null);
  }

  public static OptionValue createBoolean(OptionType type, String name, boolean bool) {
    return new OptionValue(Kind.BOOLEAN, type, name, null, null, bool, null);
  }

  public static OptionValue createString(OptionType type, String name, String val) {
    return new OptionValue(Kind.STRING, type, name, null, val, null, null);
  }

  public static OptionValue createDouble(OptionType type, String name, double val) {
    return new OptionValue(Kind.DOUBLE, type, name, null, null, null, val);
  }

  public static OptionValue createOption(Kind kind, OptionType type, String name, String val) {
    switch (kind) {
      case BOOLEAN:
        return createBoolean(type, name, Boolean.valueOf(val));
      case LONG:
        return createLong(type, name, Long.valueOf(val));
      case STRING:
        return createString(type, name, val);
      case DOUBLE:
        return createDouble(type, name, Double.valueOf(val));
      default:
        return null;
    }
  }

  @JsonCreator
  public OptionValue(@JsonProperty("kind") Kind kind,
                      @JsonProperty("type") OptionType type,
                      @JsonProperty("name") String name,
                      @JsonProperty("num_val") Long num_val,
                      @JsonProperty("string_val") String string_val,
                      @JsonProperty("bool_val") Boolean boolVal,
                      @JsonProperty("float_val") Double floatVal) {
    Preconditions.checkArgument(num_val != null || string_val != null || boolVal != null || floatVal != null);
    this.kind = kind;
    this.type = type;
    this.name = name;

    this.floatVal = floatVal;
    this.numVal = num_val;
    this.stringVal = string_val;
    this.boolVal = boolVal;
  }

  @JsonIgnore
  public Object getValue() {
    switch (kind) {
      case BOOLEAN:
        return boolVal;
      case LONG:
        return numVal;
      case STRING:
        return stringVal;
      case DOUBLE:
        return floatVal;
      default:
        return null;
    }
  }

  public String getName() {
    return name;
  }

  public Kind getKind() {
    return kind;
  }

  public OptionType getType() {
    return type;
  }

  @JsonProperty("num_val")
  public Long getNumVal() {
    return numVal;
  }

  @JsonProperty("string_val")
  public String getStringVal() {
    return stringVal;
  }

  @JsonProperty("bool_val")
  public Boolean getBoolVal() {
    return boolVal;
  }

  @JsonProperty("float_val")
  public Double getFloatVal() {
    return floatVal;
  }


  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + ((boolVal == null) ? 0 : boolVal.hashCode());
    result = prime * result + ((floatVal == null) ? 0 : floatVal.hashCode());
    result = prime * result + ((kind == null) ? 0 : kind.hashCode());
    result = prime * result + ((name == null) ? 0 : name.hashCode());
    result = prime * result + ((numVal == null) ? 0 : numVal.hashCode());
    result = prime * result + ((stringVal == null) ? 0 : stringVal.hashCode());
    result = prime * result + ((type == null) ? 0 : type.hashCode());
    return result;
  }

  public boolean equalsIgnoreType(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null) {
      return false;
    }
    if (getClass() != obj.getClass()) {
      return false;
    }
    final OptionValue other = (OptionValue) obj;
    if (boolVal == null) {
      if (other.boolVal != null) {
        return false;
      }
    } else if (!boolVal.equals(other.boolVal)) {
      return false;
    }
    if (floatVal == null) {
      if (other.floatVal != null) {
        return false;
      }
    } else if (!floatVal.equals(other.floatVal)) {
      return false;
    }
    if (kind != other.kind) {
      return false;
    }
    if (name == null) {
      if (other.name != null) {
        return false;
      }
    } else if (!name.equals(other.name)) {
      return false;
    }
    if (numVal == null) {
      if (other.numVal != null) {
        return false;
      }
    } else if (!numVal.equals(other.numVal)) {
      return false;
    }
    if (stringVal == null) {
      if (other.stringVal != null) {
        return false;
      }
    } else if (!stringVal.equals(other.stringVal)) {
      return false;
    }
    return true;
  }

  @Override
  public boolean equals(Object obj) {
    if (!equalsIgnoreType(obj)) {
      return false;
    }
    final OptionValue other = (OptionValue) obj;
    return type == other.type;
  }

  @Override
  public int compareTo(OptionValue o) {
    return this.name.compareTo(o.name);
  }

  @Override
  public String toString() {
    return "OptionValue [type=" + type + ", name=" + name + ", value=" + getValue() + "]";
  }
}
