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
package com.dremio.service.namespace;

import static java.util.Collections.unmodifiableList;

import java.util.Arrays;
import java.util.List;

import com.dremio.common.exceptions.UserException;

/**
 * Object representing a path along with a set of options
 */
public final class TableInstance {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(TableInstance.class);

  private final TableSignature sig;
  private final List<Object> params;

  public TableInstance(TableSignature sig, List<? extends Object> params) {
    super();
    if (params.size() != sig.params.size()) {
      throw UserException.parseError()
          .message(
              "should have as many params (%d) as signature (%d)",
              params.size(), sig.params.size())
          .addContext("table", sig.name)
          .build(logger);
    }
    this.sig = sig;
    this.params = unmodifiableList(params);
  }

  public String presentParams() {
    StringBuilder sb = new StringBuilder("(");
    boolean first = true;
    for (int i = 0; i < params.size(); i++) {
      Object param = params.get(i);
      if (param != null) {
        if (first) {
          first = false;
        } else {
          sb.append(", ");
        }
        TableParamDef paramDef = sig.params.get(i);
        sb.append(paramDef.name).append(": ").append(paramDef.type.getSimpleName()).append(" => ").append(param);
      }
    }
    sb.append(")");
    return sb.toString();
  }

  private Object[] toArray() {
    return array(sig, params);
  }

  public TableSignature getSig() {
    return sig;
  }

  @Override
  public int hashCode() {
    return Arrays.hashCode(toArray());
  }

  public List<Object> getParams(){
    return params;
  }

  @Override
  public boolean equals(Object obj) {
    if (obj instanceof TableInstance) {
      return Arrays.equals(this.toArray(), ((TableInstance)obj).toArray());
    }
    return false;
  }

  @Override
  public String toString() {
    return sig.name + (params.size() == 0 ? "" : presentParams());
  }

  private static Object[] array(Object... objects) {
    return objects;
  }

  /**
   * Signature of table that has multiple table parameters.
   */
  public static final class TableSignature {
    private final String name;
    private final List<TableParamDef> params;

    public TableSignature(String name, TableParamDef... params) {
      this(name, Arrays.asList(params));
    }

    public TableSignature(String name, List<TableParamDef> params) {
      this.name = name;
      this.params = unmodifiableList(params);
    }

    public String getName() {
      return name;
    }

    public List<TableParamDef> getParams() {
      return params;
    }

    private Object[] toArray() {
      return array(name, params);
    }

    @Override
    public int hashCode() {
      return Arrays.hashCode(toArray());
    }

    @Override
    public boolean equals(Object obj) {
      if (obj instanceof TableSignature) {
        return Arrays.equals(this.toArray(), ((TableSignature)obj).toArray());
      }
      return false;
    }

    @Override
    public String toString() {
      return name + params;
    }
  }

  /**
   * Table function parameter
   */
  public static final class TableParamDef {
    private final String name;
    private final Class<?> type;
    private final boolean optional;

    public TableParamDef(String name, Class<?> type) {
      this(name, type, false);
    }

    public TableParamDef(String name, Class<?> type, boolean optional) {
      this.name = name;
      this.type = type;
      this.optional = optional;
    }

    public String getName() {
      return name;
    }

    public boolean isOptional() {
      return optional;
    }

    public Class<?> getType() {
      return type;
    }

    public TableParamDef optional() {
      return new TableParamDef(name, type, true);
    }

    private Object[] toArray() {
      return array(name, type, optional);
    }

    @Override
    public int hashCode() {
      return Arrays.hashCode(toArray());
    }

    @Override
    public boolean equals(Object obj) {
      if (obj instanceof TableParamDef) {
        return Arrays.equals(this.toArray(), ((TableParamDef)obj).toArray());
      }
      return false;
    }

    @Override
    public String toString() {
      String p = name + ": " + type;
      return optional ? "[" + p + "]" : p;
    }
  }
}
