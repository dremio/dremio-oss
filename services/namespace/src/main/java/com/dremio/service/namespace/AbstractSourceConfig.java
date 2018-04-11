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
package com.dremio.service.namespace;

import com.fasterxml.jackson.annotation.JsonIgnore;

import io.protostuff.ByteString;

/**
 * A base implementation of SourceConfig that accepts AbstractConnectionConf and sets type and bytes.
 */
public abstract class AbstractSourceConfig<T extends AbstractSourceConfig<T>> {

  @SuppressWarnings("unchecked")
  public T setConnectionConf(AbstractConnectionConf conf) {
    setType(conf.getType());
    setConfig(conf.toBytesString());
    return (T) this;
  }

  /**
   * Better to use setConnectionConf()
   * @param type
   * @return
   */
  @Deprecated
  public abstract T setType(String type);

  /**
   * Better to use setConnectionConf()
   * @param type
   * @return
   */
  @Deprecated
  public abstract T setConfig(ByteString bytes);
  public abstract ByteString getConfig();
  public abstract String getType();
  public abstract String getName();
  public abstract Long getVersion();
  public abstract Object getLegacySourceTypeEnum();

  public <X extends AbstractConnectionConf> X getConnectionConf(AbstractConnectionReader reader){
    String type = getType();
    if(type == null) {
      type = getLegacySourceTypeEnum().toString();
    }

    return reader.getConnectionConf(type, getConfig());
  }

  @JsonIgnore
  public NamespaceKey getKey() {
    return new NamespaceKey(getName());
  }

}
