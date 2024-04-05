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
package com.dremio.exec.store.iceberg;

import java.net.URI;
import java.util.Map;
import java.util.Objects;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.StringUtils;

public class HadoopFsCacheKey {
  final String scheme;
  final String authority;
  final Configuration conf;
  final URI uri;
  final String userName;

  public HadoopFsCacheKey(URI uri, Iterable<Map.Entry<String, String>> conf, String userName) {
    this.conf = (Configuration) conf;
    this.uri = uri;
    scheme = uri.getScheme() == null ? "" : StringUtils.toLowerCase(uri.getScheme());
    authority = uri.getAuthority() == null ? "" : StringUtils.toLowerCase(uri.getAuthority());
    this.userName = userName;
  }

  public URI getUri() {
    return uri;
  }

  public Configuration getConf() {
    return conf;
  }

  public String getUserName() {
    return userName;
  }

  @Override
  public int hashCode() {
    return Objects.hash(scheme, authority, userName);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    HadoopFsCacheKey key = (HadoopFsCacheKey) o;
    return com.google.common.base.Objects.equal(scheme, key.scheme)
        && com.google.common.base.Objects.equal(authority, key.authority)
        && com.google.common.base.Objects.equal(userName, key.userName);
  }

  @Override
  public String toString() {
    return userName + "@" + scheme + "://" + authority;
  }
}
