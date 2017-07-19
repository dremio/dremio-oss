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
package com.dremio.dac.sources;

import static com.dremio.dac.sources.Hosts.appendHosts;
import static com.dremio.service.namespace.source.proto.SourceType.MONGO;
import static com.google.common.base.Preconditions.checkNotNull;
import static java.nio.charset.StandardCharsets.UTF_8;

import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.util.Collections;
import java.util.Iterator;

import com.dremio.common.store.StoragePluginConfig;
import com.dremio.dac.proto.model.source.AuthenticationType;
import com.dremio.dac.proto.model.source.MongoConfig;
import com.dremio.dac.proto.model.source.Property;
import com.dremio.dac.server.SingleSourceToStoragePluginConfig;
import com.dremio.plugins.mongo.MongoStoragePluginConfig;
import com.google.common.collect.Iterables;

/**
 * generates a StoragePluginConfig from a Mongo Source
 *
 */
public class MongoSourceConfigurator extends SingleSourceToStoragePluginConfig<MongoConfig> {

  public MongoSourceConfigurator() {
    super(MONGO);
  }

  static String urlEncode(String fragment) {
    try {
      return URLEncoder.encode(fragment, UTF_8.name());
    } catch (UnsupportedEncodingException e) {
      throw new AssertionError("Expecting UTF_8 to be a supported charset", e);
    }
  }

  static StringBuilder appendProperties(StringBuilder sb, Iterable<Property> propertyList, char innerDelimiter, char outerDelimiter) {
    Iterator<Property> iterator = propertyList.iterator();

    while(iterator.hasNext()) {
      Property p = iterator.next();
      sb.append(urlEncode(p.getName())).append(innerDelimiter).append(p.getValue());

      if (iterator.hasNext()) {
        sb.append(outerDelimiter);
      }
    }

    return sb;
  }

  @Override
  public StoragePluginConfig configureSingle(MongoConfig mongo) {
    // Authority
    StringBuilder connection = new StringBuilder("mongodb://");
    if (AuthenticationType.MASTER.equals(mongo.getAuthenticationType())) {
      String username = mongo.getUsername();
      if (username != null) {
        connection.append(urlEncode(username));
      }
      String password = mongo.getPassword();
      if (password != null) {
        connection.append(":").append(urlEncode(password));
      }
      connection.append("@");

      // host list
      appendHosts(connection, checkNotNull(mongo.getHostList(), "hostList missing"), ',').append("/");
      final String database = mongo.getAuthDatabase();
      if (database != null) {
        connection.append(database);
      }
    } else {
      // host list
      appendHosts(connection, checkNotNull(mongo.getHostList(), "hostList missing"), ',').append("/");
    }

    // Query string
    Iterable<Property> propertyList = checkNotNull(mongo.getUseSsl(), "useSSL missing")
        ? Iterables.concat(mongo.getPropertyList(), Collections.singleton(new Property().setName("ssl").setValue("true")))
        : mongo.getPropertyList();

    connection.append("?");
    appendProperties(connection, checkNotNull(propertyList, "propertyList missing"), '=', '&');

    // default 2s
    int authTimeoutMillis = mongo.getAuthenticationTimeoutMillis() == null ? 2000 : mongo.getAuthenticationTimeoutMillis().intValue();
    boolean secondaryReadsOnly = mongo.getSecondaryReadsOnly() == null ? false : mongo.getSecondaryReadsOnly().booleanValue();
    // 0 means disabled
    int subpartitionSize = mongo.getSubpartitionSize() == null ? 0 : mongo.getSubpartitionSize().intValue();
    MongoStoragePluginConfig config = new MongoStoragePluginConfig(
        authTimeoutMillis,
        connection.toString(),
        secondaryReadsOnly,
        subpartitionSize
        );
    return config;
  }

}
