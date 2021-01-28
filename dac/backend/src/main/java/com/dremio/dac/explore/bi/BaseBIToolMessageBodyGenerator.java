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
package com.dremio.dac.explore.bi;

import static com.dremio.dac.explore.bi.BIToolsConstants.EXPORT_HOSTNAME;
import static java.lang.String.format;

import java.lang.annotation.Annotation;
import java.lang.reflect.Type;

import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.ext.MessageBodyWriter;

import com.dremio.dac.server.WebServer;
import com.dremio.exec.proto.CoordinationProtos;
import com.dremio.options.OptionManager;
import com.dremio.service.namespace.dataset.proto.DatasetConfig;
import com.google.common.base.Joiner;
import com.google.common.base.MoreObjects;
import com.google.common.base.Strings;

/**
 * A base class for generating responses to load datasets in BI tools.
 */
abstract class BaseBIToolMessageBodyGenerator implements MessageBodyWriter<DatasetConfig> {
  private final CoordinationProtos.NodeEndpoint endpoint;
  private final String masterNode;
  private final OptionManager optionManager;

  protected BaseBIToolMessageBodyGenerator(CoordinationProtos.NodeEndpoint endpoint, OptionManager optionManager) {
    this.endpoint = endpoint;
    this.masterNode = MoreObjects.firstNonNull(endpoint.getAddress(), "localhost");
    this.optionManager = optionManager;
  }

  @Override
  public boolean isWriteable(Class<?> aClass, Type type, Annotation[] annotations, MediaType mediaType) {
    return type == DatasetConfig.class;
  }

  @Override
  public long getSize(DatasetConfig datasetConfig, Class<?> aClass, Type type, Annotation[] annotations, MediaType mediaType) {
    return -1;
  }

  protected CoordinationProtos.NodeEndpoint getEndpoint() {
    return endpoint;
  }

  /**
   * Return an appropriate hostname based on the given headers.
   * @param httpHeaders The HTTP headers to use to help generate a host name.
   * @return The hostname.
   */
  protected String getHostname(MultivaluedMap<String, Object> httpHeaders) {
    final String hostnameOverride = optionManager.getOption(EXPORT_HOSTNAME);
    if (!Strings.isNullOrEmpty(hostnameOverride)) {
      return hostnameOverride;
    }

    if (httpHeaders.containsKey(WebServer.X_DREMIO_HOSTNAME)) {
      return (String) httpHeaders.getFirst(WebServer.X_DREMIO_HOSTNAME);
    }
    return masterNode;
  }

  /**
   * Getter method to retrieve OptionManager.
   * @return the OptionManager of this instance.
   */
  protected OptionManager getOptionManager() {
    return this.optionManager;
  }

  /**
   * Add an HTTP header to specify the name of the output file for the response.
   * @param datasetConfig The dataset.
   * @param extension The file extension, not including the period.
   * @param httpHeaders The output headers.
   */
  protected static void addTargetOutputFileHeader(DatasetConfig datasetConfig,
                                                  String extension,
                                                  MultivaluedMap<String, Object> httpHeaders) {
    // Change headers to force download and suggest a filename.
    final String fullPath = Joiner.on(".").join(datasetConfig.getFullPathList());
    httpHeaders.putSingle(HttpHeaders.CONTENT_DISPOSITION,
      format("attachment; filename=\"%s.%s\"", fullPath, extension));
  }
}
