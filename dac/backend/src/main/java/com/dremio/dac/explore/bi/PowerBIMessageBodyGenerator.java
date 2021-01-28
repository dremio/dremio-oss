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

import static com.dremio.dac.server.WebServer.MediaType.APPLICATION_PBIDS;

import java.io.IOException;
import java.io.OutputStream;
import java.lang.annotation.Annotation;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.inject.Inject;
import javax.ws.rs.Produces;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Configuration;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.MultivaluedMap;

import com.dremio.dac.explore.model.DatasetPath;
import com.dremio.exec.proto.CoordinationProtos;
import com.dremio.options.OptionManager;
import com.dremio.service.namespace.dataset.proto.DatasetConfig;
import com.fasterxml.jackson.annotation.JsonGetter;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.google.common.annotations.VisibleForTesting;

/**
 * A Dataset serializer to generate PowerBI PBIDS files
 */
@Produces(APPLICATION_PBIDS)
public class PowerBIMessageBodyGenerator extends BaseBIToolMessageBodyGenerator {
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  /**
   * POJO for an instance of the PBIDS file.
   */
  static class DSRFile {
    private final List<Connection> connections = new ArrayList<>();
    void addConnection(Connection conn) {
      connections.add(conn);
    }

    public Connection[] getConnections() {
      return connections.toArray(new Connection[] {});
    }

    public String getVersion() {
      return "0.1";
    }
  }

  /**
   * POJO for an instance of the Connections array in a PBIDS file.
   */
  @VisibleForTesting
  static class Connection {
    private DataSourceReference dsr;
    private final Map<String, Object> options = new HashMap();

    @JsonGetter(value = "details")
    public DataSourceReference getDSR() {
      return dsr;
    }

    public Object getOptions() {
      return options;
    }

    public String getMode() {
      return "DirectQuery";
    }
  }

  /**
   * POJO for the details section in a PBIDS file.
   */
  @VisibleForTesting
  static class DataSourceReference {
    private DSRConnectionInfo connectionInfo;

    DataSourceReference(DSRConnectionInfo info) {
      this.connectionInfo = info;
    }

    public String getProtocol() {
      return "dremio";
    }

    public DSRConnectionInfo getAddress() {
      return connectionInfo;
    }
  }

  /**
   * POJO for the address section of the PBIDS file.
   */
  @VisibleForTesting
  static class DSRConnectionInfo {
    private String server;
    private String schema;
    private String table;

    public String getServer() {
      return server;
    }

    public String getSchema() {
      return schema;
    }

    @JsonGetter(value = "object")
    public String getTable() {
      return table;
    }
  }

  @Inject
  public PowerBIMessageBodyGenerator(@Context Configuration configuration,
                                     CoordinationProtos.NodeEndpoint endpoint,
                                     OptionManager optionManager) {
    super(endpoint, optionManager);
  }

  @Override
  public void writeTo(DatasetConfig datasetConfig, Class<?> aClass, Type type, Annotation[] annotations, MediaType mediaType, MultivaluedMap<String, Object> httpHeaders, OutputStream outputStream) throws IOException, WebApplicationException {
    final DSRFile dsrFile = createDSRFile(getHostname(httpHeaders), datasetConfig);

    final ObjectWriter writer = OBJECT_MAPPER.writer().withDefaultPrettyPrinter();
    writer.writeValue(outputStream, dsrFile);

    addTargetOutputFileHeader(datasetConfig, "pbids", httpHeaders);
  }

  /**
   * Populate a DSRFile POJO according to the given host and dataset.
   * @param hostname The host to use for the DSR file.
   * @param datasetConfig The dataset to write to the DSR file.
   * @return
   *  a DSRFile POJO containing the host and dataset connection information that can be
   *  serialized with Jackson
   */
  @VisibleForTesting
  static DSRFile createDSRFile(String hostname, DatasetConfig datasetConfig) {
    final DSRConnectionInfo connInfo = new DSRConnectionInfo();
    connInfo.server = hostname;
    final DatasetPath dataset = new DatasetPath(datasetConfig.getFullPathList());
    // PBI doesn't work right when there are quotes embedded in the schema name, which happens
    // when calling DatasetPath.toParentPath(). Instead manually join each element of the path
    // up until the table.
    connInfo.schema = String.join(".", datasetConfig.getFullPathList().subList(0, datasetConfig.getFullPathList().size() - 1));

    connInfo.table = dataset.getLeaf().getName();

    final DataSourceReference dsr = new DataSourceReference(connInfo);
    final Connection conn = new Connection();
    conn.dsr = dsr;

    final DSRFile dsrFile = new DSRFile();
    dsrFile.addConnection(conn);

    return dsrFile;
  }
}
