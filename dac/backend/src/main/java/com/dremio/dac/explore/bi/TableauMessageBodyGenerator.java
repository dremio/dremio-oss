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

import static com.dremio.dac.server.WebServer.MediaType.APPLICATION_TDS;
import static com.dremio.dac.server.WebServer.MediaType.APPLICATION_TDS_DRILL;
import static java.lang.String.format;

import java.io.IOException;
import java.io.OutputStream;
import java.lang.annotation.Annotation;
import java.lang.reflect.Type;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import javax.inject.Inject;
import javax.ws.rs.Produces;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Configuration;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.core.Response.Status;
import javax.ws.rs.ext.MessageBodyWriter;
import javax.xml.stream.XMLOutputFactory;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamWriter;

import com.dremio.common.exceptions.UserException;
import com.dremio.dac.explore.model.DatasetPath;
import com.dremio.dac.model.common.Field;
import com.dremio.dac.server.UserExceptionMapper;
import com.dremio.dac.server.WebServer;
import com.dremio.dac.util.DatasetsUtil;
import com.dremio.exec.proto.CoordinationProtos.NodeEndpoint;
import com.dremio.exec.store.ischema.InfoSchemaConstants;
import com.dremio.options.OptionManager;
import com.dremio.options.Options;
import com.dremio.options.TypeValidators.EnumValidator;
import com.dremio.options.TypeValidators.StringValidator;
import com.dremio.service.namespace.dataset.proto.DatasetConfig;
import com.google.common.base.CharMatcher;
import com.google.common.base.Joiner;
import com.google.common.base.MoreObjects;
import com.google.common.collect.ImmutableMap;

/**
 * A Dataset serializer to generate Tableau TDS files
 */
@Produces({APPLICATION_TDS, APPLICATION_TDS_DRILL})
@Options
public class TableauMessageBodyGenerator implements MessageBodyWriter<DatasetConfig> {
  public static final String CUSTOMIZATION_ENABLED = "dremio.tableau.customization.enabled";

  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(TableauMessageBodyGenerator.class);

  private static final String EMPTY_VERSION = "";

  private static final Map<String, String> CUSTOMIZATIONS = ImmutableMap.<String, String>builder()
      .put("CAP_CREATE_TEMP_TABLES", "no")
      .put("CAP_ODBC_BIND_FORCE_DATETIME_AS_CHAR", "no")
      .put("CAP_ODBC_BIND_FORCE_DATE_AS_CHAR", "no")
      .put("CAP_ODBC_BIND_FORCE_SMALL_STRING_BUFFERS", "no")
      .put("CAP_ODBC_BIND_SUPPRESS_INT64", "no")
      .put("CAP_ODBC_BIND_SUPPRESS_PREFERRED_TYPES", "no")
      .put("CAP_ODBC_BIND_SUPPRESS_WIDE_CHAR", "no")
      .put("CAP_ODBC_CURSOR_DYNAMIC", "no")
      .put("CAP_ODBC_CURSOR_FORWARD_ONLY", "yes")
      .put("CAP_ODBC_CURSOR_KEYSET_DRIVEN", "no")
      .put("CAP_ODBC_CURSOR_STATIC", "no")
      .put("CAP_ODBC_ERROR_IGNORE_FALSE_ALARM", "no")
      .put("CAP_ODBC_FETCH_CONTINUE_ON_ERROR", "no")
      .put("CAP_ODBC_FETCH_MASSIVE_BUFFERS", "no")
      .put("CAP_ODBC_FETCH_RESIZE_BUFFERS", "no")
      .put("CAP_ODBC_METADATA_STRING_LENGTH_UNKNOWN", "no")
      .put("CAP_ODBC_METADATA_SUPPRESS_EXECUTED_QUERY", "yes")
      .put("CAP_ODBC_METADATA_SUPPRESS_PREPARED_QUERY", "no")
      .put("CAP_ODBC_METADATA_SUPPRESS_SQLCOLUMNS_API", "no")
      .put("CAP_ODBC_REBIND_SKIP_UNBIND", "no")
      .put("CAP_ODBC_TRIM_VARCHAR_PADDING", "no")
      .put("CAP_ODBC_UNBIND_AUTO", "no")
      .put("CAP_ODBC_UNBIND_BATCH", "no")
      .put("CAP_ODBC_UNBIND_EACH", "no")
      .put("CAP_ODBC_USE_NATIVE_PROTOCOL", "yes")
      .put("CAP_QUERY_BOOLEXPR_TO_INTEXPR", "no")
      .put("CAP_QUERY_FROM_REQUIRES_ALIAS", "no")
      .put("CAP_QUERY_GROUP_ALLOW_DUPLICATES", "yes")
      .put("CAP_QUERY_GROUP_BY_ALIAS", "no")
      .put("CAP_QUERY_GROUP_BY_DEGREE", "no")
      .put("CAP_QUERY_HAVING_REQUIRES_GROUP_BY", "no")
      .put("CAP_QUERY_HAVING_UNSUPPORTED", "no")
      .put("CAP_QUERY_JOIN_ACROSS_SCHEMAS", "yes")
      .put("CAP_QUERY_JOIN_REQUIRES_SCOPE", "no")
      .put("CAP_QUERY_NULL_REQUIRES_CAST", "no")
      .put("CAP_QUERY_SELECT_ALIASES_SORTED", "no")
      .put("CAP_QUERY_SORT_BY_DEGREE", "no")
      .put("CAP_QUERY_SUBQUERIES", "yes")
      .put("CAP_QUERY_SUBQUERIES_WITH_TOP", "no")
      .put("CAP_QUERY_SUBQUERY_QUERY_CONTEXT", "yes")
      .put("CAP_QUERY_TOPSTYLE_LIMIT", "yes")
      .put("CAP_QUERY_TOPSTYLE_ROWNUM", "no")
      .put("CAP_QUERY_TOPSTYLE_TOP", "no")
      .put("CAP_QUERY_TOP_0_METADATA", "no")
      .put("CAP_QUERY_TOP_N", "no")
      .put("CAP_QUERY_WHERE_FALSE_METADATA", "yes")
      .put("CAP_SELECT_INTO", "no")
      .put("CAP_SELECT_TOP_INTO", "no")
      .put("CAP_SUPPRESS_CONNECTION_POOLING", "no")
      .put("CAP_SUPPRESS_DISCOVERY_QUERIES", "yes")
      .put("CAP_SUPPRESS_DISPLAY_LIMITATIONS", "yes")
      .put("SQL_TIMEDATE_ADD_INTERVALS", "no")
      .put("SQL_TIMEDATE_DIFF_INTERVALS", "no")
      .put("SQL_NUMERIC_FUNCTIONS", "1576193")
      .put("SQL_STRING_FUNCTIONS", "7291")
      .put("SQL_TIMEDATE_FUNCTIONS", "25155")
      .build();

  /**
   * Enum for the different types of Tableau export available within Dremio.
   */
  public enum TableauExportType {
    ODBC,
    NATIVE
  }

  /**
   * Option to add extra connection properties to ODBC string. Should follow ODBC connection
   * string format.
   */
  public static final StringValidator EXTRA_CONNECTION_PROPERTIES = new StringValidator(
    "export.tableau.extra-odbc-connection-properties", "");

  /**
   * Option to add extra connection properties to NATIVE connection. Should follow JDBC connection
   * string format.
   */
  public static final StringValidator EXTRA_NATIVE_CONNECTION_PROPERTIES = new StringValidator(
    "export.tableau.extra-native-connection-properties", "");

  /**
   * Option to switch between ODBC/TDC and JDBC/SDK connectors for Tableau.
   */
  public static final EnumValidator<TableauExportType> TABLEAU_EXPORT_TYPE =
    new EnumValidator<>("export.tableau.export-type", TableauExportType.class, TableauExportType.ODBC);

  private final XMLOutputFactory xmlOutputFactory = XMLOutputFactory.newInstance();
  private final NodeEndpoint endpoint;
  private final String masterNode;
  private final boolean customizationEnabled;
  private final TableauExportType exportType;
  private final OptionManager optionManager;

  @Inject
  public TableauMessageBodyGenerator(@Context Configuration configuration, NodeEndpoint endpoint, OptionManager optionManager) {
    this.endpoint = endpoint;
    this.masterNode = MoreObjects.firstNonNull(endpoint.getAddress(), "localhost");
    this.customizationEnabled = MoreObjects.firstNonNull((Boolean) configuration.getProperty(CUSTOMIZATION_ENABLED), false);
    this.optionManager = optionManager;

    // The EnumValidator lower-cases the enum for some reason, so we upper case it to match our enum values again.
    this.exportType = TableauExportType.valueOf(optionManager.getOption(TABLEAU_EXPORT_TYPE).toUpperCase(Locale.ROOT));
  }

  @Override
  public long getSize(DatasetConfig t, Class<?> type, Type genericType, Annotation[] annotations, MediaType mediaType) {
    return -1;
  }

  @Override
  public boolean isWriteable(Class<?> type, Type genericType, Annotation[] annotations, MediaType mediaType) {
    return type == DatasetConfig.class;
  }

  @Override
  public void writeTo(DatasetConfig datasetConfig, Class<?> type, Type genericType, Annotation[] annotations, MediaType mediaType,
      MultivaluedMap<String, Object> httpHeaders, OutputStream entityStream)
      throws IOException, WebApplicationException {
    final String hostname;
    if (httpHeaders.containsKey(WebServer.X_DREMIO_HOSTNAME)) {
      hostname = (String) httpHeaders.getFirst(WebServer.X_DREMIO_HOSTNAME);
    } else {
      hostname = masterNode;
    }

    // Change headers to force download and suggest a filename.
    String fullPath = Joiner.on(".").join(datasetConfig.getFullPathList());
    httpHeaders.putSingle(HttpHeaders.CONTENT_DISPOSITION, format("attachment; filename=\"%s.tds\"", fullPath));

    try {
      final XMLStreamWriter xmlStreamWriter = xmlOutputFactory.createXMLStreamWriter(entityStream, "UTF-8");

      xmlStreamWriter.writeStartDocument("utf-8", "1.0");
      writeDatasource(xmlStreamWriter, datasetConfig, hostname, mediaType);
      xmlStreamWriter.writeEndDocument();

      xmlStreamWriter.close();
    } catch (XMLStreamException e) {
      throw UserExceptionMapper.withStatus(
        UserException.dataWriteError(e)
          .message("Cannot generate TDS file")
          .addContext("Details", e.getMessage()),
        Status.INTERNAL_SERVER_ERROR
      ).build(logger);
    }
  }

  private void writeDatasource(final XMLStreamWriter xmlStreamWriter, DatasetConfig datasetConfig, String hostname, MediaType mediaType) throws XMLStreamException {
    xmlStreamWriter.writeStartElement("datasource");
    xmlStreamWriter.writeAttribute("inline", "true");
    xmlStreamWriter.writeAttribute("version", EMPTY_VERSION);

    if (WebServer.MediaType.APPLICATION_TDS_TYPE.equals(mediaType)) {
      if (TableauExportType.NATIVE == exportType) {
        writeSdkConnection(xmlStreamWriter, datasetConfig, hostname);
      } else {
        writeOdbcConnection(xmlStreamWriter, datasetConfig, hostname);
      }
    } else if (WebServer.MediaType.APPLICATION_TDS_DRILL_TYPE.equals(mediaType)) {
      writeNativeDrillConnection(xmlStreamWriter, datasetConfig, hostname);
    } else {
      throw new RuntimeException("Unsupported media type " + mediaType);
    }

    xmlStreamWriter.writeEndElement();
  }

  private void writeSdkConnection(XMLStreamWriter xmlStreamWriter, DatasetConfig datasetConfig, String hostname) throws XMLStreamException {
    final DatasetPath dataset = new DatasetPath(datasetConfig.getFullPathList());

    xmlStreamWriter.writeStartElement("connection");

    xmlStreamWriter.writeAttribute("class", "dremio");
    xmlStreamWriter.writeAttribute("dbname", InfoSchemaConstants.IS_CATALOG_NAME);

    // It has to match what is returned by the driver/Tableau
    xmlStreamWriter.writeAttribute("schema", dataset.toParentPath());
    xmlStreamWriter.writeAttribute("port", String.valueOf(endpoint.getUserPort()));
    xmlStreamWriter.writeAttribute("server", hostname);
    xmlStreamWriter.writeAttribute("username", "");

    String customExtraProperties = optionManager.getOption(EXTRA_NATIVE_CONNECTION_PROPERTIES);
    customExtraProperties = customExtraProperties.replace(" ", "").toLowerCase(Locale.ROOT);

    // The "parsing" here is rudimentary, and is not meant to be advanced. We simply need a flag to determine if SSL
    // is enabled to allow Tableau to do the right thing. As more options are added, this may need to be more complex.
    xmlStreamWriter.writeAttribute("sslmode", customExtraProperties.contains("ssl=true") ? "required" : "");

    writeRelation(xmlStreamWriter, datasetConfig);
    xmlStreamWriter.writeEndElement();
  }

  private void writeOdbcConnection(XMLStreamWriter xmlStreamWriter, DatasetConfig datasetConfig, String hostname) throws XMLStreamException {
    DatasetPath dataset = new DatasetPath(datasetConfig.getFullPathList());

    xmlStreamWriter.writeStartElement("connection");

    xmlStreamWriter.writeAttribute("class", "genericodbc");
    xmlStreamWriter.writeAttribute("dbname", InfoSchemaConstants.IS_CATALOG_NAME);

    // Create advanced properties string
    final StringBuilder extraProperties = new StringBuilder();
    final String customExtraProperties = optionManager.getOption(EXTRA_CONNECTION_PROPERTIES);
    // Writing custom extra properties first as they will take precedence over default ones
    if (!customExtraProperties.isEmpty()) {
      extraProperties.append(customExtraProperties).append(";");
    }
    extraProperties.append("AUTHENTICATIONTYPE=Basic Authentication;CONNECTIONTYPE=Direct;HOST=");
    extraProperties.append(hostname);
    xmlStreamWriter.writeAttribute("odbc-connect-string-extras", extraProperties.toString());

    // It has to match what is returned by the driver/Tableau
    xmlStreamWriter.writeAttribute("odbc-dbms-name", "Dremio");
    xmlStreamWriter.writeAttribute("odbc-driver", "Dremio Connector");
    xmlStreamWriter.writeAttribute("odbc-dsn", "");
    xmlStreamWriter.writeAttribute("odbc-suppress-connection-pooling", "");
    xmlStreamWriter.writeAttribute("odbc-use-connection-pooling", "");
    xmlStreamWriter.writeAttribute("schema", dataset.toParentPath());
    xmlStreamWriter.writeAttribute("port", String.valueOf(endpoint.getUserPort()));
    xmlStreamWriter.writeAttribute("server", "");
    xmlStreamWriter.writeAttribute("username", "");

    writeRelation(xmlStreamWriter, datasetConfig);
    if (customizationEnabled) {
      writeConnectionCustomization(xmlStreamWriter);
    }
    xmlStreamWriter.writeEndElement();
  }

  private void writeNativeDrillConnection(XMLStreamWriter xmlStreamWriter, DatasetConfig datasetConfig, String hostname) throws XMLStreamException {
    DatasetPath dataset = new DatasetPath(datasetConfig.getFullPathList());

    xmlStreamWriter.writeStartElement("connection");

    xmlStreamWriter.writeAttribute("class", "drill");
    xmlStreamWriter.writeAttribute("connection-type", "Direct");
    xmlStreamWriter.writeAttribute("authentication", "Basic Authentication");

    xmlStreamWriter.writeAttribute("odbc-connect-string-extras", "");
    // It has to match what is returned by the driver/Tableau
    xmlStreamWriter.writeAttribute("schema", dataset.toParentPath());
    xmlStreamWriter.writeAttribute("port", String.valueOf(endpoint.getUserPort()));
    xmlStreamWriter.writeAttribute("server", hostname);

    writeRelation(xmlStreamWriter, datasetConfig);
    if (customizationEnabled) {
      writeConnectionCustomization(xmlStreamWriter);
    }

    /* DX-7447 - When using the drill driver, Tableau will show an misleading error message because it could not connect
      to Dremio since it doesn't have the username/password (and doesn't prompt for them first).  To work around this,
      we generate metadata for the Dataset and add it to the .tds file.  This prefills the schema in Tableau and when
      the user starts working with the schema it will prompt the user to authenticate with Dremio.
    */
    writeTableauMetadata(xmlStreamWriter, datasetConfig);

    xmlStreamWriter.writeEndElement();
  }

  /**
   * Encode character sequence per Tableau convention
   *
   * @param sequence the sequence to encode
   * @return the encoded string
   */
  private static final String tableauEncode(CharSequence sequence) {
    return CharMatcher.is(']').replaceFrom(sequence, "]]");
  }

  private void writeRelation(XMLStreamWriter xmlStreamWriter, DatasetConfig datasetConfig) throws XMLStreamException {
    DatasetPath dataset = new DatasetPath(datasetConfig.getFullPathList());

    final String name = dataset.getLeaf().getName();
    // If dataset schema contains a dot '.' in one of its component
    // Tableau won't be able to open it
    final String schema = Joiner.on('.').join(dataset.toParentPathList());

    xmlStreamWriter.writeStartElement("relation");
    xmlStreamWriter.writeAttribute("name", name);
    xmlStreamWriter.writeAttribute("type", "table");
    xmlStreamWriter.writeAttribute("table", format("[%s].[%s]", tableauEncode(schema), tableauEncode(name)));
    xmlStreamWriter.writeEndElement();
  }

  private void writeConnectionCustomization(XMLStreamWriter xmlStreamWriter) throws XMLStreamException {
    xmlStreamWriter.writeStartElement("connection-customization");
    xmlStreamWriter.writeAttribute("class", "genericodbc");
    xmlStreamWriter.writeAttribute("enabled", "true");

    // vendor
    xmlStreamWriter.writeStartElement("vendor");
    // It has to match the ODBC vendor attribute!!!
    xmlStreamWriter.writeAttribute("name", "Dremio");
    xmlStreamWriter.writeEndElement();

    // driver
    xmlStreamWriter.writeStartElement("driver");
    // it has to match the ODBC driver name
    xmlStreamWriter.writeAttribute("name", "Dremio Connector");
    xmlStreamWriter.writeEndElement();

    // customizations
    xmlStreamWriter.writeStartElement("customizations");
    for (Map.Entry<String, String> customization : CUSTOMIZATIONS.entrySet()) {
      xmlStreamWriter.writeStartElement("customization");
      xmlStreamWriter.writeAttribute("name", customization.getKey());
      xmlStreamWriter.writeAttribute("value", customization.getValue());
      xmlStreamWriter.writeEndElement();
    }
    xmlStreamWriter.writeEndElement();

    xmlStreamWriter.writeEndElement();
  }

  private void writeTableauMetadata(XMLStreamWriter xmlStreamWriter, DatasetConfig datasetConfig) throws XMLStreamException {
    List<Field> fields = DatasetsUtil.getFieldsFromDatasetConfig(datasetConfig);

    if (fields == null) {
      return;
    }

    xmlStreamWriter.writeStartElement("metadata-records");

    for (Field field : fields) {
      String tableauType = null;

      switch (field.getType()) {
        case INTEGER:
          tableauType = "integer";
          break;
        case DECIMAL:
        case FLOAT:
          tableauType = "real";
          break;
        case BOOLEAN:
          tableauType = "boolean";
          break;
        case DATE:
          tableauType = "date";
          break;
        case TIME:
        case DATETIME:
          // tableau doesn't have a time type, it just uses datetime in that case
          tableauType = "datetime";
          break;
        default:
          // all other types we default to string
          tableauType = "string";
      }

      xmlStreamWriter.writeStartElement("metadata-record");
      xmlStreamWriter.writeAttribute("class", "column");

      // field name needs to be surrounded by []
      writeElement(xmlStreamWriter, "local-name", "[" + field.getName() + "]");
      writeElement(xmlStreamWriter, "local-type", tableauType);

      // close metadata-record
      xmlStreamWriter.writeEndElement();
    }

    xmlStreamWriter.writeEndElement();
  }

  private void writeElement(XMLStreamWriter xmlStreamWriter, String name, String contents) throws XMLStreamException {
    xmlStreamWriter.writeStartElement(name);

    if (contents != null) {
      xmlStreamWriter.writeCharacters(contents);
    }

    xmlStreamWriter.writeEndElement();
  }
}
