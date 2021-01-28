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
import static java.lang.Character.isLowerCase;
import static java.lang.Character.isUpperCase;
import static java.lang.String.format;

import java.io.IOException;
import java.io.OutputStream;
import java.lang.annotation.Annotation;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.regex.Pattern;

import javax.inject.Inject;
import javax.ws.rs.Produces;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Configuration;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.core.Response.Status;
import javax.xml.stream.XMLOutputFactory;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamWriter;

import org.apache.arrow.vector.types.pojo.ArrowType.ArrowTypeID;
import org.apache.arrow.vector.types.pojo.Field;

import com.dremio.common.exceptions.UserException;
import com.dremio.dac.explore.model.DatasetPath;
import com.dremio.dac.server.UserExceptionMapper;
import com.dremio.dac.server.WebServer;
import com.dremio.exec.proto.CoordinationProtos.NodeEndpoint;
import com.dremio.exec.record.BatchSchema;
import com.dremio.exec.store.ischema.InfoSchemaConstants;
import com.dremio.options.OptionManager;
import com.dremio.options.Options;
import com.dremio.options.TypeValidators.EnumValidator;
import com.dremio.options.TypeValidators.StringValidator;
import com.dremio.service.namespace.dataset.proto.DatasetConfig;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.CharMatcher;
import com.google.common.base.Joiner;
import com.google.common.base.MoreObjects;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableMap;

import io.protostuff.ByteString;

/**
 * A Dataset serializer to generate Tableau TDS files
 */
@Produces({APPLICATION_TDS, APPLICATION_TDS_DRILL})
@Options
public class TableauMessageBodyGenerator extends BaseBIToolMessageBodyGenerator {
  public static final String CUSTOMIZATION_ENABLED = "dremio.tableau.customization.enabled";
  private static final String DREMIO_UPDATE_COLUMN = "$_dremio_$_update_$";
  @VisibleForTesting
  static final String TABLEAU_VERSION = "18.1";

  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(TableauMessageBodyGenerator.class);
  private static final Map<String, String> CUSTOMIZATIONS = ImmutableMap.<String, String>builder()
      // Keep this map in sync with the TDC files included with the macOS and Windows installers.
      // macOS: https://github.com/dremio/odbc/blob/master/osx/DremioODBC-pkgbuild/Library/Dremio/ODBC/Resources/DremioConnector.tdc
      // Windows x86: https://github.com/dremio/odbc/blob/master/windows/x86/deploy/Resources/DremioConnector.tdc
      // Windows x64: https://github.com/dremio/odbc/blob/master/windows/x64/deploy/Resources/DremioConnector.tdc
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
      .put("CAP_ODBC_CONNECTION_STATE_VERIFY_FAST", "yes")
      .put("CAP_ODBC_CONNECTION_STATE_VERIFY_PROBE", "yes")
      .put("CAP_ODBC_CONNECTION_STATE_VERIFY_PROBE_PREPARED_QUERY", "yes")
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
      .put("CAP_QUERY_NULL_REQUIRES_CAST", "yes")
      .put("CAP_QUERY_SELECT_ALIASES_SORTED", "no")
      .put("CAP_QUERY_SORT_BY_DEGREE", "no")
      .put("CAP_QUERY_SUBQUERIES", "yes")
      .put("CAP_QUERY_SUBQUERIES_WITH_TOP", "yes")
      .put("CAP_QUERY_SUBQUERY_QUERY_CONTEXT", "yes")
      .put("CAP_QUERY_TOPSTYLE_LIMIT", "yes")
      .put("CAP_QUERY_TOPSTYLE_ROWNUM", "no")
      .put("CAP_QUERY_TOPSTYLE_TOP", "no")
      .put("CAP_QUERY_TOP_0_METADATA", "no")
      .put("CAP_QUERY_TOP_N", "yes")
      .put("CAP_QUERY_WHERE_FALSE_METADATA", "yes")
      .put("CAP_SELECT_INTO", "no")
      .put("CAP_SELECT_TOP_INTO", "no")
      .put("CAP_SUPPRESS_CONNECTION_POOLING", "no")
      .put("CAP_SUPPRESS_DISCOVERY_QUERIES", "yes")
      .put("CAP_SUPPRESS_DISPLAY_LIMITATIONS", "yes")
      // No need to support CATALOG in Tableau
      .put("SQL_CATALOG_USAGE", "0")
      // TODO: DX-25509 - Investigate the additional customizations in the TableauMessageBodyGenerator
      // that are not present in the TDC file included with the installer.
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
  private final boolean customizationEnabled;
  private final TableauExportType exportType;

  @Inject
  public TableauMessageBodyGenerator(@Context Configuration configuration, NodeEndpoint endpoint, OptionManager optionManager) {
    super(endpoint, optionManager);
    this.customizationEnabled = MoreObjects.firstNonNull((Boolean) configuration.getProperty(CUSTOMIZATION_ENABLED), false);

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

    addTargetOutputFileHeader(datasetConfig, "tds", httpHeaders);
    try {
      final XMLStreamWriter xmlStreamWriter = xmlOutputFactory.createXMLStreamWriter(entityStream, "UTF-8");

      xmlStreamWriter.writeStartDocument("utf-8", "1.0");
      writeDatasource(xmlStreamWriter, datasetConfig, getHostname(httpHeaders), mediaType);
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
    xmlStreamWriter.writeAttribute("version", TABLEAU_VERSION);

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
    writeColumnAliases(xmlStreamWriter, datasetConfig);
    xmlStreamWriter.writeEndElement();
  }

  private void writeColumnAliases(XMLStreamWriter xmlStreamWriter, DatasetConfig datasetConfig) throws XMLStreamException {
    final List<Field> fields = getFieldsFromDatasetConfig(datasetConfig);
    xmlStreamWriter.writeStartElement("aliases");
    xmlStreamWriter.writeAttribute("enabled", "yes");
    xmlStreamWriter.writeEndElement();

    for (final Field field : fields) {
      final String fieldName = field.getName();
      // Ignore DREMIO_UPDATE_COLUMN, which is an additional column that
      // is returned as along with dataset columns the RecordSchema.
      if (DREMIO_UPDATE_COLUMN.equals(fieldName)) {
        continue;
      }
      final TableauColumnMetadata tableauColumnMetadata = new TableauColumnMetadata(field.getType().getTypeID(), fieldName);
      tableauColumnMetadata.serializeToXmlWriter(xmlStreamWriter);
    }
  }

  private void writeSdkConnection(XMLStreamWriter xmlStreamWriter, DatasetConfig datasetConfig, String hostname) throws XMLStreamException {
    final DatasetPath dataset = new DatasetPath(datasetConfig.getFullPathList());

    xmlStreamWriter.writeStartElement("connection");

    xmlStreamWriter.writeAttribute("class", "dremio");
    xmlStreamWriter.writeAttribute("dbname", InfoSchemaConstants.IS_CATALOG_NAME);

    // It has to match what is returned by the driver/Tableau
    xmlStreamWriter.writeAttribute("schema", dataset.toParentPath());
    xmlStreamWriter.writeAttribute("port", String.valueOf(getEndpoint().getUserPort()));
    xmlStreamWriter.writeAttribute("server", hostname);
    xmlStreamWriter.writeAttribute("username", "");

    String customExtraProperties = getOptionManager().getOption(EXTRA_NATIVE_CONNECTION_PROPERTIES);
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
    final String customExtraProperties = getOptionManager().getOption(EXTRA_CONNECTION_PROPERTIES);
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
    xmlStreamWriter.writeAttribute("port", String.valueOf(getEndpoint().getUserPort()));
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
    xmlStreamWriter.writeAttribute("port", String.valueOf(getEndpoint().getUserPort()));
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
    final List<Field> fields = getFieldsFromDatasetConfig(datasetConfig);
    if (fields == null) {
      return;
    }
    xmlStreamWriter.writeStartElement("metadata-records");

    for (final Field field : fields) {
      final String fieldName = field.getName();
      // Ignore DREMIO_UPDATE_COLUMN, which is an additional column that
      // is returned as along with dataset columns the RecordSchema.
      if (DREMIO_UPDATE_COLUMN.equals(fieldName)) {
        continue;
      }
      final ArrowTypeID fieldId = field.getType().getTypeID();

      xmlStreamWriter.writeStartElement("metadata-record");
      xmlStreamWriter.writeAttribute("class", "column");

      // field name needs to be surrounded by []
      writeElement(xmlStreamWriter, "local-name", "[" + fieldName + "]");
      writeElement(xmlStreamWriter, "local-type", TableauColumnMetadata.getTableauDataType(fieldId));

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

  /**
   * Returns a List of Fields extracted from the DatasetConfig Record Schema.
   * @param datasetConfig The DatasetConfig to extract the fields from.
   * @return a List of Fields.
   */
  private static List<Field> getFieldsFromDatasetConfig(DatasetConfig datasetConfig) {
    final ByteString recordSchema = datasetConfig.getRecordSchema();
    return BatchSchema.deserialize(recordSchema).getFields();
  }

  static class TableauColumnMetadata {
    @VisibleForTesting
    static final String TABLEAU_TYPE_ORDINAL = "ordinal";
    @VisibleForTesting
    static final String TABLEAU_TYPE_QUANTITATIVE = "quantitative";
    @VisibleForTesting
    static final String TABLEAU_TYPE_NOMINAL = "nominal";
    // Splits the string based on delimiter underscore or whitespace, while including the delimiter in the tokens.
    private static final Pattern COLUMN_NAME_SPLIT_REGEX_PATTERN = Pattern.compile("((?<=_)|(?=_)|(?<=\\s)|(?=\\s))");
    private final String caption;
    private final String dataType;
    private final String name;
    private final TableauRole role;
    private final String type;

    enum TableauRole {
      DIMENSION("dimension"),
      MEASURE("measure");

      private final String role;

      TableauRole(String role) {
        this.role = role;
      }

      @Override
      public String toString() {
        return role;
      }
    }

    static final Map<ArrowTypeID, String> ARROW_TABLEAU_DATATYPES_MAPPINGS = ImmutableMap.<ArrowTypeID, String>builder()
      .put(ArrowTypeID.Int, "integer")
      // We map Dremio Decimal to Tableau real as the datatype attribute in the column element
      // does not have a decimal datatype attribute.
      // The valid values are boolean, date, datetime, integer, real and string.
      // Also verified that the TDS generated by Tableau treats Dremio decimal as real.
      .put(ArrowTypeID.Decimal, "real")
      .put(ArrowTypeID.FloatingPoint, "real")
      .put(ArrowTypeID.Bool, "boolean")
      .put(ArrowTypeID.Date, "date")
      // Tableau doesn't have a time type, it just uses datetime in that case
      .put(ArrowTypeID.Time, "datetime")
      .put(ArrowTypeID.Timestamp, "datetime")
      .put(ArrowTypeID.Utf8, "string")
      .build();

    static final List<String> TABLEAU_ROLE_DIMENSION_STARTS_WITH_KEYWORDS = new ArrayList<String>(){
      {
        add("code");
        add("id");
        add("key");
      }};

    static final List<String> TABLEAU_ROLE_DIMENSION_ENDS_WITH_KEYWORDS = new ArrayList<String>(){
      {
        add("code");
        add("id");
        add("key");
        add("number");
        add("num");
        add("nbr");
        add("year");
        add("yr");
        add("day");
        add("week");
        add("wk");
        add("month");
        add("quarter");
        add("qtr");
        add("fy");
      }};

    TableauColumnMetadata(ArrowTypeID fieldId, String fieldName) {
      this.caption = getPrettyColumnName(fieldName);
      this.dataType = getTableauDataType(fieldId);
      this.name = "[" + fieldName + "]";
      this.role = getTableauRole(fieldId, fieldName);
      this.type = getTableauType(fieldId, this.role);
    }

    void serializeToXmlWriter(XMLStreamWriter xmlStreamWriter) throws XMLStreamException {
      xmlStreamWriter.writeStartElement("column");
      xmlStreamWriter.writeAttribute("caption", caption);
      xmlStreamWriter.writeAttribute("datatype", dataType);
      xmlStreamWriter.writeAttribute("name", name);
      xmlStreamWriter.writeAttribute("role", role.toString());
      xmlStreamWriter.writeAttribute("type", type);
      xmlStreamWriter.writeEndElement();
    }

    /**
     * Converts the column name to a pretty column name with the following rules:
     * Non-trailing and non-leading underscore are replaced with whitespace.
     * Leading, trailing and multiple underscore remain as is.
     * Leading and trailing whitespace are removed.
     * Character after a whitespace or underscore is capitalized.
     * Converts everything to CamelCase. Ignoring rules about all CAPS as separate word.
     * Adds a space for CamelCase separation E.g. ThisCase becomes “This Case”.
     * All other rules are ignored.
     * https://help.tableau.com/current/pro/desktop/en-us/data_clean_adm.htm
     *
     * @param columnName The column name to convert to a pretty column name.
     * @return The pretty column name.
     */
    @VisibleForTesting
    static String getPrettyColumnName(String columnName) {
      // Remove leading and trailing spaces.
      final String trimmedColumnName = columnName.trim();
      if (Strings.isNullOrEmpty(trimmedColumnName) || trimmedColumnName.length() == 1) {
        // Convert single char column name to uppercase.
        return trimmedColumnName.toUpperCase();
      }
      // COLUMN_NAME_SPLIT_REGEX_PATTERN Splits the trimmed column name into tokens based on delimiters
      // underscore or whitespace, and includes the delimiters in the tokens.
      // For example the column name "hello_world hello_" is split into the following tokens.
      // "hello", "_", "world", " ", "hello", "_"
      // Based on the delimiter and its position we determine which rule should be applied.
      final String[] tokens = COLUMN_NAME_SPLIT_REGEX_PATTERN.split(trimmedColumnName);
      // The column name contains at least one whitespace or underscore
      if (tokens.length > 1) {
        return processWhitespaceAndUnderscore(tokens);
      }
      return processCamelCase(trimmedColumnName);
    }

    /**
     * Processes spaces and underscores based on their position in the column name
     * and on the basis of the rules defined below:
     * Non-trailing and non-leading underscore are replaced with whitespace.
     * Leading, trailing and multiple underscore remain as is.
     * The character after a whitespace or underscore is capitalized.
     *
     * @param tokens The column name split into tokens, based on the delimiters
     * @return The column where the spaces and the underscores have been processed as
     * per the rules.
     */
    private static String processWhitespaceAndUnderscore(String[] tokens) {
      final StringBuilder prettyColumnName = new StringBuilder();
      // We use this to keep track of the underscores.
      final StringBuilder underscoreBuffer = new StringBuilder();
      for (int i = 0; i < tokens.length; i++) {
        final String currentToken = tokens[i];
        if ("_".equals(currentToken)) {
          underscoreBuffer.append(currentToken);
        } else {
          // Non-trailing and non-leading underscore are replaced with whitespace.
          if (underscoreBuffer.length() == 1 && i > 1) {
            prettyColumnName.append(" ");
          } else {
            // Leading and multiple underscore remain as is.
            prettyColumnName.append(underscoreBuffer);
          }
          // Clear the builder.
          underscoreBuffer.setLength(0);
          // Convert non-underscore and non-whitespace tokens to CamelCase.
          prettyColumnName.append(currentToken.substring(0, 1).toUpperCase())
            .append(currentToken.substring(1).toLowerCase());
        }
      }
      // Trailing underscores remain as is.
      prettyColumnName.append(underscoreBuffer);
      return prettyColumnName.toString();
    }

    /**
     * Adds spaces to the column name if it contains CamelCase, otherwise converts
     * the column name to CamelCase as per the rules below:
     * Converts everything to CamelCase. Ignoring rules about all CAPS as separate word.
     * Adds a space for CamelCase separation E.g. ThisCase becomes “This Case”.
     *
     * @param columnName The column name to add spaces
     * @return The column
     */
    private static String processCamelCase(String columnName) {
      final StringBuilder stringBuilder = new StringBuilder();
      // We use this to keep track of the lowercase characters in the CamelCase.
      final StringBuilder lowercaseBuffer = new StringBuilder();
      for (int i = 0; i < columnName.length(); i++) {
        final char currentChar = columnName.charAt(i);
        if (i == 0) {
          // Capitalize the first character for CamelCase
          stringBuilder.append(Character.toUpperCase(currentChar));
        } else {
          // Detect a CamelCase and add a space for CamelCase separation.
          if (isLowerCase(columnName.charAt(i - 1)) && isUpperCase(currentChar)) {
            stringBuilder.append(lowercaseBuffer.toString())
              .append(" ")
              .append(currentChar);
            // Clear the builder.
            lowercaseBuffer.setLength(0);
          } else {
            lowercaseBuffer.append(Character.toLowerCase(currentChar));
          }
        }
      }
      // Add the remaining lowercase characters.
      stringBuilder.append(lowercaseBuffer);
      return stringBuilder.toString();
    }

    /**
     * Returns the Tableau DataType equivalent of the ArrowType.
     *
     * @param fieldID The ArrowTypeID that corresponds to the column.
     * @return The Tableau DataType.
     */
    @VisibleForTesting
    static String getTableauDataType(ArrowTypeID fieldID) {
      final String tableauDatatype = ARROW_TABLEAU_DATATYPES_MAPPINGS.get(fieldID);
      if (tableauDatatype == null) {
        // For all other types we default to string.
        return "string";
      }
      return tableauDatatype;
    }

    /**
     * Returns the Tableau Role for a particular column with the given ArrowType.
     * Dates and text values are dimensions, and numbers are measures.
     * https://help.tableau.com/current/pro/desktop/en-us/datafields_typesandroles_datatypes.htm
     *
     * @param fieldID   The ArrowTypeID that corresponds to the column.
     * @param fieldName The original column name
     * @return The Tableau Role either measure or dimension.
     */
    @VisibleForTesting
    static TableauRole getTableauRole(ArrowTypeID fieldID, String fieldName) {
      if ((fieldID == ArrowTypeID.Int || fieldID == ArrowTypeID.Decimal || fieldID == ArrowTypeID.FloatingPoint)
        && !treatFieldAsDimension(fieldName)) {
        return TableauRole.MEASURE;
      } else {
        return TableauRole.DIMENSION;
      }
    }

    /**
     * Determines if the the field should be treated as a Tableau Role Dimension.
     * based on if the column name starts or ends with certain keyword.
     *
     * @param columnName The column name to check the keyword in.
     * @return True if column name starts or ends with the certain keyword, false otherwise.
     */
    private static boolean treatFieldAsDimension(String columnName) {
      return (TABLEAU_ROLE_DIMENSION_STARTS_WITH_KEYWORDS.stream().anyMatch(prefix -> columnName.regionMatches(
        true, 0, prefix, 0, prefix.length()))
        || TABLEAU_ROLE_DIMENSION_ENDS_WITH_KEYWORDS.stream().anyMatch(suffix -> columnName.regionMatches(
        true, columnName.length() - suffix.length(), suffix, 0, suffix.length()))
      );
    }

    /**
     * Returns the Tableau Type attribute for a column element in the TDS file.
     *
     * @param fieldID The ArrowTypeID that corresponds to the column.
     * @return The Tableau Type, ordinal, quantitative or nominal.
     */
    @VisibleForTesting
    static String getTableauType(ArrowTypeID fieldID, TableauRole tableauRole) {
      if (fieldID == ArrowTypeID.Int || fieldID == ArrowTypeID.Decimal || fieldID == ArrowTypeID.FloatingPoint) {
        if (tableauRole == TableauRole.DIMENSION) {
          // If the field it is Discrete Dimension with numeric values, it will be ‘ordinal’.
          // https://tableauandbehold.com/2016/06/29/defining-a-tableau-data-source-programmatically/
          return TABLEAU_TYPE_ORDINAL;
        } else {
          return TABLEAU_TYPE_QUANTITATIVE;
        }
      } else {
        return TABLEAU_TYPE_NOMINAL;
      }
    }
  }
}


