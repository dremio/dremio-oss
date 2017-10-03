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
package com.dremio.dac.explore.bi;

import static com.dremio.common.utils.SqlUtils.QUOTE;

import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.lang.annotation.Annotation;
import java.lang.reflect.Type;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

import javax.ws.rs.Produces;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.ext.MessageBodyWriter;

import org.apache.calcite.sql.type.SqlTypeName;

import com.dremio.common.utils.SqlUtils;
import com.dremio.dac.explore.model.DatasetPath;
import com.dremio.dac.server.WebServer;
import com.dremio.exec.util.ViewFieldsHelper;
import com.dremio.service.namespace.dataset.proto.DatasetConfig;
import com.dremio.service.namespace.dataset.proto.ViewFieldType;
import com.google.common.base.CharMatcher;
import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableListMultimap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;

/**
 * A {@link VirtualDatasetUI} serializer as Qlik Sense apps
 */
@Produces(WebServer.MediaType.TEXT_PLAIN_QLIK_APP)
public class QlikAppMessageBodyGenerator implements MessageBodyWriter<DatasetConfig> {

  private static enum QlikFieldType {
    DIMENSION,
    MEASURE,
    DETAIL
  }

  public QlikAppMessageBodyGenerator() {
  }

  @Override
  public boolean isWriteable(Class<?> type, Type genericType, Annotation[] annotations, MediaType mediaType) {
    return type == DatasetConfig.class;
  }

  @Override
  public long getSize(DatasetConfig t, Class<?> type, Type genericType, Annotation[] annotations, MediaType mediaType) {
    return -1;
  }

  private static String quoteString(String str) {
    return "\"" + CharMatcher.is('"').replaceFrom(str, "\"\"") + "\"";
  }

  private static final Function<ViewFieldType, String> FIELD_TO_NAME = new Function<ViewFieldType, String>() {
    @Override
    public String apply(ViewFieldType input) {
      String name = input.getName();

      // Qlik needs field names that contain spaces to be surrounded by " so we surround all field names.
      // If the field name contains a ", we escape it with "" (per http://help.qlik.com/en-US/sense/3.1/Subsystems/Hub/Content/Scripting/use-quotes-in-script.htm)
      return quoteString(name);
    }
  };

  private static final Function<ViewFieldType, QlikFieldType> FIELD_TO_QLIKTYPE = new Function<ViewFieldType, QlikFieldType>() {
    @Override
    public QlikFieldType apply(ViewFieldType input) {
      // TODO DX-7003: Ideally, cardinality should be at play here
      SqlTypeName typeName = SqlTypeName.valueOf(input.getType());
      switch (typeName) {
      case CHAR:
      case VARCHAR:
        return QlikFieldType.DIMENSION;

      case BOOLEAN:
      case BIGINT:
      case DATE:
      case DECIMAL:
      case DOUBLE:
      case FLOAT:
      case INTEGER:
      case INTERVAL_YEAR:
      case INTERVAL_YEAR_MONTH:
      case INTERVAL_MONTH:
      case INTERVAL_DAY:
      case INTERVAL_DAY_HOUR:
      case INTERVAL_DAY_MINUTE:
      case INTERVAL_DAY_SECOND:
      case INTERVAL_HOUR:
      case INTERVAL_HOUR_MINUTE:
      case INTERVAL_HOUR_SECOND:
      case INTERVAL_MINUTE:
      case INTERVAL_MINUTE_SECOND:
      case INTERVAL_SECOND:
      case REAL:
      case SMALLINT:
      case TIME:
      case TINYINT:
        return QlikFieldType.MEASURE;

      default:
      }
      return QlikFieldType.DETAIL;
    }
  };

  @Override
  public void writeTo(DatasetConfig dataset, Class<?> type, Type genericType, Annotation[] annotations,
      MediaType mediaType, MultivaluedMap<String, Object> httpHeaders, OutputStream entityStream)
      throws IOException, WebApplicationException {

    List<ViewFieldType> viewFieldTypes = ViewFieldsHelper.getViewFields(dataset);

    ImmutableListMultimap<QlikFieldType, ViewFieldType> mapping = FluentIterable.from(viewFieldTypes).index(FIELD_TO_QLIKTYPE);

    DatasetPath datasetPath = new DatasetPath(dataset.getFullPathList());

    // qlik doesn't like certain characters as an identifier, so lets remove them
    String sanitizedDatasetName = dataset.getName().replaceAll("["+ Pattern.quote("/!@ *-=+{}<>,~")+"]+", "_");

    try (PrintWriter pw = new PrintWriter(entityStream)) {
      pw.println("SET DirectIdentifierQuoteChar='" + QUOTE + "';");
      pw.println();
      pw.println("LIB CONNECT TO 'Dremio';");
      pw.println();
      // Create a resident load so that data can be referenced later
      pw.format("%s: DIRECT QUERY\n", sanitizedDatasetName);
      for(Map.Entry<QlikFieldType, Collection<ViewFieldType>> entry: mapping.asMap().entrySet()) {
        writeFields(pw, entry.getKey().name(), entry.getValue());
      }

      /* Qlik supports paths with more than 2 components ("foo"."bar"."baz"."boo"), but each individual path segment
       * must be quoted to work with Dremio.  SqlUtils.quoteIdentifier will only quote when needed, so we do another
       * pass to ensure they are quoted. */
      final List<String> quotedPathComponents = Lists.newArrayList();
      for (final String component : dataset.getFullPathList()) {
        String quoted = SqlUtils.quoteIdentifier(component);
        if (!quoted.startsWith(String.valueOf(SqlUtils.QUOTE)) || !quoted.endsWith(String.valueOf(SqlUtils.QUOTE))) {
          quoted = quoteString(quoted);
        }
        quotedPathComponents.add(quoted);
      }

      pw.format("  FROM %1$s;\n", Joiner.on('.').join(quotedPathComponents));
    }
  }

  private void writeFields(PrintWriter pw, String fieldType, Collection<ViewFieldType> fields) throws IOException {
    if (!fields.isEmpty()) {
      pw.format("  %s ", fieldType);

      Joiner.on(", ").appendTo(pw, Iterables.transform(fields, FIELD_TO_NAME));
      pw.println();
    }
  }
}
