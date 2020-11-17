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

package com.dremio.exec.store.ischema;

import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.calcite.rel.type.RelDataType;

import com.dremio.common.exceptions.UserException;
import com.dremio.common.expression.SchemaPath;
import com.dremio.exec.planner.sql.CalciteArrowHelper;
import com.dremio.exec.planner.types.JavaTypeFactoryImpl;
import com.dremio.exec.record.BatchSchema;
import com.dremio.exec.store.AbstractRecordReader;
import com.dremio.exec.store.ischema.metadata.InformationSchemaMetadata;
import com.dremio.exec.store.ischema.writers.CatalogsTableWriter;
import com.dremio.exec.store.ischema.writers.ColumnsTableWriter;
import com.dremio.exec.store.ischema.writers.SchemataTableWriter;
import com.dremio.exec.store.ischema.writers.TableWriter;
import com.dremio.exec.store.ischema.writers.TablesTableWriter;
import com.dremio.exec.store.ischema.writers.ViewsTableWriter;
import com.dremio.sabot.exec.context.OperatorContext;
import com.dremio.sabot.op.scan.OutputMutator;
import com.dremio.service.catalog.InformationSchemaServiceGrpc.InformationSchemaServiceBlockingStub;
import com.dremio.service.catalog.ListCatalogsRequest;
import com.dremio.service.catalog.ListSchemataRequest;
import com.dremio.service.catalog.ListTableSchemataRequest;
import com.dremio.service.catalog.ListTablesRequest;
import com.dremio.service.catalog.ListViewsRequest;
import com.dremio.service.catalog.SearchQuery;
import com.dremio.service.catalog.TableSchema;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.AbstractIterator;
import com.google.common.collect.Iterators;

import io.grpc.Context;

/**
 * Record reader for tables in information_schema source.
 */
public class InformationSchemaRecordReader extends AbstractRecordReader {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(InformationSchemaRecordReader.class);
  private final InformationSchemaServiceBlockingStub catalogStub;
  private final InformationSchemaTable table;
  private final String catalogName;
  private final String username;
  private final SearchQuery searchQuery;
  private final boolean complexTypeSupport;

  private Context.CancellableContext context;
  private TableWriter<?> tableWriter;

  public InformationSchemaRecordReader(
    OperatorContext context,
    List<SchemaPath> fields,
    InformationSchemaServiceBlockingStub catalogStub,
    InformationSchemaTable table,
    String catalogName,
    String username,
    SearchQuery searchQuery,
    boolean complexTypeSupport
  ) {
    super(context, fields);
    this.catalogStub = catalogStub;
    this.table = table;
    this.catalogName = catalogName;
    this.username = username;
    this.searchQuery = searchQuery;
    this.complexTypeSupport = complexTypeSupport;
  }

  @Override
  public void setup(OutputMutator output) {
    context = Context.current().withCancellation();
    context.run(() -> {
      tableWriter = createTableWriter();
      tableWriter.init(output);
    });
  }

  @Override
  public int next() {
    Preconditions.checkNotNull(tableWriter, "Reader must be #setup first");
    return tableWriter.write(numRowsPerBatch);
  }

  @Override
  public void close() throws Exception {
    if (context != null) {
      context.close();
    }

    context = null;
    tableWriter = null;
  }

  @Override
  protected boolean supportsSkipAllQuery() {
    return true;
  }

  private Set<String> getGivenFields() {
    return getColumns()
      .stream()
      .peek(path -> Preconditions.checkArgument(path.isSimplePath()))
      .map(path -> path.getAsUnescapedPath().toUpperCase())
      .collect(Collectors.toSet());
  }

  private TableWriter<?> createTableWriter() {
    final Set<String> selectedFields =
      isStarQuery() ? InformationSchemaMetadata.getAllFieldNames(table.getRecordSchema()) : getGivenFields();

    switch (table) {

      case CATALOGS: {
        final ListCatalogsRequest.Builder catalogsRequest = ListCatalogsRequest.newBuilder()
          .setUsername(username);
        if (searchQuery != null) {
          catalogsRequest.setQuery(searchQuery);
        }

        // start Catalog stream from catalog service
        return new CatalogsTableWriter(catalogStub.listCatalogs(catalogsRequest.build()), selectedFields, catalogName);
      }

      case SCHEMATA: {
        final ListSchemataRequest.Builder schemataRequest = ListSchemataRequest.newBuilder()
          .setUsername(username);
        if (searchQuery != null) {
          schemataRequest.setQuery(searchQuery);
        }

        // start Schema stream from catalog service
        return new SchemataTableWriter(catalogStub.listSchemata(schemataRequest.build()), selectedFields, catalogName);
      }

      case TABLES: {
        final ListTablesRequest.Builder tablesRequest = ListTablesRequest.newBuilder()
          .setUsername(username);
        if (searchQuery != null) {
          tablesRequest.setQuery(searchQuery);
        }

        // start Table stream from catalog service
        return new TablesTableWriter(catalogStub.listTables(tablesRequest.build()), selectedFields, catalogName);
      }

      case VIEWS: {
        final ListViewsRequest.Builder viewsRequest = ListViewsRequest.newBuilder()
          .setUsername(username);
        if (searchQuery != null) {
          viewsRequest.setQuery(searchQuery);
        }

        // start View stream from catalog service
        return new ViewsTableWriter(catalogStub.listViews(viewsRequest.build()), selectedFields, catalogName);
      }

      case COLUMNS: {
        final ListTableSchemataRequest.Builder columnsRequest = ListTableSchemataRequest.newBuilder()
          .setUsername(username);
        if (searchQuery != null) {
          columnsRequest.setQuery(searchQuery);
        }
        // start TableSchema stream from catalog service
        final Iterator<TableSchema> tableSchemata = catalogStub.listTableSchemata(columnsRequest.build());

        // For each TableSchema, iterates over #flatMap of batch_schema field, which represents the records in the
        // "COLUMNS" table, and not the TableSchema message itself (unlike other tables).
        final Iterator<Column> columnIterator = new AbstractIterator<Column>() {
          Iterator<Column> currentIterator = null;

          @Override
          protected Column computeNext() {
            while (true) {
              if (currentIterator != null && currentIterator.hasNext()) {
                return currentIterator.next();
              }

              if (!tableSchemata.hasNext()) {
                return endOfData();
              }

              // Gets next TableSchema from the catalog service only after exhausting the current one. See comment in
              // TableWriter#write.
              final TableSchema currentSchema = tableSchemata.next();
              BatchSchema bs = BatchSchema.deserialize(currentSchema.getBatchSchema().toByteArray());
              //If an inconsistency is detected don't attempt converting to Arrow format since it will cause an assertion failure.  Put out a warning and move on to next row.
              if (bs.getFieldCount() == 0) {
                // Add a warning message to indicate this table has missing fields
                logger.warn("{}.{}.{} has missing fields or incorrect format. ", currentSchema.getCatalogName(), currentSchema.getSchemaName(), currentSchema.getTableName());
                continue;
              }
              final RelDataType rowType =
                CalciteArrowHelper.wrap(bs)
                  .toCalciteRecordType(JavaTypeFactoryImpl.INSTANCE, complexTypeSupport);
              //noinspection ConstantConditions
              currentIterator = Iterators.transform(rowType.getFieldList().iterator(),
                field -> new Column(Strings.isNullOrEmpty(catalogName) ? currentSchema.getCatalogName() : catalogName,
                  currentSchema.getSchemaName(),
                  currentSchema.getTableName(),
                  field));
            }
          }
        };
        return new ColumnsTableWriter(columnIterator, selectedFields, catalogName);
      }
      default:
        throw UserException.unsupportedError()
          .message("InformationSchemaRecordReader does not support table of '%s' type", table)
          .buildSilently();
    }
  }
}
