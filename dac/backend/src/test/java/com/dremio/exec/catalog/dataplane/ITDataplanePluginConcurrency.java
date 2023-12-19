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
package com.dremio.exec.catalog.dataplane;

import static com.dremio.exec.catalog.dataplane.DataplaneTestDefines.DEFAULT_BRANCH_NAME;
import static com.dremio.exec.catalog.dataplane.DataplaneTestDefines.alterTableChangeColumnQuery;
import static com.dremio.exec.catalog.dataplane.DataplaneTestDefines.createEmptyTableQuery;
import static com.dremio.exec.catalog.dataplane.DataplaneTestDefines.createReplaceViewQuery;
import static com.dremio.exec.catalog.dataplane.DataplaneTestDefines.createViewQuery;
import static com.dremio.exec.catalog.dataplane.DataplaneTestDefines.dropTableQuery;
import static com.dremio.exec.catalog.dataplane.DataplaneTestDefines.generateUniqueTableName;
import static com.dremio.exec.catalog.dataplane.DataplaneTestDefines.generateUniqueViewName;
import static com.dremio.exec.catalog.dataplane.DataplaneTestDefines.insertTableQuery;
import static com.dremio.exec.catalog.dataplane.DataplaneTestDefines.tablePathWithFolders;
import static com.dremio.exec.catalog.dataplane.ITDataplanePluginConcurrency.Operation.ALTER_TABLE_COLUMN;
import static com.dremio.exec.catalog.dataplane.ITDataplanePluginConcurrency.Operation.CREATE_EMPTY_TABLE;
import static com.dremio.exec.catalog.dataplane.ITDataplanePluginConcurrency.Operation.CREATE_VIEW;
import static com.dremio.exec.catalog.dataplane.ITDataplanePluginConcurrency.Operation.INSERT;
import static com.dremio.exec.catalog.dataplane.ITDataplanePluginConcurrency.Operation.REPLACE_VIEW;
import static com.dremio.exec.catalog.dataplane.TestDataplaneAssertions.assertNessieHasTable;
import static com.dremio.exec.catalog.dataplane.TestDataplaneAssertions.assertNessieHasView;
import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

import org.junit.jupiter.api.Test;

import com.dremio.catalog.model.VersionContext;

public class ITDataplanePluginConcurrency extends ITDataplanePluginTestSetup {

  enum Operation {
    INSERT,
    CREATE_EMPTY_TABLE,
    CREATE_VIEW,
    ALTER_TABLE_COLUMN,
    REPLACE_VIEW
  }

  static class QueryOperationThread extends Thread {
    private final String threadName;

    private final List<List<String>> tablePaths;
    private final List<List<String>> viewPaths;
    private final CountDownLatch latch;
    private final int totalRows;
    private int index;
    private final AtomicBoolean isExceptionFound;
    private final Set<Operation> operations;

    QueryOperationThread(String threadName,
                         List<List<String>> tablePaths,
                         List<List<String>> viewPaths,
                         int index,
                         CountDownLatch latch,
                         int totalRows,
                         AtomicBoolean isExceptionFound,
                         Set<Operation> operations) {
      this.threadName = threadName;
      this.tablePaths = tablePaths;
      this.viewPaths = viewPaths;
      this.latch = latch;
      this.totalRows = totalRows;
      this.index = index;
      this.isExceptionFound = isExceptionFound;
      this.operations = operations;
    }

    QueryOperationThread(String threadName,
                         List<List<String>> tablePaths,
                         List<List<String>> viewPaths,
                         CountDownLatch latch,
                         int totalRows,
                         AtomicBoolean isExceptionFound,
                         Set<Operation> operations) {
      this(threadName, tablePaths, viewPaths, -1, latch, totalRows, isExceptionFound, operations);
    }

    QueryOperationThread(String threadName,
                         List<List<String>> tablePath,
                         int index,
                         CountDownLatch latch,
                         int totalRows,
                         AtomicBoolean isExceptionFound,
                         Set<Operation> operations) {
      this(threadName, tablePath, null, index, latch, totalRows, isExceptionFound, operations);
    }

    @Override
    public void run() {
      try {
        for (int row = 0; row < totalRows; row++) {
          if (index == -1) {
            index = (int) (Math.random() * tablePaths.size());
          }
          if (operations.contains(INSERT)) {
            runSQL(insertTableQuery(tablePaths.get(index)));
          }
          if (operations.contains(CREATE_EMPTY_TABLE)) {
            createFolders(tablePaths.get(index), VersionContext.ofBranch(DEFAULT_BRANCH_NAME));
            runSQL(createEmptyTableQuery(tablePaths.get(index)));
          }
          if (operations.contains(CREATE_VIEW)) {
            createFolders(viewPaths.get(index), VersionContext.ofBranch(DEFAULT_BRANCH_NAME));
            runSQL(createViewQuery(viewPaths.get(index), tablePaths.get(index)));
          }
          if (operations.contains(ALTER_TABLE_COLUMN)) {
            final List<String> changeColDef = Collections.singletonList("\"id\" \"id\" int");
            runSQL(alterTableChangeColumnQuery(tablePaths.get(index), changeColDef));
          }
          if (operations.contains(REPLACE_VIEW)) {
            runSQL(createReplaceViewQuery(viewPaths.get(index), tablePaths.get(index)));
          }
        }
      } catch (Exception e) {
        isExceptionFound.set(true);
      } finally {
        latch.countDown();
      }
    }
  }

  @Test
  public void insertIntoSameEmptyTableConcurrently() throws Exception {
    // Arrange
    final String tableName = generateUniqueTableName();
    final List<String> tablePath = tablePathWithFolders(tableName);
    createFolders(tablePath, VersionContext.ofBranch(DEFAULT_BRANCH_NAME));
    runSQL(createEmptyTableQuery(tablePath));

    // Act
    int numberOfThreads = 2;
    int totalRows = 1;
    ExecutorService executor = Executors.newFixedThreadPool(numberOfThreads);
    CountDownLatch latch = new CountDownLatch(numberOfThreads);
    final AtomicBoolean isException = new AtomicBoolean(false);
    final Set<Operation> operations = new HashSet<>();
    operations.add(INSERT);
    for (int i = 0; i < numberOfThreads; i++) {
      String threadName = String.format("insert-nessie-thread-%d", i + 1);
      executor.execute(() -> {
        new QueryOperationThread(threadName, Collections.singletonList(tablePath), 0, latch, totalRows, isException, operations).start();
      });
    }
    executor.shutdown();

    // wait for the latch to be decremented to 0 by the 3 threads
    latch.await();

    //Assert
    assertTableHasExpectedNumRows(tablePath, numberOfThreads * totalRows * 3);
    assertThat(isException.get()).isFalse();

    // cleanup
    runSQL(dropTableQuery(tablePath));
  }

  @Test
  public void insertIntoDifferentEmptyTablesConcurrently() throws Exception {
    // Arrange
    final String tableName = generateUniqueTableName();
    final List<String> tablePath1 = tablePathWithFolders(tableName);
    final List<String> tablePath2 = tablePathWithFolders(tableName);
    final List<String> tablePath3 = tablePathWithFolders(tableName);
    createFolders(tablePath1, VersionContext.ofBranch(DEFAULT_BRANCH_NAME));
    createFolders(tablePath2, VersionContext.ofBranch(DEFAULT_BRANCH_NAME));
    createFolders(tablePath3, VersionContext.ofBranch(DEFAULT_BRANCH_NAME));
    runSQL(createEmptyTableQuery(tablePath1));
    runSQL(createEmptyTableQuery(tablePath2));
    runSQL(createEmptyTableQuery(tablePath3));

    final List<List<String>> tablePaths = Arrays.asList(tablePath1, tablePath2, tablePath3);

    // Act
    int numberOfThreads = 3;
    int totalRows = 10;
    ExecutorService executor = Executors.newFixedThreadPool(numberOfThreads);
    CountDownLatch latch = new CountDownLatch(numberOfThreads);
    final AtomicBoolean isException = new AtomicBoolean(false);
    final Set<Operation> operations = new HashSet<>();
    operations.add(INSERT);
    for (int i = 0; i < 3; i++) {
      String threadName = String.format("insert-nessie-thread-%d", i + 1);
      int finalI = i;
      executor.execute(() -> {
        if (finalI == 0) {
          new QueryOperationThread(threadName, tablePaths, 0, latch, totalRows, isException, operations).start();
        } else if (finalI == 1) {
          new QueryOperationThread(threadName, tablePaths, 1, latch, totalRows, isException, operations).start();
        } else {
          new QueryOperationThread(threadName, tablePaths, 2, latch, totalRows, isException, operations).start();
        }
      });
    }
    executor.shutdown();

    // wait for the latch to be decremented to 0 by the 3 threads (on 3 different tables)
    latch.await();

    //Assert
    assertTableHasExpectedNumRows(tablePath1, totalRows * 3);
    assertTableHasExpectedNumRows(tablePath2, totalRows * 3);
    assertTableHasExpectedNumRows(tablePath3, totalRows * 3);
    assertThat(isException.get()).isFalse();

    // cleanup
    runSQL(dropTableQuery(tablePath1));
    runSQL(dropTableQuery(tablePath2));
    runSQL(dropTableQuery(tablePath3));
  }

  @Test
  public void concurrentDDLOperationsTests() throws Exception {
    final String tableName = generateUniqueTableName();
    final String viewName = generateUniqueViewName();

    //Creates a 12 different table paths to be used
    int totalTables = 12;
    List<List<String>> tablePaths = new ArrayList<>();
    List<List<String>> viewPaths = new ArrayList<>();
    for (int i = 0; i < totalTables; i++) {
      final List<String> tablePath = tablePathWithFolders(tableName);
      tablePaths.add(tablePath);
      final List<String> viewKey = tablePathWithFolders(viewName);
      viewPaths.add(viewKey);
    }

    // Act concurrently to create a set of folders and 12 empty tables/ views on those tables
    int numberOfThreads = 4;
    ExecutorService executor = Executors.newFixedThreadPool(numberOfThreads);
    final CountDownLatch latch1 = new CountDownLatch(totalTables);
    final AtomicBoolean isException1 = new AtomicBoolean(false);
    final Set<Operation> operations = new HashSet<>();
    operations.add(CREATE_EMPTY_TABLE);
    operations.add(CREATE_VIEW);
    for (int i = 0; i < totalTables; i++) {
      String threadName = String.format("create-empty-table-create-view-nessie-thread-%d", i + 1);
      int finalI = i;
      executor.submit(() -> new QueryOperationThread(threadName, tablePaths, viewPaths, finalI, latch1, 1, isException1, operations).start());
    }

    // wait for the latch to be decremented to 0
    latch1.await();

    //Assert to verify that create folders/ tables/ views worked and there is no exception
    assertThat(isException1.get()).isFalse();

    //Assert to verify all tables/ views gets created successfully
    for (int i = 0; i < totalTables; i++) {
      assertNessieHasTable(tablePaths.get(i), DEFAULT_BRANCH_NAME, this);
      assertNessieHasView(viewPaths.get(i), DEFAULT_BRANCH_NAME, this);
    }

    // Act concurrently for altering the table and altering the view
    final AtomicBoolean isException2 = new AtomicBoolean(false);
    final CountDownLatch latch2 = new CountDownLatch(numberOfThreads);
    operations.clear();
    operations.add(ALTER_TABLE_COLUMN);
    operations.add(REPLACE_VIEW);
    for (int i = 0; i < numberOfThreads; i++) {
      String threadName = String.format("alter-table-column-replace-view-nessie-thread-%d", i + 1);
      executor.submit(() -> new QueryOperationThread(threadName, tablePaths, viewPaths, latch2, 3, isException2, operations).start());
    }
    executor.shutdown();

    // wait for the latch to be decremented to 0
    latch2.await();

    //Assert to verify there is no exception for alter table and replacing view
    assertThat(isException2.get()).isFalse();
  }
}
