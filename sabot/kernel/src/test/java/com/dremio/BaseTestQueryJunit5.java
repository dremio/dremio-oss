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
package com.dremio;

import com.dremio.common.AutoCloseables;
import java.util.Properties;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocatorFactory;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestInfo;

/**
 * This class extends the {@link BaseTestQuery} class and serves as the base class for JUnit 5 test
 * classes. It inherits all the methods and properties from the {@link BaseTestQuery} class.
 *
 * <p>It should be used as base test class if you wish to use junit5.
 */
public class BaseTestQueryJunit5 extends BaseTestQuery {

  // Allocators for reading query results, compare to ExecTest (but it's still on JUnit4)
  private BufferAllocator rootAllocator;

  @BeforeAll
  static void beforeClass() throws Throwable {
    // junit5 tests will ignore the @ClassRule annotation, so we have to reset it manually
    SABOT_NODE_RULE.before();
  }

  @AfterAll
  static void afterClass() throws Throwable {
    // junit5 tests will ignore the @ClassRule annotation, so we have to reset it manually
    SABOT_NODE_RULE.after();

    // the class setup/teardown methods of BaseTestQuery use junit4 annotations and are
    // thus not called automatically when running with junit5.
    // since closeClient is idempotent we call it universally here to prevent resource leaks.
    BaseTestQuery.closeClient();
  }

  @BeforeEach
  public void resetTestClient() throws Exception {
    updateClient((Properties) null);
  }

  @BeforeEach
  public void setUpAllocators(TestInfo testInfo) throws Throwable {
    // Same as ExecTest.initAllocators() (stuck on Junit4) but using junit5 TestInfo
    rootAllocator = RootAllocatorFactory.newRoot(DEFAULT_SABOT_CONFIG);
    allocator =
        rootAllocator.newChildAllocator(testInfo.getDisplayName(), 0, rootAllocator.getLimit());
  }

  @AfterEach
  public void tearDownAllocators() throws Exception {
    AutoCloseables.close(allocator, rootAllocator);
  }
}
