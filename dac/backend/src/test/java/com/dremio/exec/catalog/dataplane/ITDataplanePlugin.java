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


import static com.dremio.exec.ExecConstants.ENABLE_ICEBERG_ADVANCED_DML;
import static com.dremio.exec.ExecConstants.ENABLE_ICEBERG_VACUUM;
import static com.dremio.exec.ExecConstants.VERSIONED_INFOSCHEMA_ENABLED;
import static com.dremio.exec.catalog.dataplane.DataplaneTestDefines.NO_ANCESTOR;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.projectnessie.error.NessieConflictException;
import org.projectnessie.error.NessieNotFoundException;
import org.projectnessie.model.Branch;
import org.projectnessie.model.Detached;
import org.projectnessie.model.Tag;
import org.projectnessie.tools.compatibility.api.NessieServerProperty;

import com.dremio.BaseTestQuery;

/**
 * Integration tests for Dataplane Plugin which runs for Nessie source in software against Nessie server (new model)
 * which enforces namespace validation as default.
 * Two models are used in Nessie servers: "PERSIST" and "DATABASE_ADAPTER".
 * "PERSIST" is the new model and "DATABASE_ADAPTER" is the old model
 * By default, it uses the old model if no model name specified in config property "nessie.test.storage.kind" of NessieServer
 *
 * Note: Requires complex setup, see base class ITDataplanePluginTestSetup for
 * more details
 *
 * Adding new tests:
 *   - Check to see if your test(s) fit into an existing category, if so, add them there
 *   - If a new category of tests is needed, add the new set of tests in a
 *       separate file, and use the Nested pattern (see below) to define the new
 *       set of tests in this file.
 *
 * Instructions for running tests from Maven CLI:
 *   1) Run the entire ITDataplanePlugin test suite
 *     - mvn failsafe:integration-test -pl :dremio-dac-backend -Dit.test=ITDataplanePlugin
 *   2) Run a specific category, e.g. AlterTestCases
 *     - mvn failsafe:integration-test -pl :dremio-dac-backend -Dit.test="ITDataplanePlugin*NestedAlterTests"
 *   3) Run a specific test, e.g. AlterTestCases#alterTableAddOneColumn
 *     - mvn failsafe:integration-test -pl :dremio-dac-backend -Dit.test="ITDataplanePlugin*NestedAlterTests#alterTableAddOneColumn"
 *
 * Note: The syntax of these commands is very particular, check it carefully
 *   - e.g. goal must be failsafe:integration-test, not just integration-test
 *   - Nested class must be quoted with wildcard (*), even though $ separator should have worked
 *
 * Instructions for running tests from IntelliJ:
 *   For either of:
 *     1) Run the entire ITDataplanePlugin test suite
 *     2) Run a specific category, e.g. AlterTestCases
 *   IntelliJ can automatically create a run configuration for you.
 *
 *   Just click the green checkmark/arrow next to the top-level class for (1) or
 *   next to the Nested class for (2).
 *
 *   For:
 *     3) Run a specific test, e.g. AlterTestCases#alterTableAddOneColumn
 *   IntelliJ does not detect these automatically, so you'll have to manually
 *   modify one of the above auto-configurations. Change the configuration to
 *   the following values:
 *     - Dropdown set to "Method" (NOT "Class")
 *     - First text box set to: com.dremio.exec.catalog.dataplane.ITDataplanePlugin.NestedAlterTests
 *     - Second text box set to: alterTableAddOneColumn
 */
@NessieServerProperty(name = "nessie.test.storage.kind", value = "PERSIST")  //PERSIST is the new model in Nessie Server
public class ITDataplanePlugin extends ITDataplanePluginTestSetup {

  @BeforeAll
  public static void beforeAll() {
    setSystemOption(ENABLE_ICEBERG_ADVANCED_DML, "true");
    setSystemOption(VERSIONED_INFOSCHEMA_ENABLED, "true");
    setSystemOption(ENABLE_ICEBERG_VACUUM, "true");
  }

  @AfterAll
  public static void afterAll() {
    resetSystemOption(ENABLE_ICEBERG_ADVANCED_DML.getOptionName());
    resetSystemOption(VERSIONED_INFOSCHEMA_ENABLED.getOptionName());
    resetSystemOption(ENABLE_ICEBERG_VACUUM.getOptionName());
  }

  @BeforeEach
  // Since we can't set to an empty context, setting it to some known but *wrong* context on purpose.
  public void before() throws Exception {
    // Note: dfs_hadoop is immutable.
    BaseTestQuery.test("USE dfs_hadoop");
    Branch defaultBranch = getNessieClient().getDefaultBranch();
    getNessieClient().getAllReferences().stream()
      .forEach(
        ref -> {
          try {
            if (ref instanceof Branch && !ref.getName().equals(defaultBranch.getName())) {
              getNessieClient().deleteBranch().branch((Branch) ref).delete();
            } else if (ref instanceof Tag) {
              getNessieClient().deleteTag().tag((Tag) ref).delete();
            }
          } catch (NessieConflictException | NessieNotFoundException e) {
            throw new RuntimeException(e);
          }
        });

    getNessieClient().assignBranch().branch(defaultBranch).assignTo(Detached.of(NO_ANCESTOR)).assign();
  }

  @Nested
  class NestedInfoSchemaTests extends InfoSchemaTestCases {
    NestedInfoSchemaTests() {
      super(ITDataplanePlugin.this);
    }
  }

  @Nested
  class NestedInfoSchemaCombinationTests extends InfoSchemaCombinationTestCases {
    NestedInfoSchemaCombinationTests() {
      super(ITDataplanePlugin.this);
    }
  }

  @Nested
  class NestedFolderTests extends FolderTestCases {
    NestedFolderTests() {super(ITDataplanePlugin.this);}
  }

  @Nested
  class NestedAlterTests extends AlterTestCases {
    NestedAlterTests() {
      super(ITDataplanePlugin.this);
    }
  }

  @Nested
  class NestedBranchTagTests extends BranchTagTestCases {
    NestedBranchTagTests() {
      super(ITDataplanePlugin.this);
    }
  }

  @Nested
  class NestedCreateTests extends CreateTestCases {
    NestedCreateTests() {
      super(ITDataplanePlugin.this);
    }
  }

  @Nested
  class NestedCtasTests extends CtasTestCases {
    NestedCtasTests() {
      super(ITDataplanePlugin.this);
    }
  }

  @Nested
  class NestedDeleteTests extends DeleteTestCases {
    NestedDeleteTests() {
      super(ITDataplanePlugin.this);
    }
  }

  @Nested
  class NestedDropTests extends DropTestCases {
    NestedDropTests() {
      super(ITDataplanePlugin.this);
    }
  }

  @Nested
  class NestedInsertTests extends InsertTestCases {
    NestedInsertTests() {
      super(ITDataplanePlugin.this);
    }
  }

  @Nested
  class NestedSelectTests extends SelectTestCases {
    NestedSelectTests() {
      super(ITDataplanePlugin.this);
    }
  }

  @Nested
  class NestedMergeTests extends MergeTestCases {
    NestedMergeTests() {
      super(ITDataplanePlugin.this);
    }
  }

  @Nested
  class NestedUpdateTests extends UpdateTestCases {
    NestedUpdateTests() {
      super(ITDataplanePlugin.this);
    }
  }

  @Nested
  class NestedOptimizeTests extends OptimizeTestCases {
    NestedOptimizeTests() {
      super(ITDataplanePlugin.this);
    }
  }

  @Nested
  class NestedViewTests extends ViewTestCases {
    NestedViewTests() {
      super(ITDataplanePlugin.this);
    }
  }

  @Nested
  class NestedDeleteFolderTestCases extends DeleteFolderTestCases {
    NestedDeleteFolderTestCases() {
      super(ITDataplanePlugin.this);
    }
  }

  @Nested
  class NestedIDTests extends IdTestCases {
    NestedIDTests() {
      super(ITDataplanePlugin.this);
    }
  }

  @Nested
  class NestedCopyIntoTests extends CopyIntoTestCases {
    NestedCopyIntoTests() {
      super(ITDataplanePlugin.this, createTempLocation(), TEMP_SCHEMA_HADOOP);
    }
  }

  @Nested
  class NestedRollbackTests extends RollbackTestCases {
    NestedRollbackTests() {
      super(ITDataplanePlugin.this);
    }
  }
  @Nested
  class NestedVacuumTests extends VacuumTestCases {
    NestedVacuumTests() {
      super(ITDataplanePlugin.this);
    }
  }
}
