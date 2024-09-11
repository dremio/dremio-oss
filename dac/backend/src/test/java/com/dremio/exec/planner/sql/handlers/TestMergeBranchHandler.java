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
package com.dremio.exec.planner.sql.handlers;

import static com.dremio.exec.ExecConstants.ENABLE_MERGE_BRANCH_BEHAVIOR;
import static com.dremio.exec.ExecConstants.ENABLE_USE_VERSION_SYNTAX;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.Assert.assertFalse;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.projectnessie.model.MergeBehavior.DROP;
import static org.projectnessie.model.MergeBehavior.FORCE;
import static org.projectnessie.model.MergeBehavior.NORMAL;

import com.dremio.catalog.model.VersionContext;
import com.dremio.common.exceptions.UserException;
import com.dremio.exec.catalog.Catalog;
import com.dremio.exec.catalog.VersionedPlugin;
import com.dremio.exec.ops.QueryContext;
import com.dremio.exec.planner.sql.handlers.direct.MergeBranchResult;
import com.dremio.exec.planner.sql.parser.CompoundIdentifier;
import com.dremio.exec.planner.sql.parser.SqlMergeBranch;
import com.dremio.exec.store.NoDefaultBranchException;
import com.dremio.exec.store.ReferenceAlreadyExistsException;
import com.dremio.exec.store.ReferenceConflictException;
import com.dremio.exec.store.ReferenceNotFoundException;
import com.dremio.exec.work.foreman.ForemanSetupException;
import com.dremio.options.OptionManager;
import com.dremio.plugins.MergeBranchOptions;
import com.dremio.plugins.dataplane.store.DataplanePlugin;
import com.dremio.sabot.rpc.user.UserSession;
import com.dremio.service.namespace.NamespaceKey;
import com.dremio.test.DremioTest;
import java.util.Arrays;
import java.util.List;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;
import org.mockito.quality.Strictness;
import org.mockito.stubbing.Answer;
import org.projectnessie.model.MergeBehavior;
import org.projectnessie.model.MergeResponse;

/** Tests for ALTER BRANCH MERGE. */
public class TestMergeBranchHandler extends DremioTest {

  private static final String DEFAULT_SOURCE_NAME = "localnessie";
  private static final String TARGET_BRANCH = "targetbranch";
  private static final String SOURCE_BRANCH = "sourcebranch";
  private static final String DEFAULT_BRANCH_NAME = "branchName";
  private static final String DUMMY_HASH = "abcdef123";
  private static final String MAIN_VERSION_CONTEXT = "mainVersionContext";
  private static final VersionContext DEFAULT_VERSION =
      VersionContext.ofBranch(DEFAULT_BRANCH_NAME);
  @Rule public MockitoRule rule = MockitoJUnit.rule().strictness(Strictness.STRICT_STUBS);
  private QueryContext context;
  private OptionManager optionManager;
  private Catalog catalog;
  private UserSession userSession;
  private MergeBranchHandler handler;
  private DataplanePlugin dataplanePlugin;

  private MergeBranchHandler handlerVersionContext;
  private SqlMergeBranch mergeBranch;
  private SqlMergeBranch mergeBranchVersionContext;
  private SqlMergeBranch mergeBranchWithNoSource;
  private SqlMergeBranch mergeBranchWithNoTargetBranch;
  private SqlMergeBranch mergeBranchWithNoSourceAndTargetBranch;
  private DataplanePlugin dataplanePluginVersionContext;
  private MergeResponse mergeResponse;
  private MergeBranchOptions mergeBranchOptions;

  @Before
  public void setup() throws Exception {
    context = mock(QueryContext.class);
    optionManager = mock(OptionManager.class);
    catalog = mock(Catalog.class);
    userSession = mock(UserSession.class);
    dataplanePlugin = mock(DataplanePlugin.class);
    mergeResponse = mock(MergeResponse.class);

    when(context.getCatalog()).thenReturn(catalog);
    when(context.getOptions()).thenReturn(optionManager);
    when(context.getSession()).thenReturn(userSession);

    handler = new MergeBranchHandler(context);

    mergeBranch =
        new SqlMergeBranch(
            SqlParserPos.ZERO,
            SqlLiteral.createBoolean(false, SqlParserPos.ZERO),
            new SqlIdentifier(SOURCE_BRANCH, SqlParserPos.ZERO),
            new SqlIdentifier(TARGET_BRANCH, SqlParserPos.ZERO),
            new SqlIdentifier(DEFAULT_SOURCE_NAME, SqlParserPos.ZERO),
            null,
            null,
            null,
            null,
            null);

    mergeBranchWithNoSource =
        new SqlMergeBranch(
            SqlParserPos.ZERO,
            SqlLiteral.createBoolean(false, SqlParserPos.ZERO),
            new SqlIdentifier(SOURCE_BRANCH, SqlParserPos.ZERO),
            new SqlIdentifier(TARGET_BRANCH, SqlParserPos.ZERO),
            null,
            null,
            null,
            null,
            null,
            null);

    mergeBranchWithNoTargetBranch =
        new SqlMergeBranch(
            SqlParserPos.ZERO,
            SqlLiteral.createBoolean(false, SqlParserPos.ZERO),
            new SqlIdentifier(SOURCE_BRANCH, SqlParserPos.ZERO),
            null,
            new SqlIdentifier(DEFAULT_SOURCE_NAME, SqlParserPos.ZERO),
            null,
            null,
            null,
            null,
            null);

    mergeBranchWithNoSourceAndTargetBranch =
        new SqlMergeBranch(
            SqlParserPos.ZERO,
            SqlLiteral.createBoolean(false, SqlParserPos.ZERO),
            new SqlIdentifier(SOURCE_BRANCH, SqlParserPos.ZERO),
            null,
            null,
            null,
            null,
            null,
            null,
            null);
  }

  public void setUpDataplanePlugin() {
    when(optionManager.getOption(ENABLE_USE_VERSION_SYNTAX)).thenReturn(true);
    when(catalog.getSource(anyString())).thenReturn(dataplanePlugin);
    when(dataplanePlugin.isWrapperFor(VersionedPlugin.class)).thenReturn(true);
    when(dataplanePlugin.unwrap(VersionedPlugin.class)).thenReturn(dataplanePlugin);
    when(dataplanePlugin.isWrapperFor(VersionedPlugin.class)).thenReturn(true);
    when(userSession.getDefaultSchemaPath())
        .thenReturn(new NamespaceKey(Arrays.asList(DEFAULT_SOURCE_NAME, "unusedFolder")));
    when(userSession.getSessionVersionForSource(DEFAULT_SOURCE_NAME)).thenReturn(DEFAULT_VERSION);
  }

  public void setUpVersionContext() {
    QueryContext queryVersionContext = mock(QueryContext.class);
    OptionManager optionManagerVersionContext = mock(OptionManager.class);
    Catalog catalogVersionContext = mock(Catalog.class);
    UserSession userSessionVersionContext = mock(UserSession.class);
    dataplanePluginVersionContext = mock(DataplanePlugin.class);
    when(dataplanePluginVersionContext.isWrapperFor(VersionedPlugin.class)).thenReturn(true);
    when(dataplanePluginVersionContext.unwrap(VersionedPlugin.class))
        .thenReturn(dataplanePluginVersionContext);
    when(queryVersionContext.getCatalog()).thenReturn(catalogVersionContext);
    when(queryVersionContext.getOptions()).thenReturn(optionManagerVersionContext);
    when(queryVersionContext.getSession()).thenReturn(userSessionVersionContext);
    when(optionManagerVersionContext.getOption(ENABLE_USE_VERSION_SYNTAX)).thenReturn(true);
    when(catalogVersionContext.getSource(anyString())).thenReturn(dataplanePluginVersionContext);
    when(dataplanePluginVersionContext.isWrapperFor(VersionedPlugin.class)).thenReturn(true);
    when(userSessionVersionContext.getSessionVersionForSource(DEFAULT_SOURCE_NAME))
        .thenReturn(VersionContext.ofBranch(MAIN_VERSION_CONTEXT));
    handlerVersionContext = new MergeBranchHandler(queryVersionContext);
    mergeBranchVersionContext =
        new SqlMergeBranch(
            SqlParserPos.ZERO,
            SqlLiteral.createBoolean(false, SqlParserPos.ZERO),
            new SqlIdentifier(SOURCE_BRANCH, SqlParserPos.ZERO),
            null,
            new SqlIdentifier(DEFAULT_SOURCE_NAME, SqlParserPos.ZERO),
            null,
            null,
            null,
            null,
            null);
  }

  @Test
  public void mergeBranchSucceed() throws Exception {
    when(dataplanePlugin.mergeBranch(anyString(), anyString(), any(MergeBranchOptions.class)))
        .thenReturn(mergeResponse);
    when(mergeResponse.wasSuccessful()).thenReturn(true);
    when(mergeResponse.getExpectedHash()).thenReturn(DUMMY_HASH);
    setUpDataplanePlugin();
    List<MergeBranchResult> result = handler.toResult("", mergeBranch);
    assertFalse(result.isEmpty());
    assertThat(result.get(0).message)
        .contains(
            String.format(
                "%s has been merged into %s at %s",
                mergeBranch.getSourceBranchName(), mergeBranch.getTargetBranchName(), DUMMY_HASH));
  }

  @Test
  public void mergeBranchSucceedForVersionContext()
      throws ForemanSetupException,
          ReferenceAlreadyExistsException,
          ReferenceNotFoundException,
          NoDefaultBranchException,
          ReferenceConflictException {
    setUpVersionContext();
    when(dataplanePluginVersionContext.mergeBranch(
            anyString(), anyString(), any(MergeBranchOptions.class)))
        .thenReturn(mergeResponse);
    when(mergeResponse.wasSuccessful()).thenReturn(true);
    when(mergeResponse.getExpectedHash()).thenReturn(DUMMY_HASH);
    List<MergeBranchResult> result = handlerVersionContext.toResult("", mergeBranchVersionContext);
    assertFalse(result.isEmpty());
    assertThat(result.get(0).message)
        .contains(
            String.format(
                "%s has been merged into %s at %s",
                mergeBranchVersionContext.getSourceBranchName(), MAIN_VERSION_CONTEXT, DUMMY_HASH));
  }

  @Test
  public void mergeBranchThrowUnsupport() {
    when(optionManager.getOption(ENABLE_USE_VERSION_SYNTAX)).thenReturn(false);

    assertThatThrownBy(() -> handler.toResult("", mergeBranch)).isInstanceOf(UserException.class);
  }

  @Test
  public void createBranchThrowNotFoundForVersionContext() throws Exception {
    setUpVersionContext();

    doThrow(ReferenceNotFoundException.class)
        .when(dataplanePluginVersionContext)
        .mergeBranch(anyString(), any(), any(MergeBranchOptions.class));

    List<MergeBranchResult> result = handlerVersionContext.toResult("", mergeBranchVersionContext);
    assertFalse(result.isEmpty());
    assertThat(result.get(0).message)
        .contains(
            String.format(
                "Failed to merge branch %s into %s",
                mergeBranchVersionContext.getSourceBranchName(), MAIN_VERSION_CONTEXT));
  }

  @Test
  public void mergeBranchThrowWrongPlugin() {
    assertThatThrownBy(() -> handler.toResult("", mergeBranchWithNoSource))
        .isInstanceOf(UserException.class);
  }

  @Test
  public void mergeBranchThrowNotFound() throws Exception {
    setUpDataplanePlugin();

    doThrow(ReferenceNotFoundException.class)
        .when(dataplanePlugin)
        .mergeBranch(anyString(), anyString(), any(MergeBranchOptions.class));

    List<MergeBranchResult> result = handler.toResult("", mergeBranch);
    assertFalse(result.isEmpty());
    assertThat(result.get(0).message)
        .contains(
            String.format(
                "Failed to merge branch %s into %s",
                mergeBranchWithNoSource.getSourceBranchName(),
                mergeBranchWithNoSource.getTargetBranchName()));
  }

  @Test
  public void mergeBranchNoDefaultBranch()
      throws ReferenceConflictException, ReferenceNotFoundException {

    // TODO: Is this var needed?
    final SqlMergeBranch mergeBranch =
        new SqlMergeBranch(
            SqlParserPos.ZERO,
            SqlLiteral.createBoolean(false, SqlParserPos.ZERO),
            new SqlIdentifier(SOURCE_BRANCH, SqlParserPos.ZERO),
            null,
            new SqlIdentifier(DEFAULT_SOURCE_NAME, SqlParserPos.ZERO),
            null,
            null,
            null,
            null,
            null);

    assertThatThrownBy(() -> handler.toResult("", mergeBranch)).isInstanceOf(UserException.class);
  }

  @Test
  public void mergeBranchNoTargetBranch() throws Exception {
    setUpDataplanePlugin();
    // Arrange
    when(dataplanePlugin.mergeBranch(anyString(), anyString(), any(MergeBranchOptions.class)))
        .thenReturn(mergeResponse);
    when(mergeResponse.wasSuccessful()).thenReturn(true);
    when(mergeResponse.getExpectedHash()).thenReturn(DUMMY_HASH);
    // Act
    List<MergeBranchResult> result = handler.toResult("", mergeBranchWithNoTargetBranch);

    // Assert
    assertThat(result).isNotEmpty();
    assertThat(result.get(0).message)
        .contains(
            String.format(
                "%s has been merged into %s at %s",
                mergeBranchWithNoTargetBranch.getSourceBranchName(),
                DEFAULT_VERSION.getValue(),
                DUMMY_HASH));
  }

  @Test
  public void mergeBranchEmptySourceUsesSessionContext() throws Exception {
    setUpDataplanePlugin();
    // Arrange
    when(dataplanePlugin.mergeBranch(anyString(), anyString(), any(MergeBranchOptions.class)))
        .thenReturn(mergeResponse);
    when(mergeResponse.wasSuccessful()).thenReturn(true);
    when(mergeResponse.getExpectedHash()).thenReturn(DUMMY_HASH);
    // Act
    List<MergeBranchResult> result = handler.toResult("", mergeBranchWithNoSource);

    // Assert
    assertThat(result).isNotEmpty();
    assertThat(result.get(0).message)
        .contains(
            String.format(
                "%s has been merged into %s at %s",
                mergeBranchWithNoSource.getSourceBranchName(),
                mergeBranchWithNoSource.getTargetBranchName(),
                DUMMY_HASH));
  }

  @Test
  public void mergeBranchWithNoTargetBranchAndUseSessionContext() throws Exception {
    setUpDataplanePlugin();
    // Arrange
    when(dataplanePlugin.mergeBranch(anyString(), anyString(), any(MergeBranchOptions.class)))
        .thenReturn(mergeResponse);
    when(mergeResponse.wasSuccessful()).thenReturn(true);
    when(mergeResponse.getExpectedHash()).thenReturn(DUMMY_HASH);

    // Act
    List<MergeBranchResult> result = handler.toResult("", mergeBranchWithNoSourceAndTargetBranch);

    // Assert
    assertThat(result).isNotEmpty();
    assertThat(result.get(0).message)
        .contains(
            String.format(
                "%s has been merged into %s at %s",
                mergeBranchWithNoTargetBranch.getSourceBranchName(),
                DEFAULT_VERSION.getValue(),
                DUMMY_HASH));
  }

  @Test
  public void mergeBranchWithDryRunMergeOption() throws Exception {
    setUpDataplanePlugin();
    final SqlMergeBranch mergeBranch =
        new SqlMergeBranch(
            SqlParserPos.ZERO,
            SqlLiteral.createBoolean(true, SqlParserPos.ZERO),
            new SqlIdentifier(DEFAULT_BRANCH_NAME, SqlParserPos.ZERO),
            new SqlIdentifier(TARGET_BRANCH, SqlParserPos.ZERO),
            new SqlIdentifier(DEFAULT_SOURCE_NAME, SqlParserPos.ZERO),
            null,
            null,
            null,
            null,
            null);
    when(dataplanePlugin.mergeBranch(anyString(), anyString(), any(MergeBranchOptions.class)))
        .thenReturn(mergeResponse);
    when(mergeResponse.wasSuccessful()).thenReturn(true);
    when(mergeResponse.getExpectedHash()).thenReturn(DUMMY_HASH);

    List<MergeBranchResult> result = handler.toResult("", mergeBranch);
    assertFalse(result.isEmpty());
    assertThat(result.get(0).message).contains("can be merged");
  }

  @Test
  public void mergeBranchWithDefaultMergeBehaviorOption()
      throws ForemanSetupException, ReferenceConflictException, ReferenceNotFoundException {
    setUpDataplanePlugin();
    when(optionManager.getOption(ENABLE_MERGE_BRANCH_BEHAVIOR)).thenReturn(true);
    when(mergeResponse.getExpectedHash()).thenReturn(DUMMY_HASH);
    final MergeBehavior expectedMergeBehavior = NORMAL;
    final SqlMergeBranch mergeBranch =
        new SqlMergeBranch(
            SqlParserPos.ZERO,
            SqlLiteral.createBoolean(false, SqlParserPos.ZERO),
            new SqlIdentifier(DEFAULT_BRANCH_NAME, SqlParserPos.ZERO),
            new SqlIdentifier(TARGET_BRANCH, SqlParserPos.ZERO),
            new SqlIdentifier(DEFAULT_SOURCE_NAME, SqlParserPos.ZERO),
            expectedMergeBehavior,
            null,
            null,
            null,
            null);

    doAnswer(
            new Answer<MergeResponse>() {
              @Override
              public MergeResponse answer(InvocationOnMock invocation) throws Throwable {
                assertThat(((MergeBranchOptions) invocation.getArgument(2)).defaultMergeBehavior())
                    .isEqualTo(expectedMergeBehavior);
                return mergeResponse;
              }
            })
        .when(dataplanePlugin)
        .mergeBranch(any(), any(), any());

    when(mergeResponse.wasSuccessful()).thenReturn(true);

    List<MergeBranchResult> result = handler.toResult("", mergeBranch);
    assertFalse(result.isEmpty());
    assertThat(result.get(0).message).contains("has been merged");
  }

  @Test
  public void mergeBranchOnConflictThrowNotSupported() {
    setUpDataplanePlugin();
    when(optionManager.getOption(ENABLE_MERGE_BRANCH_BEHAVIOR)).thenReturn(false);

    final SqlMergeBranch mergeBranch =
        new SqlMergeBranch(
            SqlParserPos.ZERO,
            SqlLiteral.createBoolean(false, SqlParserPos.ZERO),
            new SqlIdentifier(DEFAULT_BRANCH_NAME, SqlParserPos.ZERO),
            new SqlIdentifier(TARGET_BRANCH, SqlParserPos.ZERO),
            new SqlIdentifier(DEFAULT_SOURCE_NAME, SqlParserPos.ZERO),
            NORMAL,
            null,
            null,
            null,
            null);

    assertThatThrownBy(() -> handler.toResult("", mergeBranch)).isInstanceOf(UserException.class);
  }

  @Test
  public void testValidateExceptMergeBehaviorsAreUnique()
      throws ForemanSetupException, ReferenceConflictException, ReferenceNotFoundException {
    setUpDataplanePlugin();
    when(optionManager.getOption(ENABLE_MERGE_BRANCH_BEHAVIOR)).thenReturn(true);
    final SqlNodeList fakeList = new SqlNodeList(SqlParserPos.ZERO);

    assertMergeBranchSucceeds(
        generateSqlMergeBranchByMergeOptions(
            false,
            SOURCE_BRANCH,
            TARGET_BRANCH,
            DEFAULT_SOURCE_NAME,
            null,
            null,
            null,
            null,
            null));

    assertMergeBranchSucceeds(
        generateSqlMergeBranchByMergeOptions(
            false,
            SOURCE_BRANCH,
            TARGET_BRANCH,
            DEFAULT_SOURCE_NAME,
            NORMAL,
            null,
            null,
            null,
            null));
    assertMergeBranchSucceeds(
        generateSqlMergeBranchByMergeOptions(
            false,
            SOURCE_BRANCH,
            TARGET_BRANCH,
            DEFAULT_SOURCE_NAME,
            NORMAL,
            FORCE,
            null,
            fakeList,
            null));
    assertMergeBranchSucceeds(
        generateSqlMergeBranchByMergeOptions(
            false,
            SOURCE_BRANCH,
            TARGET_BRANCH,
            DEFAULT_SOURCE_NAME,
            NORMAL,
            FORCE,
            DROP,
            fakeList,
            fakeList));
    assertMergeBranchThrowsExpectedErrorAndErrorMessage(
        generateSqlMergeBranchByMergeOptions(
            false,
            SOURCE_BRANCH,
            TARGET_BRANCH,
            DEFAULT_SOURCE_NAME,
            NORMAL,
            NORMAL,
            null,
            null,
            null),
        UserException.class,
        "must be distinct");
    assertMergeBranchThrowsExpectedErrorAndErrorMessage(
        generateSqlMergeBranchByMergeOptions(
            false,
            SOURCE_BRANCH,
            TARGET_BRANCH,
            DEFAULT_SOURCE_NAME,
            NORMAL,
            NORMAL,
            NORMAL,
            null,
            null),
        UserException.class,
        "must be distinct");
  }

  @Test
  public void testInvalidContentKey()
      throws ReferenceConflictException, ReferenceNotFoundException {
    setUpDataplanePlugin();
    when(optionManager.getOption(ENABLE_MERGE_BRANCH_BEHAVIOR)).thenReturn(true);
    final SqlNodeList list = new SqlNodeList(SqlParserPos.ZERO);
    CompoundIdentifier.Builder builder = CompoundIdentifier.newBuilder();
    builder.addString("wrongsource", false, SqlParserPos.ZERO);
    builder.addString("folder", false, SqlParserPos.ZERO);
    builder.addString("key", false, SqlParserPos.ZERO);
    list.add(builder.build());

    assertMergeBranchThrowsExpectedErrorAndErrorMessage(
        generateSqlMergeBranchByMergeOptions(
            false,
            SOURCE_BRANCH,
            TARGET_BRANCH,
            DEFAULT_SOURCE_NAME,
            NORMAL,
            DROP,
            null,
            list,
            null),
        UserException.class,
        String.format("Content must be from source [%s]", DEFAULT_SOURCE_NAME));
  }

  @Test
  public void testDuplicateContentKey()
      throws ReferenceConflictException, ReferenceNotFoundException {
    setUpDataplanePlugin();
    when(optionManager.getOption(ENABLE_MERGE_BRANCH_BEHAVIOR)).thenReturn(true);
    final SqlNodeList list = new SqlNodeList(SqlParserPos.ZERO);
    CompoundIdentifier.Builder builder = CompoundIdentifier.newBuilder();
    builder.addString(DEFAULT_SOURCE_NAME, false, SqlParserPos.ZERO);
    builder.addString("folder", false, SqlParserPos.ZERO);
    builder.addString("key", false, SqlParserPos.ZERO);
    list.add(builder.build());

    assertMergeBranchThrowsExpectedErrorAndErrorMessage(
        generateSqlMergeBranchByMergeOptions(
            false,
            SOURCE_BRANCH,
            TARGET_BRANCH,
            DEFAULT_SOURCE_NAME,
            NORMAL,
            DROP,
            FORCE,
            list,
            list),
        UserException.class,
        "Each key can only be used once in an EXCEPT clause.");
  }

  private void assertMergeBranchSucceeds(SqlMergeBranch mergeBranch) throws ForemanSetupException {
    assertMergeBranchSucceeds(mergeBranch, DUMMY_HASH);
  }

  private void assertMergeBranchSucceeds(SqlMergeBranch mergeBranch, String hash)
      throws ForemanSetupException {
    when(dataplanePlugin.mergeBranch(anyString(), anyString(), any(MergeBranchOptions.class)))
        .thenReturn(mergeResponse);
    when(mergeResponse.wasSuccessful()).thenReturn(true);
    when(mergeResponse.getExpectedHash()).thenReturn(hash);
    List<MergeBranchResult> result = handler.toResult("", mergeBranch);
    assertFalse(result.isEmpty());
    assertThat(result.get(0).message)
        .contains(
            String.format(
                "%s has been merged into %s at %s",
                mergeBranch.getSourceBranchName(), mergeBranch.getTargetBranchName(), hash));
  }

  @Test
  public void assertMergeBranchSucceedsWhenHashIsEmpty()
      throws ReferenceConflictException, ReferenceNotFoundException, ForemanSetupException {
    setUpDataplanePlugin();
    when(mergeResponse.wasSuccessful()).thenReturn(true);

    assertMergeBranchSucceeds(
        generateSqlMergeBranchByMergeOptions(
            false, SOURCE_BRANCH, TARGET_BRANCH, DEFAULT_SOURCE_NAME, null, null, null, null, null),
        "[unknown hash]");
  }

  private void assertMergeBranchThrowsExpectedErrorAndErrorMessage(
      SqlMergeBranch mergeBranch,
      Class<? extends Exception> exception,
      String expectedErrorMessage) {
    assertThatThrownBy(() -> handler.toResult("", mergeBranch))
        .isInstanceOf(exception)
        .hasMessageContaining(expectedErrorMessage);
  }

  private SqlMergeBranch generateSqlMergeBranchByMergeOptions(
      boolean dryRun,
      String sourceBranch,
      String targetBranch,
      String sourceName,
      MergeBehavior defaultMergeBehavior,
      MergeBehavior mergeBehavior1,
      MergeBehavior mergeBehavior2,
      SqlNodeList exceptContentList1,
      SqlNodeList exceptContentList2) {
    return new SqlMergeBranch(
        SqlParserPos.ZERO,
        SqlLiteral.createBoolean(dryRun, SqlParserPos.ZERO),
        new SqlIdentifier(sourceBranch, SqlParserPos.ZERO),
        new SqlIdentifier(targetBranch, SqlParserPos.ZERO),
        new SqlIdentifier(sourceName, SqlParserPos.ZERO),
        defaultMergeBehavior,
        mergeBehavior1,
        mergeBehavior2,
        exceptContentList1,
        exceptContentList2);
  }
}
