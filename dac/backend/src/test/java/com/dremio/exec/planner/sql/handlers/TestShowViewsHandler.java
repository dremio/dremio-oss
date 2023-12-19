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

import static com.dremio.exec.ExecConstants.ENABLE_USE_VERSION_SYNTAX;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.AssertionsForClassTypes.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.stream.Stream;

import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.assertj.core.api.Assertions;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;
import org.mockito.quality.Strictness;

import com.dremio.catalog.model.VersionContext;
import com.dremio.common.exceptions.UserException;
import com.dremio.exec.catalog.Catalog;
import com.dremio.exec.planner.sql.handlers.direct.ShowViewsHandler;
import com.dremio.exec.planner.sql.parser.ReferenceType;
import com.dremio.exec.planner.sql.parser.SqlShowViews;
import com.dremio.exec.store.ReferenceConflictException;
import com.dremio.exec.store.ReferenceNotFoundException;
import com.dremio.exec.store.StoragePlugin;
import com.dremio.options.OptionManager;
import com.dremio.plugins.ExternalNamespaceEntry;
import com.dremio.plugins.ExternalNamespaceEntry.Type;
import com.dremio.plugins.dataplane.store.DataplanePlugin;
import com.dremio.sabot.rpc.user.UserSession;
import com.dremio.service.namespace.NamespaceKey;
import com.google.common.collect.ImmutableList;

public class TestShowViewsHandler {
  private static final String STATEMENT_SOURCE_NAME = "source_name";
  private static final String SESSION_SOURCE_NAME = "session_source_name";
  private static final String NON_EXISTENT_SOURCE_NAME = "non_exist";

  private static final String STATEMENT_REF_NAME = "statement_ref_name";
  private static final String STATEMENT_BRANCH_NAME = "statement_branch_name";
  private static final String STATEMENT_TAG_NAME = "statement_tag_name";
  private static final String STATEMENT_COMMIT_HASH = "DEADBEEFDEADBEEF";
  private static final String SESSION_BRANCH_NAME = "session_branch_name";

  @Rule
  public MockitoRule rule = MockitoJUnit.rule().strictness(Strictness.STRICT_STUBS);

  @Mock
  private OptionManager optionManager;
  @Mock private Catalog catalog;
  @Mock private UserSession userSession;
  @Mock private DataplanePlugin dataplanePlugin;

  @InjectMocks
  private ShowViewsHandler showViewsHandler;

  @Test // SHOW VIEWS
  public void showViewsSupportKeyDisabledThrows() {
    // Arrange
    // Note that it gets session source first to determine whether source is versioned or not.
    setUpSessionSource();
    when(optionManager.getOption(ENABLE_USE_VERSION_SYNTAX))
      .thenReturn(false);

    SqlShowViews input = TestShowViewsHandler.SqlShowViewsBuilder.builder().build();

    // Act and Assert
    assertThatThrownBy(() -> showViewsHandler.toResult("", input))
      .isInstanceOf(UserException.class)
      .hasMessageContaining("SHOW VIEWS")
      .hasMessageContaining("not supported");
  }

  @Test
  public void showViewsNonExistentSource() {
    // Arrange
    SqlShowViews input = TestShowViewsHandler.SqlShowViewsBuilder.builder()
      .withReference(ReferenceType.REFERENCE, STATEMENT_REF_NAME)
      .withSource(NON_EXISTENT_SOURCE_NAME)
      .build();

    when(userSession.getDefaultSchemaPath())
      .thenReturn(new NamespaceKey(Arrays.asList(SESSION_SOURCE_NAME, "unusedFolder")));

    // Assert + Act
    Assertions.assertThatThrownBy(() -> showViewsHandler.toResult("", input))
      .isInstanceOf(UserException.class)
      .hasMessageContaining("does not exist");
  }

  @Test //SHOW VIEWS
  public void showViewsReturnsViewResult() throws Exception {

    // Arrange
    setUpSessionSource();
    setUpSessionSourceVersion();
    setUpSupportKeys();

    ExternalNamespaceEntry viewEntry1 = createRandomViewEntry();
    ExternalNamespaceEntry viewEntry2 = createRandomViewEntry();

    when(dataplanePlugin.listViewsIncludeNested(
      Collections.emptyList(),
      VersionContext.ofBranch(SESSION_BRANCH_NAME)))
      .thenReturn(Stream.of(viewEntry1, viewEntry2));

    SqlShowViews input = TestShowViewsHandler.SqlShowViewsBuilder.builder().build();

    // Act
 List<ShowViewsHandler.ShowViewResult> result = showViewsHandler.toResult("", input);

    // Assert
    List<ShowViewsHandler.ShowViewResult> expectedViewInfoList = createExpectedShowViewResultList(SESSION_SOURCE_NAME, viewEntry1, viewEntry2);
    assertThat(result).hasSize(2);
    assertThat(result).isEqualTo(expectedViewInfoList);
  }

  @Test // SHOW VIEWS (Nested)
  public void showViewsNestedSessionSourceReturnsViewsInfo() throws Exception {
    // Arrange
    final String nestedFolder = "NestedFolder";
    setUpSessionSource(nestedFolder);
    setUpSessionSourceVersion();
    setUpSupportKeys();

    ExternalNamespaceEntry viewEntry = createRandomViewEntry();

    when(dataplanePlugin.listViewsIncludeNested(
      ImmutableList.of(nestedFolder),
      VersionContext.ofBranch(SESSION_BRANCH_NAME)))
      .thenReturn(Stream.of(viewEntry));

    SqlShowViews input = TestShowViewsHandler.SqlShowViewsBuilder.builder().build();

    // Act
    List<ShowViewsHandler.ShowViewResult> result = showViewsHandler.toResult("", input);

    // Assert
    ShowViewsHandler.ShowViewResult  expectedViewInfo = createExpectedShowViewResult(SESSION_SOURCE_NAME, viewEntry);
    assertThat(result).hasSize(1);
    assertThat(result.get(0)).isEqualTo(expectedViewInfo);
  }

  @Test // SHOW VIEWS in <source with nested folder>
  public void showViewsNestedSourceReturnsViewsInfo() throws Exception {
    // Arrange
    final String nestedFolder = "NestedFolder";
    setUpStatementSource(nestedFolder);
    setUpStatementSourceVersion();
    setUpSupportKeys();

    ExternalNamespaceEntry viewEntry = createRandomViewEntry();

    when(dataplanePlugin.listViewsIncludeNested(
      ImmutableList.of(nestedFolder),
      VersionContext.ofBranch(SESSION_BRANCH_NAME)))
      .thenReturn(Stream.of(viewEntry));

    SqlShowViews input = TestShowViewsHandler.SqlShowViewsBuilder.builder().build();

    // Act
    List<ShowViewsHandler.ShowViewResult> result = showViewsHandler.toResult("", input);

    // Assert
    ShowViewsHandler.ShowViewResult expectedViewInfo = createExpectedShowViewResult(STATEMENT_SOURCE_NAME, viewEntry);
    assertThat(result).hasSize(1);
    assertThat(result.get(0)).isEqualTo(expectedViewInfo);
  }

  @Test
  public void showViewsReturnsMatchingResult() throws Exception {

    // Arrange
    setUpSessionSource();
    setUpSessionSourceVersion();
    setUpSupportKeys();

    ExternalNamespaceEntry viewEntry1 = createRandomViewEntryWithSpecificPrefix("abc");
    ExternalNamespaceEntry viewEntry2 = createRandomViewEntryWithSpecificPrefix("xyz");

    when(dataplanePlugin.listViewsIncludeNested(
      Collections.emptyList(),
      VersionContext.ofBranch(SESSION_BRANCH_NAME)))
      .thenReturn(Stream.of(viewEntry1, viewEntry2));

    SqlShowViews input = TestShowViewsHandler.SqlShowViewsBuilder.builder()
      .withLike("%ab%")
      .build();

    // Act
    List<ShowViewsHandler.ShowViewResult> result = showViewsHandler.toResult("", input);

    // Assert
    List<ShowViewsHandler.ShowViewResult> expectedTableInfoList = createExpectedShowViewResultList(SESSION_SOURCE_NAME, viewEntry1);
    assertThat(result).hasSize(1);
    assertThat(result).isEqualTo(expectedTableInfoList);
  }


  @Test // SHOW VIEWS AT BRANCH<branch> IN <source>
  public void showViewsBranchReturnsViewInfo() throws Exception {
    // Arrange
    setUpStatementSource();
    setUpStatementSourceVersion();
    setUpSupportKeys();

    ExternalNamespaceEntry viewEntry = createRandomViewEntry();

    when(dataplanePlugin.listViewsIncludeNested(
      Collections.emptyList(),
      VersionContext.ofBranch(STATEMENT_BRANCH_NAME)))
      .thenReturn(Stream.of(viewEntry));

    SqlShowViews input = TestShowViewsHandler.SqlShowViewsBuilder.builder()
      .withReference(ReferenceType.BRANCH, STATEMENT_BRANCH_NAME)
      .withSource(STATEMENT_SOURCE_NAME)
      .build();

    // Act
    List<ShowViewsHandler.ShowViewResult> result = showViewsHandler.toResult("", input);

    // Assert
    ShowViewsHandler.ShowViewResult expectedViewInfo = createExpectedShowViewResult(STATEMENT_SOURCE_NAME, viewEntry);
    assertThat(result).hasSize(1);
    assertThat(result.get(0)).isEqualTo(expectedViewInfo);
  }

  @Test // SHOW VIEWS AT TAG <tag> IN <source>
  public void showViewsTagReturnsViewInfo() throws Exception {
    // Arrange
    setUpStatementSource();
    setUpStatementSourceVersion();
    setUpSupportKeys();

    ExternalNamespaceEntry viewEntry = createRandomViewEntry();

    when(dataplanePlugin.listViewsIncludeNested(
      Collections.emptyList(),
      VersionContext.ofTag(STATEMENT_TAG_NAME)))
      .thenReturn(Stream.of(viewEntry));

    SqlShowViews input = TestShowViewsHandler.SqlShowViewsBuilder.builder()
      .withReference(ReferenceType.TAG, STATEMENT_TAG_NAME)
      .withSource(STATEMENT_SOURCE_NAME)
      .build();

    // Act
    List<ShowViewsHandler.ShowViewResult> result = showViewsHandler.toResult("", input);

    // Assert
    ShowViewsHandler.ShowViewResult expectedViewInfo = createExpectedShowViewResult(STATEMENT_SOURCE_NAME, viewEntry);
    assertThat(result).hasSize(1);
    assertThat(result.get(0)).isEqualTo(expectedViewInfo);
  }

  @Test // SHOW VIEWS AT REF <arbitrary ref>
  public void showViewsRefSessionSourceNonexistentThrows() {
    // Arrange
    when(catalog.containerExists(new NamespaceKey(SESSION_SOURCE_NAME)))
      .thenReturn(false);
    when(userSession.getDefaultSchemaPath())
      .thenReturn(new NamespaceKey(ImmutableList.of((SESSION_SOURCE_NAME))));

    SqlShowViews input = TestShowViewsHandler.SqlShowViewsBuilder.builder()
      .withReference(ReferenceType.REFERENCE, STATEMENT_REF_NAME)
      .build();

    // Act and Assert
    assertThatThrownBy(() -> showViewsHandler.toResult("", input))
      .isInstanceOf(UserException.class)
      .hasMessageContaining(SESSION_SOURCE_NAME)
      .hasMessageContaining("does not exist");
  }

  @Test // SHOW VIEWS AT REF <ref> (using non-nessie source)
  public void showViewsRefSessionSourceNonNessieThrows() {
    // Arrange
    when(catalog.containerExists(new NamespaceKey(SESSION_SOURCE_NAME)))
      .thenReturn(true);
    when(catalog.getSource(SESSION_SOURCE_NAME))
      .thenReturn(mock(StoragePlugin.class)); // Non-nessie source
    when(userSession.getDefaultSchemaPath())
      .thenReturn(new NamespaceKey(ImmutableList.of(SESSION_SOURCE_NAME, "unusedFolder")));

    SqlShowViews input = TestShowViewsHandler.SqlShowViewsBuilder.builder()
      .withReference(ReferenceType.REFERENCE, STATEMENT_REF_NAME)
      .build();

    // Act and Assert
    assertThatThrownBy(() -> showViewsHandler.toResult("", input))
      .isInstanceOf(UserException.class)
      .hasMessageContaining("does not support show views");
  }

  @Test // SHOW VIEWS AT REF <non-existent ref> IN <arbitrary source>
  public void showViewsReferenceNotFoundThrows() {
    // Arrange
    setUpStatementSource();
    setUpStatementSourceVersion();
    setUpSupportKeys();

    when(dataplanePlugin.listViewsIncludeNested(
      Collections.emptyList(),
      VersionContext.ofRef(STATEMENT_REF_NAME)))
      .thenThrow(ReferenceNotFoundException.class);

    SqlShowViews input = TestShowViewsHandler.SqlShowViewsBuilder.builder()
      .withReference(ReferenceType.REFERENCE, STATEMENT_REF_NAME)
      .withSource(STATEMENT_SOURCE_NAME)
      .build();

    // Act and Assert
    assertThatThrownBy(() -> showViewsHandler.toResult("", input))
      .isInstanceOf(UserException.class)
      .hasMessageContaining("not found")
      .hasMessageContaining(STATEMENT_SOURCE_NAME)
      .hasMessageContaining(STATEMENT_REF_NAME);
  }

  @Test // SHOW VIEWS AT REF <ref conflict> IN <arbitrary source>
  public void showViewsReferenceConflictThrows() {
    // Arrange
    setUpStatementSource();
    setUpStatementSourceVersion();
    setUpSupportKeys();

    when(dataplanePlugin.listViewsIncludeNested(
      Collections.emptyList(),
      VersionContext.ofRef(STATEMENT_REF_NAME)))
      .thenThrow(ReferenceConflictException.class);

    SqlShowViews input = TestShowViewsHandler.SqlShowViewsBuilder.builder()
      .withReference(ReferenceType.REFERENCE, STATEMENT_REF_NAME)
      .withSource(STATEMENT_SOURCE_NAME)
      .build();

    // Act and Assert
    assertThatThrownBy(() -> showViewsHandler.toResult("", input))
      .isInstanceOf(UserException.class)
      .hasMessageContaining("has conflict")
      .hasMessageContaining(STATEMENT_SOURCE_NAME)
      .hasMessageContaining(STATEMENT_REF_NAME);
  }

  // Sets up to return the Session Source Version in user session (which shouldn't get
  // used if the statement has a source).
  private void setUpStatementSourceVersion()
  {
    VersionContext sessionVersionContext = VersionContext.ofBranch(SESSION_BRANCH_NAME);
    when(userSession.getSessionVersionForSource(STATEMENT_SOURCE_NAME))
      .thenReturn(sessionVersionContext);
  }

  // Sets up to return the Session Source Version in user session (which gets used if
  // "AT <refType> <refValue>" is not specified in query).
  private void setUpSessionSourceVersion()
  {
    VersionContext sessionVersionContext = VersionContext.ofBranch(SESSION_BRANCH_NAME);
    when(userSession.getSessionVersionForSource(SESSION_SOURCE_NAME))
      .thenReturn(sessionVersionContext);
  }

  // Sets up to return the Statement Source in catalog (which gets used if
  // "IN <source>" is specified in query).
  private void setUpStatementSource()
  {
    setUpStatementSource(null);
  }

  private void setUpStatementSource(String nestedFolder)
  {
    when(catalog.containerExists(new NamespaceKey(STATEMENT_SOURCE_NAME)))
      .thenReturn(true);
    when(catalog.getSource(STATEMENT_SOURCE_NAME))
      .thenReturn(dataplanePlugin);
    List<String> path = nestedFolder != null ?
      ImmutableList.of(STATEMENT_SOURCE_NAME, nestedFolder) :
      ImmutableList.of(STATEMENT_SOURCE_NAME);
    when(userSession.getDefaultSchemaPath())
      .thenReturn(new NamespaceKey(path));

  }

  // Sets up to enable SQL syntax in option manager.
  private void setUpSupportKeys()
  {
    when(optionManager.getOption(ENABLE_USE_VERSION_SYNTAX))
      .thenReturn(true);
  }

  // Sets up to return the Session Source in catalog (which gets used if
  // "IN <source>" is not specified in query).
  private void setUpSessionSource()
  {
    setUpSessionSource(null);
  }

  private void setUpSessionSource(String nestedFolder)
  {
    when(catalog.containerExists(new NamespaceKey(SESSION_SOURCE_NAME)))
      .thenReturn(true);
    when(catalog.getSource(SESSION_SOURCE_NAME))
      .thenReturn(dataplanePlugin);
    List<String> path = nestedFolder!= null ?
      ImmutableList.of(SESSION_SOURCE_NAME, nestedFolder) :
      ImmutableList.of(SESSION_SOURCE_NAME);
    when(userSession.getDefaultSchemaPath())
      .thenReturn(new NamespaceKey(path));
  }

  private ExternalNamespaceEntry createRandomViewEntry()
  {
    String randomName = UUID.randomUUID().toString();
    return ExternalNamespaceEntry.of(Type.ICEBERG_VIEW, ImmutableList.of(randomName));
  }

  private ExternalNamespaceEntry createRandomViewEntryWithSpecificPrefix(String prefix)
  {
    String randomName = prefix + UUID.randomUUID();
    return ExternalNamespaceEntry.of(Type.ICEBERG_VIEW, ImmutableList.of(randomName));
  }

  private ShowViewsHandler.ShowViewResult createExpectedShowViewResult(String sourceName, ExternalNamespaceEntry viewEntry)
  {
    String sourceNameAndNamespace = sourceName;
    if(!viewEntry.getNamespace().isEmpty())
    {
      sourceNameAndNamespace = String.join(".", sourceName, String.join(".", viewEntry.getNamespace()));
    }

    return new ShowViewsHandler.ShowViewResult(
      sourceNameAndNamespace,
      viewEntry.getName());
  }

  private List<ShowViewsHandler.ShowViewResult> createExpectedShowViewResultList(String sourceName, ExternalNamespaceEntry... viewEntries) {
    String sourceNameAndNamespace = sourceName;
    List<ShowViewsHandler.ShowViewResult> returnList = new ArrayList<>();
    for (ExternalNamespaceEntry viewEntry : viewEntries) {
      if (!viewEntry.getNamespace().isEmpty()) {
        sourceNameAndNamespace = String.join(".", sourceName, String.join(".", viewEntry.getNamespace()));
      }
      returnList.add(new ShowViewsHandler.ShowViewResult(sourceNameAndNamespace, viewEntry.getName()));
    }
    return returnList;
  }



  static class SqlShowViewsBuilder {
    private String sourceName;
    private ReferenceType referenceType;
    private String reference;
    private String likePattern;

    static TestShowViewsHandler.SqlShowViewsBuilder builder() {
      return new TestShowViewsHandler.SqlShowViewsBuilder();
    }

    TestShowViewsHandler.SqlShowViewsBuilder withSource(String sourceName) {
      this.sourceName = sourceName;
      return this;
    }

    TestShowViewsHandler.SqlShowViewsBuilder withReference(ReferenceType referenceType, String reference) {
      this.referenceType = referenceType;
      this.reference = reference;
      return this;
    }

    TestShowViewsHandler.SqlShowViewsBuilder withLike(String likePattern) {
      this.likePattern = likePattern;
      return this;
    }

    // SHOW VIEWS [AT <refType> <refValue>] [IN <sourceName>]
    SqlShowViews build()
    {
      return new SqlShowViews(
        SqlParserPos.ZERO,
        referenceType,
        reference != null ? new SqlIdentifier(reference, SqlParserPos.ZERO) : null,
        null,
        sourceName != null ? new SqlIdentifier(sourceName, SqlParserPos.ZERO) : null,
        likePattern != null ? SqlLiteral.createCharString(likePattern, SqlParserPos.ZERO) : null);
    }
  }

}
