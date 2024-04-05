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
package com.dremio.plugins;

import static com.dremio.plugins.NessieClient.ContentMode.ENTRY_WITH_CONTENT;
import static com.dremio.plugins.NessieClient.NestingMode.IMMEDIATE_CHILDREN_ONLY;
import static com.dremio.plugins.NessieClientOptions.BYPASS_CONTENT_CACHE;
import static com.dremio.plugins.NessieClientOptions.NESSIE_CONTENT_CACHE_SIZE_ITEMS;
import static com.dremio.plugins.NessieClientOptions.NESSIE_CONTENT_CACHE_TTL_MINUTES;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.RETURNS_SELF;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.projectnessie.model.Content.Type.ICEBERG_TABLE;
import static org.projectnessie.model.Content.Type.NAMESPACE;
import static org.projectnessie.model.EntriesResponse.Entry.entry;

import com.dremio.catalog.model.ResolvedVersionContext;
import com.dremio.catalog.model.VersionContext;
import com.dremio.context.RequestContext;
import com.dremio.context.UserContext;
import com.dremio.exec.store.ConnectionRefusedException;
import com.dremio.exec.store.ReferenceInfo;
import com.dremio.exec.store.ReferenceNotFoundByTimestampException;
import com.dremio.options.OptionManager;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import java.net.ConnectException;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.commons.collections4.ListUtils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;
import org.projectnessie.client.api.DeleteBranchBuilder;
import org.projectnessie.client.api.GetAllReferencesBuilder;
import org.projectnessie.client.api.GetCommitLogBuilder;
import org.projectnessie.client.api.GetContentBuilder;
import org.projectnessie.client.api.GetEntriesBuilder;
import org.projectnessie.client.api.NessieApiV2;
import org.projectnessie.client.http.HttpClientException;
import org.projectnessie.client.rest.NessieNotAuthorizedException;
import org.projectnessie.error.ErrorCode;
import org.projectnessie.error.ImmutableNessieError;
import org.projectnessie.error.NessieBadRequestException;
import org.projectnessie.error.NessieConflictException;
import org.projectnessie.error.NessieError;
import org.projectnessie.error.NessieNotFoundException;
import org.projectnessie.model.Branch;
import org.projectnessie.model.CommitMeta;
import org.projectnessie.model.Content;
import org.projectnessie.model.ContentKey;
import org.projectnessie.model.IcebergTable;
import org.projectnessie.model.ImmutableNamespace;
import org.projectnessie.model.LogResponse;
import org.projectnessie.model.Reference;
import org.projectnessie.model.ReferencesResponse;
import org.projectnessie.model.Tag;

@MockitoSettings(strictness = Strictness.STRICT_STUBS)
@ExtendWith(MockitoExtension.class)
public class TestNessieClientImpl {

  private static final List<String> CATALOG_KEY = Arrays.asList("test1", "test2");
  private static final List<String> CATALOG_KEY_2 = Arrays.asList("test3", "test4");
  private static final ContentKey CONTENT_KEY = ContentKey.of(CATALOG_KEY);
  private static final Content CONTENT = IcebergTable.of("test", 0L, 0, 0, 0);
  private static final ResolvedVersionContext VERSION =
      ResolvedVersionContext.ofBranch("main", "0123456789abcdeff");
  private static final ResolvedVersionContext VERSION_2 =
      ResolvedVersionContext.ofBranch("dev", "0123456789bbcdeff");
  private static final Map<ContentKey, Content> CONTENT_MAP =
      new HashMap<ContentKey, Content>() {
        {
          put(CONTENT_KEY, CONTENT);
        }
      };
  private static final List<Branch> BRANCHES =
      Arrays.asList(
          Branch.of("main", "a0f4f33a14fa610c75ff8cd89b6a54f5df61fcb7"),
          Branch.of("dev", "07b92b065b57ec8d69c5249daa33c329259f7284"));
  private static final List<Tag> TAGS =
      Arrays.asList(
          Tag.of("version1", "63941c19fcae9207e5cdf567e53e5e417a46b75a"),
          Tag.of("version2", "1231251c19fcae9207e5cdfdaa33c329259f7284"));
  private static final List<Reference> REFERENCES = ListUtils.union(BRANCHES, TAGS);

  @Mock private NessieApiV2 nessieApi;
  @Mock private OptionManager optionManager;

  private NessieClientImpl nessieClient;

  private GetContentBuilder builder;

  @BeforeEach
  public void setUp(TestInfo info) {
    final Set<String> testTags = info != null ? info.getTags() : ImmutableSet.of();
    if (testTags.stream().anyMatch(tag -> tag.equals("skipBeforeEach"))) {
      return;
    }

    doReturn(NESSIE_CONTENT_CACHE_SIZE_ITEMS.getDefault().getNumVal())
        .when(optionManager)
        .getOption(NESSIE_CONTENT_CACHE_SIZE_ITEMS);
    doReturn(NESSIE_CONTENT_CACHE_TTL_MINUTES.getDefault().getNumVal())
        .when(optionManager)
        .getOption(NESSIE_CONTENT_CACHE_TTL_MINUTES);
    doReturn(BYPASS_CONTENT_CACHE.getDefault().getBoolVal())
        .when(optionManager)
        .getOption(BYPASS_CONTENT_CACHE);

    builder = mock(GetContentBuilder.class, RETURNS_SELF);
    nessieClient = spy(new NessieClientImpl(nessieApi, optionManager));
  }

  @Test
  public void testNessieContentCacheWithoutUser() throws NessieNotFoundException {
    when(nessieApi.getContent()).thenReturn(builder);
    when(builder.get()).thenReturn(CONTENT_MAP);

    RequestContext.empty().run(() -> nessieClient.getContent(CATALOG_KEY, VERSION, null));
    verify(builder, times(1)).get();

    RequestContext.empty().run(() -> nessieClient.getContent(CATALOG_KEY, VERSION, null));
    verify(builder, times(2)).get();
  }

  @Test
  public void testNessieContentCacheWithSameUser() throws NessieNotFoundException {
    when(nessieApi.getContent()).thenReturn(builder);
    when(builder.get()).thenReturn(CONTENT_MAP);

    RequestContext.current()
        .with(UserContext.CTX_KEY, new UserContext("User1"))
        .run(() -> nessieClient.getContent(CATALOG_KEY, VERSION, null));
    verify(builder, times(1)).get();

    RequestContext.current()
        .with(UserContext.CTX_KEY, new UserContext("User1"))
        .run(() -> nessieClient.getContent(CATALOG_KEY, VERSION, null));
    verify(builder, times(1)).get();
  }

  @Test
  public void testNessieContentCacheWithDifferentVersion() throws NessieNotFoundException {
    when(nessieApi.getContent()).thenReturn(builder);
    when(builder.get()).thenReturn(generateRandomMap());

    RequestContext.current()
        .with(UserContext.CTX_KEY, new UserContext("User1"))
        .run(() -> nessieClient.getContent(CATALOG_KEY, VERSION, null));
    verify(builder, times(1)).get();

    RequestContext.current()
        .with(UserContext.CTX_KEY, new UserContext("User1"))
        .run(() -> nessieClient.getContent(CATALOG_KEY, VERSION_2, null));
    verify(builder, times(2)).get();
  }

  @Test
  public void testNessieContentCacheWithDifferentCatalogKey() throws NessieNotFoundException {
    when(nessieApi.getContent()).thenReturn(builder);
    when(builder.get()).thenReturn(CONTENT_MAP);

    RequestContext.current()
        .with(UserContext.CTX_KEY, new UserContext("User1"))
        .run(() -> nessieClient.getContent(CATALOG_KEY, VERSION, null));
    verify(builder, times(1)).get();

    RequestContext.current()
        .with(UserContext.CTX_KEY, new UserContext("User1"))
        .run(() -> nessieClient.getContent(CATALOG_KEY_2, VERSION, null));
    verify(builder, times(2)).get();
  }

  @Test
  public void testNessieContentCacheMultipleUsers() throws NessieNotFoundException {
    when(nessieApi.getContent()).thenReturn(builder);
    when(builder.get()).thenReturn(CONTENT_MAP);

    RequestContext.current()
        .with(UserContext.CTX_KEY, new UserContext("User1"))
        .run(() -> nessieClient.getContent(CATALOG_KEY, VERSION, null));
    verify(builder, times(1)).get();

    RequestContext.current()
        .with(UserContext.CTX_KEY, new UserContext("User1"))
        .run(() -> nessieClient.getContent(CATALOG_KEY, VERSION, null));
    verify(builder, times(1)).get();

    RequestContext.current()
        .with(UserContext.CTX_KEY, new UserContext("User2"))
        .run(() -> nessieClient.getContent(CATALOG_KEY, VERSION, null));
    verify(builder, times(2)).get();

    RequestContext.current()
        .with(UserContext.CTX_KEY, new UserContext("User1"))
        .run(() -> nessieClient.getContent(CATALOG_KEY, VERSION, null));
    verify(builder, times(2)).get();
  }

  @Test
  public void testBranchNames() {
    ResolvedVersionContext resolvedVersionContext = mock(ResolvedVersionContext.class);
    when(resolvedVersionContext.getCommitHash())
        .thenReturn("d0628f078890fec234b98b873f9e1f3cd140988a");
    Branch branch = nessieClient.getBranch("test_valid_branch", resolvedVersionContext);
    assertThat(branch.getName()).isEqualTo("test_valid_branch");
    assertThatThrownBy(() -> nessieClient.getBranch("test invalid branch", resolvedVersionContext))
        .hasMessageContaining(
            "Invalid branch name: test invalid branch. Reference name must start with a letter");
  }

  @Test
  public void testTagNames() {
    ResolvedVersionContext resolvedVersionContext = mock(ResolvedVersionContext.class);
    when(resolvedVersionContext.getCommitHash())
        .thenReturn("d0628f078890fec234b98b873f9e1f3cd140988a");
    Tag tag = nessieClient.getTag("test_valid_tag", resolvedVersionContext);
    assertThat(tag.getName()).isEqualTo("test_valid_tag");
    assertThatThrownBy(() -> nessieClient.getTag("test invalid tag", resolvedVersionContext))
        .hasMessageContaining(
            "Invalid tag name: test invalid tag. Reference name must start with a letter");
  }

  @Test
  public void testDropBranch() throws NessieConflictException, NessieNotFoundException {
    DeleteBranchBuilder deleteBranchBuilder = mock(DeleteBranchBuilder.class);
    when(nessieApi.deleteBranch()).thenReturn(deleteBranchBuilder);
    when(deleteBranchBuilder.branchName("main")).thenReturn(deleteBranchBuilder);
    when(deleteBranchBuilder.hash("d0628f078890fec234b98b873f9e1f3cd140988a"))
        .thenReturn(deleteBranchBuilder);
    NessieError nessieError =
        ImmutableNessieError.builder()
            .errorCode(ErrorCode.BAD_REQUEST)
            .status(400)
            .reason("Default branch 'main' cannot be deleted")
            .build();
    doThrow(new NessieBadRequestException(nessieError)).when(deleteBranchBuilder).delete();
    assertThatThrownBy(
            () -> nessieClient.dropBranch("main", "d0628f078890fec234b98b873f9e1f3cd140988a"))
        .hasMessageContaining("Cannot drop the branch 'main'");

    when(deleteBranchBuilder.branchName("not_main")).thenReturn(deleteBranchBuilder);
    when(deleteBranchBuilder.hash("d0628f078890fec234b98b873f9e1f3cd140988b"))
        .thenReturn(deleteBranchBuilder);
    doNothing().when(deleteBranchBuilder).delete();
    nessieClient.dropBranch("not_main", "d0628f078890fec234b98b873f9e1f3cd140988b");
  }

  @Test
  public void testResolveVersionContextThrowsNotAuthorized() throws NessieNotFoundException {

    NessieError nessieError =
        ImmutableNessieError.builder()
            .message("Not Authorized error")
            .errorCode(ErrorCode.FORBIDDEN)
            .status(401)
            .reason("Unauthorized HTTP 401 Error")
            .build();
    when(nessieApi.getDefaultBranch()).thenThrow(new NessieNotAuthorizedException(nessieError));
    assertThatThrownBy(() -> nessieClient.resolveVersionContext(VersionContext.ofRef(null)))
        .hasMessageContaining("Unable to authenticate to the Nessie server");
    assertThatThrownBy(() -> nessieClient.resolveVersionContext(VersionContext.ofRef(null)))
        .hasMessageNotContaining("NessieNotAuthorizedException");
  }

  @Test
  public void testGetDefaultBranchThrowsConnectionRefused() throws NessieNotFoundException {

    ConnectException connectException = new ConnectException("Connection Refused");
    when(nessieApi.getDefaultBranch())
        .thenThrow(
            new HttpClientException(
                "Failed to execute GET request against 'http://localhost:19120/api/v2/trees/-'.",
                connectException));
    assertThatThrownBy(() -> nessieClient.getDefaultBranch())
        .isInstanceOf(ConnectionRefusedException.class)
        .hasMessageContaining("Connection refused while connecting to the Nessie Server");
  }

  private Map<ContentKey, Content> generateRandomMap() {
    Map<ContentKey, Content> map = new HashMap<>();
    ContentKey key = ContentKey.of(Arrays.asList(generateRandomString(), generateRandomString()));
    Content content = IcebergTable.of(generateRandomString(), 0L, 0, 0, 0);
    map.put(key, content);
    return map;
  }

  private String generateRandomString() {
    int leftLimit = 97; // letter 'a'
    int rightLimit = 122; // letter 'z'
    int targetStringLength = 5;
    Random random = new Random();
    StringBuilder buffer = new StringBuilder(targetStringLength);
    for (int i = 0; i < targetStringLength; i++) {
      int randomLimitedInt = leftLimit + (int) (random.nextFloat() * (rightLimit - leftLimit + 1));
      buffer.append((char) randomLimitedInt);
    }
    return buffer.toString();
  }

  @Test
  public void testListBranches() {
    setUpReferences(BRANCHES);
    List<ReferenceInfo> expectedBranches =
        BRANCHES.stream()
            .map(
                ref ->
                    new ReferenceInfo(
                        NessieClientImpl.BRANCH_REFERENCE, ref.getName(), ref.getHash()))
            .collect(Collectors.toList());
    Stream<ReferenceInfo> actualBranches = nessieClient.listBranches();
    assertThat(actualBranches).isNotNull().containsExactlyElementsOf(expectedBranches);
  }

  @Test
  public void testListTags() {
    setUpReferences(TAGS);
    List<ReferenceInfo> expectedTags =
        TAGS.stream()
            .map(
                ref ->
                    new ReferenceInfo(NessieClientImpl.TAG_REFERENCE, ref.getName(), ref.getHash()))
            .collect(Collectors.toList());
    Stream<ReferenceInfo> actualTags = nessieClient.listTags();
    assertThat(actualTags).isNotNull().containsExactlyElementsOf(expectedTags);
  }

  @Test
  public void testListReferences() {
    setUpReferences(REFERENCES);
    List<ReferenceInfo> expectedReferences =
        REFERENCES.stream()
            .map(
                ref ->
                    new ReferenceInfo(
                        ref instanceof Branch
                            ? NessieClientImpl.BRANCH_REFERENCE
                            : NessieClientImpl.TAG_REFERENCE,
                        ref.getName(),
                        ref.getHash()))
            .collect(Collectors.toList());
    Stream<ReferenceInfo> actualReferences = nessieClient.listReferences();
    assertThat(actualReferences).isNotNull().containsExactlyElementsOf(expectedReferences);
  }

  @SuppressWarnings("unchecked")
  private void setUpReferences(List<? extends Reference> references) {
    GetAllReferencesBuilder getAllReferencesBuilder = mock(GetAllReferencesBuilder.class);
    ReferencesResponse referencesResponse = mock(ReferencesResponse.class);
    when(getAllReferencesBuilder.get()).thenReturn(referencesResponse);
    when(referencesResponse.getReferences()).thenReturn((List<Reference>) references);
    when(nessieApi.getAllReferences()).thenReturn(getAllReferencesBuilder);
  }

  @Test
  public void testListVirtualNamespaces() throws NessieNotFoundException {
    GetEntriesBuilder requestBuilder = mock(GetEntriesBuilder.class, RETURNS_SELF);
    when(requestBuilder.stream())
        .thenReturn(
            Stream.of(
                entry(ContentKey.of("a", "b", "T1"), ICEBERG_TABLE, "id1"),
                entry(ContentKey.of("a", "b", "c", "T2"), ICEBERG_TABLE, "id2"),
                entry(ContentKey.of("a", "b", "d", "T4"), ICEBERG_TABLE, "id4"),
                entry(
                    ContentKey.of("a", "b", "e"),
                    NAMESPACE,
                    "bogus-id"), // overriden by the full entry below
                entry(
                    ContentKey.of("a", "b", "e"),
                    NAMESPACE,
                    ImmutableNamespace.builder().addElements("a", "b", "e").id("ns2").build()),
                entry(ContentKey.of("a", "b", "e", "T5"), ICEBERG_TABLE, "id5"),
                entry(ContentKey.of("a", "b", "c"), NAMESPACE, "ns1"),
                entry(ContentKey.of("a", "b", "T3"), ICEBERG_TABLE, "id3")));
    when(nessieApi.getEntries()).thenReturn(requestBuilder);

    Map<String, ExternalNamespaceEntry> entriesByName =
        nessieClient
            .listEntries(
                ImmutableList.of("a", "b"),
                VERSION,
                IMMEDIATE_CHILDREN_ONLY,
                ENTRY_WITH_CONTENT,
                null,
                null)
            .collect(Collectors.toMap(ExternalNamespaceEntry::getName, e -> e));
    assertThat(entriesByName).hasEntrySatisfying("T1", e -> assertThat(e.getId()).isEqualTo("id1"));
    assertThat(entriesByName).hasEntrySatisfying("T3", e -> assertThat(e.getId()).isEqualTo("id3"));
    assertThat(entriesByName).hasEntrySatisfying("c", e -> assertThat(e.getId()).isEqualTo("ns1"));
    assertThat(entriesByName)
        .hasEntrySatisfying("d", e -> assertThat(e.getId()).isNull()); // implicit namespace
    assertThat(entriesByName).hasEntrySatisfying("e", e -> assertThat(e.getId()).isEqualTo("ns2"));
    assertThat(entriesByName)
        .hasEntrySatisfying("e", e -> assertThat(e.getNessieContent()).isNotNull());
    assertThat(entriesByName).hasSize(5);
  }

  @Test
  public void testResolveVersionContextAtTimestamp() throws NessieNotFoundException {
    // Arrange
    String expectedHash = "abcdef";
    Instant someTimestamp = Instant.now();
    VersionContext ofRefAsOfTimestamp = VersionContext.ofRefAsOfTimestamp("main", someTimestamp);

    GetCommitLogBuilder requestBuilder = mock(GetCommitLogBuilder.class, RETURNS_SELF);
    when(requestBuilder.get())
        .thenReturn(
            LogResponse.builder()
                .addLogEntries(
                    LogResponse.LogEntry.builder()
                        .commitMeta(
                            CommitMeta.builder()
                                .hash(expectedHash)
                                .message("unusedButRequired")
                                .build())
                        .build())
                .build());
    when(nessieApi.getCommitLog()).thenReturn(requestBuilder);

    // Act
    ResolvedVersionContext resolvedVersionContext =
        nessieClient.resolveVersionContext(ofRefAsOfTimestamp);

    // Assert
    assertThat(resolvedVersionContext.isCommit()).isTrue();
    assertThat(resolvedVersionContext.getCommitHash()).isEqualTo(expectedHash);
  }

  @Test
  public void testResolveVersionContextAtTimestampThrowsException() throws NessieNotFoundException {
    // Arrange
    Instant someTimestamp = Instant.now();
    VersionContext ofRefAsOfTimestamp = VersionContext.ofRefAsOfTimestamp("main", someTimestamp);

    GetCommitLogBuilder requestBuilder = mock(GetCommitLogBuilder.class, RETURNS_SELF);
    when(requestBuilder.get()).thenReturn(LogResponse.builder().build());
    when(nessieApi.getCommitLog()).thenReturn(requestBuilder);

    // Act
    String error =
        String.format(
            "There are no commits at or before timestamp '%s' in reference '%s'. Please specify another timestamp",
            Timestamp.from(someTimestamp), ofRefAsOfTimestamp.getValue());

    assertThatThrownBy(() -> nessieClient.resolveVersionContext(ofRefAsOfTimestamp))
        .isInstanceOf(ReferenceNotFoundByTimestampException.class)
        .hasMessageContaining(error);
  }

  @Test
  @org.junit.jupiter.api.Tag("skipBeforeEach")
  public void testBypassCache() throws NessieNotFoundException {
    // arrange
    doReturn(NESSIE_CONTENT_CACHE_SIZE_ITEMS.getDefault().getNumVal())
        .when(optionManager)
        .getOption(NESSIE_CONTENT_CACHE_SIZE_ITEMS);
    doReturn(NESSIE_CONTENT_CACHE_TTL_MINUTES.getDefault().getNumVal())
        .when(optionManager)
        .getOption(NESSIE_CONTENT_CACHE_TTL_MINUTES);
    doReturn(true).when(optionManager).getOption(BYPASS_CONTENT_CACHE);

    builder = mock(GetContentBuilder.class, RETURNS_SELF);
    nessieClient = spy(new NessieClientImpl(nessieApi, optionManager));

    when(nessieApi.getContent()).thenReturn(builder);
    when(builder.get()).thenReturn(CONTENT_MAP);

    // act
    RequestContext.current()
        .with(UserContext.CTX_KEY, new UserContext("User1"))
        .run(() -> nessieClient.getContent(CATALOG_KEY, VERSION, null));
    RequestContext.current()
        .with(UserContext.CTX_KEY, new UserContext("User1"))
        .run(() -> nessieClient.getContent(CATALOG_KEY, VERSION, null));

    // assert
    verify(builder, times(2)).get();
    // the nessie api should be called again, because we bypass the cache
    verify(nessieApi.getContent(), times(2)).get();
  }

  @Test
  @org.junit.jupiter.api.Tag("skipBeforeEach")
  public void testCacheNotBypassed() throws NessieNotFoundException {
    // arrange
    doReturn(NESSIE_CONTENT_CACHE_SIZE_ITEMS.getDefault().getNumVal())
        .when(optionManager)
        .getOption(NESSIE_CONTENT_CACHE_SIZE_ITEMS);
    doReturn(NESSIE_CONTENT_CACHE_TTL_MINUTES.getDefault().getNumVal())
        .when(optionManager)
        .getOption(NESSIE_CONTENT_CACHE_TTL_MINUTES);
    doReturn(false).when(optionManager).getOption(BYPASS_CONTENT_CACHE);

    builder = mock(GetContentBuilder.class, RETURNS_SELF);
    nessieClient = spy(new NessieClientImpl(nessieApi, optionManager));

    when(nessieApi.getContent()).thenReturn(builder);
    when(builder.get()).thenReturn(CONTENT_MAP);

    // act
    RequestContext.current()
        .with(UserContext.CTX_KEY, new UserContext("User1"))
        .run(() -> nessieClient.getContent(CATALOG_KEY, VERSION, null));
    RequestContext.current()
        .with(UserContext.CTX_KEY, new UserContext("User1"))
        .run(() -> nessieClient.getContent(CATALOG_KEY, VERSION, null));

    // assert
    verify(builder, times(1)).get();
    // the nessie api should only be called once even though we made 2 calls to getContent
    verify(nessieApi.getContent(), times(1)).get();
  }
}
