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

package com.dremio.service.script;

import com.dremio.catalog.model.VersionContext;
import com.dremio.common.map.CaseInsensitiveMap;
import com.dremio.context.RequestContext;
import com.dremio.context.UserContext;
import com.dremio.dac.proto.model.dataset.DatasetProtobuf;
import com.dremio.datastore.SearchQueryUtils;
import com.dremio.datastore.SearchTypes;
import com.dremio.datastore.api.Document;
import com.dremio.datastore.api.FindByCondition;
import com.dremio.datastore.api.ImmutableFindByCondition;
import com.dremio.datastore.indexed.IndexKey;
import com.dremio.sabot.rpc.user.UserSession;
import com.dremio.service.script.proto.ScriptProto.Script;
import com.dremio.service.script.proto.ScriptProto.ScriptRequest;
import com.dremio.service.usersessions.UserSessionService;
import com.google.common.base.Preconditions;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import io.opentelemetry.instrumentation.annotations.WithSpan;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import javax.inject.Inject;
import javax.inject.Provider;
import javax.ws.rs.NotFoundException;

/** ScriptService to perform various operations of script. */
public class ScriptServiceImpl implements ScriptService {
  private static final org.slf4j.Logger logger =
      org.slf4j.LoggerFactory.getLogger(ScriptServiceImpl.class);

  private static final Long CONTENT_MAX_LENGTH = 250_000L;
  private static final Long DESCRIPTION_MAX_LENGTH = 1024L;
  private static final Long MAX_SCRIPTS_PER_USER = 1000L;
  private static final Long NAME_MAX_LENGTH = 128L;
  private static final Map<String, IndexKey> sortParamToIndex =
      new HashMap<String, IndexKey>() {
        {
          put("name", ScriptStoreIndexedKeys.NAME);
          put("createdAt", ScriptStoreIndexedKeys.CREATED_AT);
          put("modifiedAt", ScriptStoreIndexedKeys.MODIFIED_AT);
        }
      };

  private final Provider<ScriptStore> scriptStoreProvider;

  private ScriptStore scriptStore;
  private final Provider<UserSessionService> userSessionServiceProvider;

  @Inject
  public ScriptServiceImpl(
      Provider<ScriptStore> scriptStoreProvider,
      Provider<UserSessionService> userSessionServiceProvider) {
    this.scriptStoreProvider = scriptStoreProvider;
    this.userSessionServiceProvider = userSessionServiceProvider;
  }

  @Override
  @WithSpan
  public List<Script> getScripts(
      int offset, int limit, String search, String orderBy, String filter, String createdBy) {
    ImmutableFindByCondition.Builder builder = new ImmutableFindByCondition.Builder();

    FindByCondition condition =
        builder
            .setCondition(getConditionForAccessibleScripts(search, filter, createdBy))
            .setOffset(offset)
            .setLimit(limit)
            .setSort(getSortCondition(orderBy))
            .build();
    Iterable<Document<String, Script>> scripts = scriptStore.getAllByCondition(condition);
    return Lists.newArrayList(Iterables.transform(scripts, Document<String, Script>::getValue));
  }

  protected Iterable<SearchTypes.SearchFieldSorting> getSortCondition(String orderBy) {
    String[] orders = orderBy.split(",");
    List<SearchTypes.SearchFieldSorting> sortOrders = new ArrayList<>();
    for (String order : orders) {
      if (order.length() == 0) {
        continue;
      }
      SearchTypes.SearchFieldSorting.Builder searchFieldSorting =
          SearchTypes.SearchFieldSorting.newBuilder();
      if (order.startsWith("-")) {
        searchFieldSorting.setOrder(SearchTypes.SortOrder.DESCENDING);
        order = order.substring(1);
      } else {
        searchFieldSorting.setOrder(SearchTypes.SortOrder.ASCENDING);
      }
      if (sortParamToIndex.containsKey(order)) {
        searchFieldSorting.setType(sortParamToIndex.get(order).getSortedValueType());
        searchFieldSorting.setField(sortParamToIndex.get(order).getIndexFieldName());
        sortOrders.add(searchFieldSorting.build());
      } else {
        throw new IllegalArgumentException(
            String.format("sort on parameter : %s not supported.", order));
      }
    }
    return sortOrders;
  }

  @Override
  @WithSpan
  public Script createScript(ScriptRequest scriptRequest)
      throws DuplicateScriptNameException, MaxScriptsLimitReachedException {
    // create script entry
    long countOfScriptsByCurrentUser = getCountOfScriptsByCurrentUser();
    if (countOfScriptsByCurrentUser >= MAX_SCRIPTS_PER_USER) {
      throw new MaxScriptsLimitReachedException(MAX_SCRIPTS_PER_USER, countOfScriptsByCurrentUser);
    }
    validateScriptRequest(scriptRequest);
    checkDuplicateScriptName(scriptRequest.getName());

    Script script = newScriptFromScriptRequest(scriptRequest);
    return scriptStore.create(script.getScriptId(), script);
  }

  private long getCountOfScriptsByCurrentUser() {
    SearchTypes.SearchQuery condition =
        getConditionForAccessibleScripts("", "", getCurrentUserId());
    return scriptStore.getCountByCondition(condition);
  }

  protected void validateScriptRequest(ScriptRequest scriptRequest) {
    int scriptContentLength = scriptRequest.getContent().length();
    Preconditions.checkArgument(
        scriptRequest.getContent().length() <= CONTENT_MAX_LENGTH,
        "Maximum %s characters allowed in script content. You have typed in %s characters.",
        CONTENT_MAX_LENGTH,
        scriptContentLength);
    int scriptNameLength = scriptRequest.getName().length();
    Preconditions.checkArgument(
        scriptRequest.getName().length() <= NAME_MAX_LENGTH,
        "Maximum %s characters allowed in script name. You have typed in %s characters.",
        NAME_MAX_LENGTH,
        scriptNameLength);
    int descriptionLength = scriptRequest.getDescription().length();
    Preconditions.checkArgument(
        scriptRequest.getDescription().length() <= DESCRIPTION_MAX_LENGTH,
        "Maximum %s characters allowed in script description. You have typed in %s characters.",
        DESCRIPTION_MAX_LENGTH,
        descriptionLength);
  }

  @Override
  @WithSpan
  public Script updateScript(String scriptId, ScriptRequest scriptRequest)
      throws ScriptNotFoundException, DuplicateScriptNameException, ScriptNotAccessible {
    validateScriptRequest(scriptRequest);

    Script existingScript = getScriptById(scriptId);

    return validateAndUpdateScript(existingScript, scriptRequest);
  }

  @Override
  @WithSpan
  public Script updateScriptContext(String scriptId, String sessionId)
      throws ScriptNotFoundException, ScriptNotAccessible {
    validateScriptId(scriptId);
    validateSessionId(sessionId);

    Script existingScript = getScriptById(scriptId);

    return updateScriptContext(existingScript, sessionId);
  }

  protected Script updateScriptContext(Script existingScript, String sessionId)
      throws ScriptNotFoundException {
    UserSessionService.UserSessionAndVersion sessionAndVersion =
        userSessionServiceProvider.get().getSession(sessionId);
    if (sessionAndVersion == null) {
      throw new NotFoundException(String.format("Session id %s expired/not found.", sessionId));
    }
    final UserSession session = sessionAndVersion.getSession();
    final List<String> context =
        session.getDefaultSchemaPath() == null
            ? Lists.newArrayList()
            : session.getDefaultSchemaPath().getPathComponents();
    final CaseInsensitiveMap<VersionContext> versionContextMap = session.getSourceVersionMapping();
    final List<DatasetProtobuf.SourceVersionReference> referenceList =
        SourceVersionReferenceUtils.createSourceVersionReferenceListFromContextMap(
            versionContextMap);

    Script script =
        existingScript.toBuilder()
            .setModifiedAt(System.currentTimeMillis())
            .setModifiedBy(getCurrentUserId())
            .clearContext()
            .addAllContext(context)
            .clearReferences()
            .addAllReferences(referenceList)
            .build();

    return scriptStore.update(script.getScriptId(), script);
  }

  protected Script validateAndUpdateScript(Script existingScript, ScriptRequest scriptRequest)
      throws ScriptNotFoundException, DuplicateScriptNameException {

    // check if new name entered already exists.
    if (!existingScript.getName().equals(scriptRequest.getName())) {
      checkDuplicateScriptName(scriptRequest.getName());
    }

    Script script =
        existingScript.toBuilder()
            .setName(scriptRequest.getName())
            .setDescription(scriptRequest.getDescription())
            .setModifiedAt(System.currentTimeMillis())
            .setModifiedBy(getCurrentUserId())
            .clearContext()
            .addAllContext(scriptRequest.getContextList())
            .setContent(scriptRequest.getContent())
            .clearReferences()
            .addAllReferences(scriptRequest.getReferencesList())
            .clearJobIds()
            .addAllJobIds(scriptRequest.getJobIdsList())
            .build();

    return scriptStore.update(script.getScriptId(), script);
  }

  @Override
  @WithSpan
  public Script getScriptById(String scriptId) throws ScriptNotFoundException, ScriptNotAccessible {
    // check if scriptId is valid
    validateScriptId(scriptId);

    Optional<Script> script = scriptStore.get(scriptId);
    if (!script.isPresent()) {
      throw new ScriptNotFoundException(scriptId);
    }
    return script.get();
  }

  @Override
  @WithSpan
  public void deleteScriptById(String scriptId)
      throws ScriptNotFoundException, ScriptNotAccessible {

    validateScriptId(scriptId);

    Script script = getScriptById(scriptId);
    scriptStore.delete(scriptId);
  }

  @Override
  @WithSpan
  public Long getCountOfMatchingScripts(String search, String filter, String createdBy) {
    SearchTypes.SearchQuery condition = getConditionForAccessibleScripts(search, filter, createdBy);
    return scriptStore.getCountByCondition(condition);
  }

  private void checkDuplicateScriptName(String name) throws DuplicateScriptNameException {
    try {
      Optional<Script> script = scriptStore.getByName(name);
      if (script.isPresent()) {
        throw new DuplicateScriptNameException(name);
      }
    } catch (ScriptNotFoundException ignored) {

    }
  }

  private Script newScriptFromScriptRequest(ScriptRequest scriptRequest) {
    long currentTime = System.currentTimeMillis();
    return scriptFromData(
        UUID.randomUUID().toString(),
        scriptRequest.getName(),
        currentTime,
        getCurrentUserId(),
        scriptRequest.getDescription(),
        currentTime,
        getCurrentUserId(),
        scriptRequest.getContextList(),
        scriptRequest.getReferencesList(),
        scriptRequest.getContent(),
        scriptRequest.getJobIdsList());
  }

  private Script scriptFromData(
      String scriptId,
      String name,
      long createdAt,
      String createdBy,
      String description,
      long modifiedAt,
      String modifiedBy,
      List<String> context,
      List<DatasetProtobuf.SourceVersionReference> references,
      String content,
      List<String> jobIds) {
    return Script.newBuilder()
        .setScriptId(scriptId)
        .setName(name)
        .setCreatedAt(createdAt)
        .setCreatedBy(createdBy)
        .setDescription(description)
        .setModifiedAt(modifiedAt)
        .setModifiedBy(modifiedBy)
        .addAllContext(context)
        .addAllReferences(references)
        .setContent(content)
        .addAllJobIds(jobIds)
        .build();
  }

  protected SearchTypes.SearchQuery getConditionForAccessibleScripts(
      String search, String filter, String createdBy) {
    List<SearchTypes.SearchQuery> conditions = new ArrayList<>();
    conditions.add(SearchQueryUtils.newContainsTerm(ScriptStoreIndexedKeys.NAME, search));

    if (createdBy != null) {
      conditions.add(SearchQueryUtils.newTermQuery(ScriptStoreIndexedKeys.CREATED_BY, createdBy));
    }
    return SearchQueryUtils.and(conditions);
  }

  protected String getCurrentUserId() {
    return RequestContext.current().get(UserContext.CTX_KEY).getUserId();
  }

  public void validateScriptId(String scriptId) {
    validateUUID("scriptId", scriptId);
  }

  public void validateSessionId(String sessionId) {
    validateUUID("sessionId", sessionId);
  }

  private void validateUUID(String fieldName, String uuid) {
    Preconditions.checkNotNull(uuid, fieldName + " must be provided.");
    try {
      UUID.fromString(uuid);
    } catch (IllegalArgumentException exception) {
      logger.error("{} : {} is not a valid UUID.", fieldName, uuid);
      throw new IllegalArgumentException(
          String.format("%s '%s' must be valid UUID.", fieldName, uuid));
    }
  }

  @Override
  public void start() throws Exception {
    logger.info("Starting ScriptService");
    this.scriptStore = scriptStoreProvider.get();
    logger.info("Script Service is up.");
  }

  @Override
  public void close() throws Exception {}
}
