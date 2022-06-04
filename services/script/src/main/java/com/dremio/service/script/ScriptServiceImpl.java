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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

import javax.inject.Inject;
import javax.inject.Provider;
import javax.ws.rs.core.SecurityContext;

import com.dremio.datastore.SearchQueryUtils;
import com.dremio.datastore.SearchTypes;
import com.dremio.datastore.api.Document;
import com.dremio.datastore.api.FindByCondition;
import com.dremio.datastore.api.ImmutableFindByCondition;
import com.dremio.datastore.indexed.IndexKey;
import com.dremio.service.script.proto.ScriptProto.Script;
import com.dremio.service.script.proto.ScriptProto.ScriptRequest;
import com.dremio.service.users.UserNotFoundException;
import com.dremio.service.users.UserService;
import com.google.common.base.Preconditions;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;

/**
 * ScriptService to perform various operations of script.
 */
public class ScriptServiceImpl implements ScriptService {

  private static final org.slf4j.Logger logger =
    org.slf4j.LoggerFactory.getLogger(ScriptServiceImpl.class);

  private static final Long CONTENT_MAX_LENGTH = 10_000L;
  private static final Long DESCRIPTION_MAX_LENGTH = 1024L;
  private static final Long MAX_SCRIPTS_PER_USER = 100L;
  private static final Long NAME_MAX_LENGTH = 128L;

  private final ScriptStore scriptStore;
  private final SecurityContext securityContext;
  private final UserService userService;

  private static final Map<String, IndexKey> sortParamToIndex = new HashMap<String, IndexKey>() {{
    put("name", ScriptStoreIndexedKeys.NAME);
    put("createdAt", ScriptStoreIndexedKeys.CREATED_AT);
    put("modifiedAt", ScriptStoreIndexedKeys.MODIFIED_AT);
  }};

  @Inject
  public ScriptServiceImpl(Provider<ScriptStore> scriptStoreProvider,
                           SecurityContext securityContext,
                           UserService userService) {
    scriptStore = scriptStoreProvider.get();
    this.securityContext = securityContext;
    this.userService = userService;
  }

  @Override
  public List<Script> getScripts(int offset,
                                 int limit,
                                 String search,
                                 String orderBy,
                                 String filter) {
    ImmutableFindByCondition.Builder builder = new ImmutableFindByCondition.Builder();
    FindByCondition condition =
      builder.setCondition(
          SearchQueryUtils.and(
            SearchQueryUtils.newTermQuery(ScriptStoreIndexedKeys.CREATED_BY, getCurrentUserId()),
            SearchQueryUtils.newContainsTerm(ScriptStoreIndexedKeys.NAME, search)
          ))
        .setOffset(offset)
        .setLimit(limit)
        .setSort(getSortCondition(orderBy))
        .build();
    Iterable<Document<String, Script>> scripts = scriptStore.getAllByCondition(condition);
    return Lists.newArrayList(Iterables.transform(scripts, Document<String, Script>::getValue));
  }

  private Iterable<SearchTypes.SearchFieldSorting> getSortCondition(String orderBy) {
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
        throw new IllegalArgumentException(String.format("sort on parameter : %s not supported.",
                                                         order));
      }
    }
    return sortOrders;
  }

  @Override
  public Script createScript(ScriptRequest scriptRequest)
    throws DuplicateScriptNameException {
    // create script entry
    Preconditions.checkArgument(getCountOfMatchingScripts("", "") < MAX_SCRIPTS_PER_USER,
                                String.format("Maximum %s scripts are allowed per user.",
                                              MAX_SCRIPTS_PER_USER));
    Preconditions.checkArgument(scriptRequest.getName().length() <= NAME_MAX_LENGTH,
                                String.format("Maximum %s characters allowed in script name.",
                                              NAME_MAX_LENGTH));
    Preconditions.checkArgument(scriptRequest.getContent().length() <= CONTENT_MAX_LENGTH,
                                String.format("Maximum %s characters allowed in script content.",
                                              CONTENT_MAX_LENGTH));
    Preconditions.checkArgument(scriptRequest.getDescription().length() <= DESCRIPTION_MAX_LENGTH,
                                String.format("Maximum %s characters allowed in script description.",
                                              DESCRIPTION_MAX_LENGTH));

    checkDuplicateScriptName(scriptRequest.getName());

    Script script = newScriptFromScriptRequest(scriptRequest);
    return scriptStore.create(script.getScriptId(), script);
  }

  @Override
  public Script updateScript(String scriptId,
                             ScriptRequest scriptRequest)
    throws ScriptNotFoundException, DuplicateScriptNameException, ScriptNotAccessible {
    Preconditions.checkArgument(scriptRequest.getContent().length() <= CONTENT_MAX_LENGTH,
                                String.format("Maximum %s characters allowed in script content.",
                                              CONTENT_MAX_LENGTH));
    Preconditions.checkArgument(scriptRequest.getName().length() <= NAME_MAX_LENGTH,
                                String.format("Maximum %s characters allowed in script name.",
                                              NAME_MAX_LENGTH));
    Preconditions.checkArgument(scriptRequest.getDescription().length() <= DESCRIPTION_MAX_LENGTH,
                                String.format("Maximum %s characters allowed in script description.",
                                              DESCRIPTION_MAX_LENGTH));

    Script existingScript = getScriptById(scriptId);

    // check if new name entered already exists.
    if (!existingScript.getName().equals(scriptRequest.getName())) {
      checkDuplicateScriptName(scriptRequest.getName());
    }
    Script script = existingScript.toBuilder()
      .setName(scriptRequest.getName())
      .setDescription(scriptRequest.getDescription())
      .setModifiedAt(System.currentTimeMillis())
      .setModifiedBy(getCurrentUserId())
      .clearContext()
      .addAllContext(scriptRequest.getContextList())
      .setContent(scriptRequest.getContent())
      .build();
    return scriptStore.update(script.getScriptId(), script);
  }

  private void checkDuplicateScriptName(String name)
    throws DuplicateScriptNameException {
    try {
      Optional<Script> script =
        scriptStore.getByName(name, getCurrentUserId());
      if (script.isPresent()) {
        throw new DuplicateScriptNameException(name);
      }
    } catch (ScriptNotFoundException ignored) {

    }
  }

  @Override
  public Script getScriptById(String scriptId) throws ScriptNotFoundException, ScriptNotAccessible {
    Optional<Script> script = scriptStore.get(scriptId);
    if (!script.isPresent()) {
      throw new ScriptNotFoundException(scriptId);
    }
    if (!script.get().getCreatedBy().equals(getCurrentUserId())) {
      throw new ScriptNotAccessible();
    }
    return script.get();
  }

  @Override
  public void deleteScriptById(String scriptId)
    throws ScriptNotFoundException, ScriptNotAccessible {
    getScriptById(scriptId);
    scriptStore.delete(scriptId);
  }

  @Override
  public Long getCountOfMatchingScripts(String search, String filter) {
    SearchTypes.SearchQuery
      condition = SearchQueryUtils.and(
      SearchQueryUtils.newTermQuery(ScriptStoreIndexedKeys.CREATED_BY, getCurrentUserId()),
      SearchQueryUtils.newContainsTerm(ScriptStoreIndexedKeys.NAME, search)
    );
    return scriptStore.getCountByCondition(condition);
  }

  private Script newScriptFromScriptRequest(ScriptRequest scriptRequest) {
    long currentTime = System.currentTimeMillis();
    return scriptFromData(UUID.randomUUID().toString(),
                          scriptRequest.getName(),
                          currentTime,
                          getCurrentUserId(),
                          scriptRequest.getDescription(),
                          currentTime,
                          getCurrentUserId(),
                          scriptRequest.getContextList(),
                          scriptRequest.getContent());
  }

  private Script scriptFromData(String scriptId,
                                String name,
                                long createdAt,
                                String createdBy,
                                String description,
                                long modifiedAt,
                                String modifiedBy,
                                List<String> context,
                                String content) {
    return Script.newBuilder()
      .setScriptId(scriptId)
      .setName(name)
      .setCreatedAt(createdAt)
      .setCreatedBy(createdBy)
      .setDescription(description)
      .setModifiedAt(modifiedAt)
      .setModifiedBy(modifiedBy)
      .addAllContext(context)
      .setContent(content)
      .build();
  }

  protected String getCurrentUserId() {
    try {
      return userService.getUser(securityContext.getUserPrincipal().getName()).getUID().getId();
    } catch (UserNotFoundException exception) {
      // ideally this case should never be reached.
      logger.error("User : {} not found. Error {}",
                   securityContext.getUserPrincipal().getName(),
                   exception.getMessage());
      return null;
    }
  }
}
