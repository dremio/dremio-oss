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

import java.util.Optional;

import com.dremio.datastore.SearchTypes;
import com.dremio.datastore.api.Document;
import com.dremio.datastore.api.FindByCondition;
import com.dremio.service.script.proto.ScriptProto;

/**
 * Dummy class for no operations in script store
 */
public class NoOpScriptStoreImpl implements ScriptStore{
  public static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(NoOpScriptStoreImpl.class);

  public NoOpScriptStoreImpl() {
    logger.info("NoOp Script Store is up.");
  }

  @Override
  public void start() throws Exception {
    logger.info("Starting NoOpScriptStoreImpl");
  }

  @Override
  public Optional<ScriptProto.Script> get(String scriptId) throws ScriptNotFoundException {
    throw new UnsupportedOperationException("get not implemented in NoOpScriptStoreImpl");
  }

  @Override
  public Optional<ScriptProto.Script> getByName(String name) throws ScriptNotFoundException {
    throw new UnsupportedOperationException("getByName not implemented in NoOpScriptStoreImpl");
  }

  @Override
  public ScriptProto.Script create(String scriptId,
                                   ScriptProto.Script script) {
    throw new UnsupportedOperationException("create not implemented in NoOpScriptStoreImpl");
  }

  @Override
  public ScriptProto.Script update(String scriptId,
                                   ScriptProto.Script script) throws ScriptNotFoundException {
    throw new UnsupportedOperationException("update not implemented in NoOpScriptStoreImpl");
  }

  @Override
  public void delete(String scriptId) throws ScriptNotFoundException {
    throw new UnsupportedOperationException("delete not implemented in NoOpScriptStoreImpl");
  }

  @Override
  public Iterable<Document<String, ScriptProto.Script>> getAllByCondition(FindByCondition condition) {
    throw new UnsupportedOperationException("getAllByCondition not implemented in NoOpScriptStoreImpl");
  }

  @Override
  public long getCountByCondition(SearchTypes.SearchQuery condition) {
    throw new UnsupportedOperationException("getCountByCondition not implemented in NoOpScriptStoreImpl");
  }

  @Override
  public void close() throws Exception {
    logger.info("Stopped NoOp Script Store.");
  }
}
