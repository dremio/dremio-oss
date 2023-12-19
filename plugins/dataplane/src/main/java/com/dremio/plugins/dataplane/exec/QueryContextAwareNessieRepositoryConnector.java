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
package com.dremio.plugins.dataplane.exec;

import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.stream.Stream;

import org.projectnessie.client.api.NessieApiV2;
import org.projectnessie.error.NessieNotFoundException;
import org.projectnessie.gc.repository.NessieRepositoryConnector;
import org.projectnessie.gc.repository.RepositoryConnector;
import org.projectnessie.model.Content;
import org.projectnessie.model.Content.Type;
import org.projectnessie.model.ContentKey;
import org.projectnessie.model.Detached;
import org.projectnessie.model.LogResponse.LogEntry;
import org.projectnessie.model.Reference;

import com.dremio.common.utils.protos.QueryIdHelper;
import com.dremio.context.JobIdContext;
import com.dremio.context.RequestContext;
import com.dremio.exec.proto.UserBitShared.QueryId;

public class QueryContextAwareNessieRepositoryConnector implements RepositoryConnector {

  private final RequestContext requestContext;
  private final RepositoryConnector delegate;

  public QueryContextAwareNessieRepositoryConnector(QueryId queryId, NessieApiV2 nessieApi) {
    this.delegate = NessieRepositoryConnector.nessie(nessieApi);
    this.requestContext = RequestContext.current().with(JobIdContext.CTX_KEY,
        new JobIdContext(QueryIdHelper.getQueryId(queryId)));
  }

  @Override
  public Stream<Reference> allReferences() throws NessieNotFoundException {
    return requestCtxAwareStream(delegate::allReferences);
  }

  @Override
  public Stream<LogEntry> commitLog(Reference ref) throws NessieNotFoundException {
    return requestCtxAwareStream(() -> delegate.commitLog(ref));
  }

  @Override
  public Stream<Entry<ContentKey, Content>> allContents(Detached ref, Set<Type> types)
      throws NessieNotFoundException {
    return requestCtxAwareStream(() -> delegate.allContents(ref, types));
  }

  private <T> Stream<T> requestCtxAwareStream(Callable<Stream<T>> in) throws NessieNotFoundException {
    try {
      return requestContext.callStream(in);
    } catch (NessieNotFoundException e) {
      throw e;
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void close() throws Exception {
    delegate.close();
  }
}
