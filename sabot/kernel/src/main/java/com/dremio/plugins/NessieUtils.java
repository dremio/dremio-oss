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

import com.dremio.common.util.Retryer;
import javax.inject.Provider;
import org.projectnessie.client.api.NessieApiV2;
import org.projectnessie.error.BaseNessieClientServerException;
import org.projectnessie.error.NessieRuntimeException;
import org.projectnessie.model.Branch;
import org.projectnessie.model.CommitMeta;
import org.projectnessie.model.ContentKey;
import org.projectnessie.model.Namespace;
import org.projectnessie.model.Operation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class NessieUtils {

  private static final Logger logger = LoggerFactory.getLogger(NessieUtils.class);

  private static final int NESSIE_NS_AUTOCREATE_RETRIES = 10;
  private static final int NESSIE_NS_AUTOCREATE_BASE_WAIT_MS = 5;
  private static final int NESSIE_NS_AUTOCREATE_MAX_WAIT_MS = 500;

  private NessieUtils() {
    // Utility class not meant for instantiation.
  }

  public static void createNamespaceInDefaultBranchIfNotExists(
      Provider<NessieApiV2> apiProvider, String singleLevelNamespace) {
    // Retries are needed to resolve rare, but possible contention on namespace creation from
    // multiple engine nodes.
    // Note: The operation is idempotent.
    Retryer.newBuilder()
        .retryIfExceptionOfType(BaseNessieClientServerException.class)
        .retryIfExceptionOfType(NessieRuntimeException.class)
        .setMaxRetries(NESSIE_NS_AUTOCREATE_RETRIES)
        .setWaitStrategy(
            Retryer.WaitStrategy.EXPONENTIAL,
            NESSIE_NS_AUTOCREATE_BASE_WAIT_MS,
            NESSIE_NS_AUTOCREATE_MAX_WAIT_MS)
        .build()
        .call(
            () -> {
              NessieApiV2 api = apiProvider.get();

              Namespace ns = Namespace.of(singleLevelNamespace);
              ContentKey key = ns.toContentKey();

              Branch branch = api.getDefaultBranch();

              boolean exists = api.getContent().reference(branch).key(key).get().containsKey(key);
              logger.trace("Namespace {} exists: {}", key, exists);

              if (exists) {
                return null;
              }

              logger.debug("Creating Namespace {} in {}", key, branch);

              Branch committed =
                  api.commitMultipleOperations()
                      .commitMeta(CommitMeta.fromMessage("Create namespace " + key))
                      .branch(branch)
                      .operation(Operation.Put.of(key, ns))
                      .commit();

              logger.info("Created Namespace {} in reference {}", key, committed);
              return null;
            });
  }
}
