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
package com.dremio.services.nessie.proxy;

import org.projectnessie.api.v1.params.BaseMergeTransplant;
import org.projectnessie.client.api.MergeTransplantBuilder;
import org.projectnessie.client.api.PagingBuilder;
import org.projectnessie.model.Branch;
import org.projectnessie.model.Detached;
import org.projectnessie.model.Reference;
import org.projectnessie.model.Tag;

final class ProxyUtil {
  private ProxyUtil() {}

  static <B extends PagingBuilder<B, RESP, ENTRY>, RESP, ENTRY> B paging(B builder, String pageToken,
                                                                         Integer maxRecords) {
    if (pageToken != null) {
      builder.pageToken(pageToken);
    }
    if (maxRecords != null) {
      builder.maxRecords(maxRecords);
    }
    return builder;
  }

  static <T extends MergeTransplantBuilder<T>> MergeTransplantBuilder<T> applyBaseMergeTransplant(
    MergeTransplantBuilder<T> builder, BaseMergeTransplant base) {
    if (base.getKeyMergeModes() != null) {
      base.getKeyMergeModes().forEach(
        keyBehavior -> builder.mergeMode(keyBehavior.getKey(), keyBehavior.getMergeBehavior()));
    }
    if (base.getDefaultKeyMergeMode() != null) {
      builder.defaultMergeMode(base.getDefaultKeyMergeMode());
    }
    if (base.keepIndividualCommits() != null) {
      builder.keepIndividualCommits(base.keepIndividualCommits());
    }
    if (base.isDryRun() != null) {
      builder.dryRun(base.isDryRun());
    }
    if (base.isFetchAdditionalInfo() != null) {
      builder.fetchAdditionalInfo(base.isFetchAdditionalInfo());
    }
    if (base.isReturnConflictAsResult() != null) {
      builder.returnConflictAsResult(base.isReturnConflictAsResult());
    }
    return builder;
  }

  public static Reference toReference(String name, Reference.ReferenceType type, String hash) {
    if (name == null) {
      return Detached.of(hash);
    }

    switch (type) {
      case BRANCH:
        return Branch.of(name, hash);
      case TAG:
        return Tag.of(name, hash);
      default:
        throw new IllegalArgumentException("Unsupported reference type: " + type);
    }
  }

}
