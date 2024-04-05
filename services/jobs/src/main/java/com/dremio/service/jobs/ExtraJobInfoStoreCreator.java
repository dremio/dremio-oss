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
package com.dremio.service.jobs;

import com.dremio.datastore.api.DocumentConverter;
import com.dremio.datastore.api.DocumentWriter;
import com.dremio.datastore.api.LegacyIndexedStore;
import com.dremio.datastore.api.LegacyIndexedStoreCreationFunction;
import com.dremio.datastore.api.LegacyStoreBuildingFactory;
import com.dremio.datastore.format.Format;
import com.dremio.datastore.indexed.IndexKey;
import com.dremio.service.job.proto.ExtraJobInfo;
import com.dremio.service.job.proto.JobId;
import java.util.Date;

/** Creator for ExtraJobinfo store. */
public class ExtraJobInfoStoreCreator
    implements LegacyIndexedStoreCreationFunction<JobId, ExtraJobInfo> {
  public static final String NAME = "extraJobInfo";
  public static final IndexKey EXTRA_JOB_INFO_TTL_EXPIRY =
      IndexKey.newBuilder("ttl", "expireAt", Date.class).build();

  @SuppressWarnings("unchecked")
  @Override
  public LegacyIndexedStore<JobId, ExtraJobInfo> build(LegacyStoreBuildingFactory factory) {
    return factory
        .<JobId, ExtraJobInfo>newStore()
        .name(NAME)
        .keyFormat(Format.wrapped(JobId.class, JobId::getId, JobId::new, Format.ofString()))
        .valueFormat(Format.ofProtostuff(ExtraJobInfo.class))
        .buildIndexed(new ExtraJobInfoConverter());
  }

  private static final class ExtraJobInfoConverter
      implements DocumentConverter<JobId, ExtraJobInfo> {
    private Integer version = 1;

    @Override
    public void convert(DocumentWriter writer, JobId key, ExtraJobInfo record) {
      if (record.getTtlExpireAt() != null) {
        writer.writeTTLExpireAt(EXTRA_JOB_INFO_TTL_EXPIRY, record.getTtlExpireAt());
      }
    }

    @Override
    public Integer getVersion() {
      return version;
    }
  }
}
