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

package com.dremio.datastore.api.options;

import org.immutables.value.Value;

import com.dremio.datastore.RemoteDataStoreProtobuf.PutOptionInfo;
import com.dremio.datastore.RemoteDataStoreProtobuf.PutOptionType;
import com.dremio.datastore.api.Document;
import com.dremio.datastore.api.KVStore;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;

/**
 * VersionOption for specifying version tag for optimistic concurrency control. It is used as an
 * option in PUT and DELETE KVStore operations. PUT and DELETE operations would fail if the version
 * of the target document has been updated since the tag provided by this VersionOption.
 */
@JsonDeserialize(builder = ImmutableVersionOption.Builder.class)
@Value.Immutable
public interface VersionOption extends KVStore.PutOption, KVStore.DeleteOption {

  /**
   * Gets the version tag.
   * @return the version tag associated with this option object.
   */
  String getTag();

  @Override
  default PutOptionInfo getPutOptionInfo() {
    return PutOptionInfo.newBuilder()
      .setType(PutOptionType.VERSION)
      .setParameter(getTag())
      .build();
  }

  static VersionOption from(Document<?, ?> doc) {
    Preconditions.checkArgument(doc.getTag() != null);
    return new ImmutableVersionOption.Builder().setTag(doc.getTag()).build();
  }

  /**
   * Stores information about the VersionOption parsed from an array of KVStoreOptions.
   */
  class TagInfo {
    private final boolean hasVersionOption;
    private final boolean hasCreateOption;
    private final String tag;

    @VisibleForTesting
    public TagInfo(boolean hasVersionOption, boolean hasCreateOption, String tag) {
      this.hasVersionOption = hasVersionOption;
      this.hasCreateOption = hasCreateOption;
      this.tag = tag;
    }

    public boolean hasVersionOption() {
      return hasVersionOption;
    }

    public boolean hasCreateOption() {
      return hasCreateOption;
    }

    public String getTag() {
      return tag;
    }
  }

  /**
   * Retrieve the version option.
   */
  static TagInfo getTagInfo(KVStore.KVStoreOption... options) {
    KVStoreOptionUtility.validateOptions(options);

    if (null == options || options.length == 0) {
      return new TagInfo(false, false, null);
    }

    for (KVStore.KVStoreOption option: options) {
      if (option instanceof VersionOption) {
        return new TagInfo(true, false, ((VersionOption) option).getTag());
      } else if (option == CREATE) {
        // XXX: This is kinda gross. Create is not a version option, yet we collapse it down to this piece of information.
        // We should work to deprecate/eliminate this class, or at the very least stop making it represent a Create.
        return new TagInfo(true, true, null);
      }
    }

    return new TagInfo(false, false, null);
  }
}
