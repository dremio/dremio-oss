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
import java.util.List;
import java.util.Map;

import com.dremio.catalog.model.VersionContext;
import com.dremio.common.map.CaseInsensitiveMap;
import com.dremio.dac.proto.model.dataset.DatasetProtobuf;
import com.dremio.dac.proto.model.dataset.SourceVersionReference;
import com.dremio.dac.proto.model.dataset.VersionContextType;

public class SourceVersionReferenceUtils {

  public static List<SourceVersionReference> createSourceVersionReferenceList(List<DatasetProtobuf.SourceVersionReference> references) {
    List<SourceVersionReference> sourceVersionReferenceList = new ArrayList<>();
    if (references != null) {
      for (DatasetProtobuf.SourceVersionReference entry: references) {
        DatasetProtobuf.VersionContext versionContext = entry.getReference();
        DatasetProtobuf.VersionContextType versionContextType = versionContext.getType();
        String sourceName= entry.getSourceName();
        switch (versionContextType) {
          case BRANCH:
            com.dremio.dac.proto.model.dataset.VersionContext versionContextBranch =
              new com.dremio.dac.proto.model.dataset.VersionContext(VersionContextType.BRANCH, versionContext.getValue());
            sourceVersionReferenceList.add(new SourceVersionReference(sourceName, versionContextBranch));
            break;
          case TAG:
            com.dremio.dac.proto.model.dataset.VersionContext versionContextTag =
              new com.dremio.dac.proto.model.dataset.VersionContext(VersionContextType.TAG, versionContext.getValue());
            sourceVersionReferenceList.add(new SourceVersionReference(sourceName, versionContextTag));
            break;
          case COMMIT:
            com.dremio.dac.proto.model.dataset.VersionContext versionContextCommit =
              new com.dremio.dac.proto.model.dataset.VersionContext(VersionContextType.COMMIT, versionContext.getValue());
            sourceVersionReferenceList.add(new SourceVersionReference(sourceName, versionContextCommit));
            break;
          default:
            throw new IllegalArgumentException("Unrecognized versionContextType: " + versionContextType);
        }
      }
    }

    return sourceVersionReferenceList;
  }

  public static List<DatasetProtobuf.SourceVersionReference> createSourceVersionReferenceProtoList(List<SourceVersionReference> references) {
    List<DatasetProtobuf.SourceVersionReference> sourceVersionReferenceProtoList = new ArrayList<>();
    if (references != null) {
      for (SourceVersionReference entry: references) {
        com.dremio.dac.proto.model.dataset.VersionContext versionContext = entry.getReference();
        com.dremio.dac.proto.model.dataset.VersionContextType versionContextType = versionContext.getType();
        String sourceName= entry.getSourceName();
        DatasetProtobuf.VersionContextType protoVersionContextType;
        switch (versionContextType) {
          case BRANCH:
            protoVersionContextType = DatasetProtobuf.VersionContextType.BRANCH;
            break;
          case TAG:
            protoVersionContextType = DatasetProtobuf.VersionContextType.TAG;
            break;
          case COMMIT:
            protoVersionContextType = DatasetProtobuf.VersionContextType.COMMIT;
            break;
          default:
            throw new IllegalArgumentException("Unrecognized versionContextType: " + versionContextType);
        }
        sourceVersionReferenceProtoList.add(com.dremio.dac.proto.model.dataset.DatasetProtobuf.SourceVersionReference.newBuilder()
          .setSourceName(sourceName)
          .setReference(DatasetProtobuf.VersionContext.newBuilder()
            .setType(protoVersionContextType)
            .setValue(versionContext.getValue())
            .build())
          .build());

      }
    }

    return sourceVersionReferenceProtoList;
  }

  public static List<DatasetProtobuf.SourceVersionReference> createSourceVersionReferenceListFromContextMap(
    CaseInsensitiveMap<com.dremio.catalog.model.VersionContext> references) {
    List<DatasetProtobuf.SourceVersionReference> sourceVersionReferenceList = new ArrayList<>();
    if (references != null) {
      for (Map.Entry<String, VersionContext> entry: references.entrySet()) {
        VersionContext versionContext = entry.getValue();
        VersionContext.Type versionContextType = versionContext.getType();
        String sourceName= entry.getKey();
        DatasetProtobuf.VersionContextType protoVersionContextType;
        switch (versionContextType) {
          case BRANCH:
            protoVersionContextType = DatasetProtobuf.VersionContextType.BRANCH;
            break;
          case TAG:
            protoVersionContextType = DatasetProtobuf.VersionContextType.TAG;
            break;
          case COMMIT:
            protoVersionContextType = DatasetProtobuf.VersionContextType.COMMIT;
            break;
          default:
            throw new IllegalArgumentException("Unrecognized versionContextType: " + versionContextType);
        }
        sourceVersionReferenceList.add(com.dremio.dac.proto.model.dataset.DatasetProtobuf.SourceVersionReference.newBuilder()
          .setSourceName(sourceName)
          .setReference(DatasetProtobuf.VersionContext.newBuilder()
            .setType(protoVersionContextType)
            .setValue(versionContext.getValue())
            .build())
          .build());
      }
    }

    return sourceVersionReferenceList;
  }

}
