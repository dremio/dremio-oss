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

export const sampleSources = {
  sources: [
    {
      "@type": "EnterpriseSourceUI",
      config: {
        accessKey: "",
        secure: false,
        externalBucketList: ["samples.dremio.com"],
        rootPath: "/",
        enableAsync: true,
        compatibilityMode: false,
        isCachingEnabled: true,
        maxCacheSpacePct: 100,
        requesterPays: false,
        enableFileStatusCheck: true,
        defaultCtasFormat: "ICEBERG",
        isPartitionInferenceEnabled: false,
        credentialType: "NONE",
      },
      type: "S3",
      name: "Samples",
      ctime: 1695670351914,
      id: "1c176bb9-5ff1-4ba0-af31-f3b2bd5b4448",
      metadataPolicy: {
        updateMode: "PREFETCH_QUERIED",
        namesRefreshMillis: 3600000,
        authTTLMillis: 86400000,
        datasetDefinitionRefreshAfterMillis: 3600000,
        datasetDefinitionExpireAfterMillis: 10800000,
        deleteUnavailableDatasets: true,
        autoPromoteDatasets: true,
      },
      state: {
        status: "good",
        suggestedUserAction: "",
        messages: [],
      },
      tag: "Uzm3DMQkx70=",
      accelerationGracePeriod: 10800000,
      accelerationRefreshPeriod: 3600000,
      accelerationNeverExpire: true,
      accelerationNeverRefresh: true,
      allowCrossSourceSelection: false,
      disableMetadataValidityCheck: false,
      namespaceAttributes: [],
      resourcePath: "/source/Samples",
      fullPathList: ["Samples"],
      links: {
        rename: "/source/Samples/rename",
        jobs: "/jobs?filters=%7B%22contains%22%3A%5B%22Samples%22%5D%2C%22qt%22%3A%5B%22UI%22%2C%22EXTERNAL%22%5D%7D",
        format: "/source/Samples/folder_format",
        self: "/source/Samples",
        file_preview: "/source/Samples/file_preview",
        file_format: "/source/Samples/file_format",
      },
    },
  ],
};
