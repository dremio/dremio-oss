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

export const sampleResourceTree = {
  resources: [
    {
      type: "SPACE",
      name: "TestFolder",
      fullPath: ["TestFolder"],
      url: "/resourcetree/TestFolder",
      id: "20e97d49-6a10-4753-bea3-3a0e89175d9b",
    },
    {
      id: "1c176bb9-5ff1-4ba0-af31-f3b2bd5b4448",
      type: "SOURCE",
      name: "Samples",
      fullPath: ["Samples"],
      state: {
        status: "good",
        suggestedUserAction: "",
        messages: [],
      },
    },
    {
      type: "HOME",
      name: "@dremio",
      fullPath: ["@dremio"],
      url: "/resourcetree/%22%40dremio%22",
      id: "da197222-e17c-4bcf-9e4f-30c9272ad7dd",
    },
  ],
};
