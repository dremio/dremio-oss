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

const DEFAULT_DURATION_ONE_HOUR = 3600000;
const DEFAULT_DURATION_THREE_HOUR = DEFAULT_DURATION_ONE_HOUR * 3;

export const metaDataPolicyDefaultValues = () => {
  return {
    namesRefreshMillis: DEFAULT_DURATION_ONE_HOUR,
    datasetDefinitionRefreshAfterMillis: DEFAULT_DURATION_ONE_HOUR,
    datasetDefinitionExpireAfterMillis: DEFAULT_DURATION_THREE_HOUR,
  };
};
