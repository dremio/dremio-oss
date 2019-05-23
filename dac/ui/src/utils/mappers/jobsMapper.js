/*
 * Copyright (C) 2017-2018 Dremio Corporation
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
class JobsMapper {
  constructor() {
    this.mapJobDetails = this.mapJobDetails.bind(this);
  }

  mapJobs(payload) {
    return ((payload && payload.jobs) || []).map((item) => {
      return Immutable.fromJS(item);
    });
  }

  mapDatasetsJobs(payload) {
    return payload && payload.jobs.map((item) => {
      return item.jobAttempt.info.dataset;
    });
  }

  jobDetails(json) {
    if (!json) {
      return {};
    }
    return {
      id: json.id,
      user: json.user,
      path: json.path,
      state: json.state
    };
  }

  mapTableDatasetProfiles(datasetList) {
    return datasetList && datasetList.map((item) => {
      const { datasetProfile } = item;
      return {
        datasetPathsList: datasetProfile.datasetPathsList,
        parallelism: datasetProfile.parallelism,
        waitOnSource: datasetProfile.waitOnSource,
        bytesRead: datasetProfile.bytesRead,
        recordsRead: datasetProfile.recordsRead,
        accelerated: datasetProfile.accelerated, // not received pending DX-5519
        pushdownQuery: item.pushdownQuery
      };
    });
  }

  mapJobDetails(json) {
    // @TODO move jobs to normalize
    if (json) {
      json.tableDatasetList = this.mapTableDatasetProfiles(json.tableDatasetProfiles);
      return json;
    }
  }
}

export default new JobsMapper();
