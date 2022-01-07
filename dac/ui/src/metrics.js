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

import ApiUtils from 'utils/apiUtils/apiUtils';
import moment from 'moment';

export default {
  fetchClusterStats() {
    return ApiUtils.fetchJson('cluster/stats?showCompactStats=true', json => {
      return json;
    }, () => {
    }); //ignore errors
  },

  fetchDailyJobStats(numDaysBack) {
    let url = 'cluster/jobstats';
    if (numDaysBack) {
      const end = moment().unix() * 1000;
      const start = moment().subtract(numDaysBack, 'days').unix() * 1000;

      url += `?start=${start}&end=${end}&onlyDateWiseTotals=true`;
    }

    return ApiUtils.fetchJson(url, json => {
      return json;
    }, () => {}); //ignore errors
  },

  fetchUserStats(numDaysBack) {
    let url = 'stats/user';
    if (numDaysBack) {
      const end = moment().unix() * 1000;
      const start = moment().subtract(numDaysBack, 'days').unix() * 1000;

      url += `?start=${start}&end=${end}&onlyUniqueUsersByDate=true`;
    }
    return ApiUtils.fetchJson(url, json => {
      return json;
    }, () => {}); //ignore errors
  }
};
