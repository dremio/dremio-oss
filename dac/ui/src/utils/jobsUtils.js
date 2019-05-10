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
import moment from 'moment';

import timeUtils from './timeUtils';

// see JobState.java
export const JobState = {
  NOT_SUBMITTED: 'NOT_SUBMITTED',
  STARTING: 'STARTING',
  RUNNING: 'RUNNING',
  COMPLETED: 'COMPLETED',
  CANCELED: 'CANCELED',
  FAILED: 'FAILED',
  CANCELLATION_REQUESTED: 'CANCELLATION_REQUESTED',
  ENQUEUED: 'ENQUEUED'
};

const RECORD_STEP = 1000;
const RECORDS_IN_THOUSTHAND = RECORD_STEP;

class JobsUtils {

  getNumberOfRunningJobs(jobs) {
    if (jobs) {
      return jobs.filter((item) => item.get('state').toLowerCase &&
        item.get('state').toLowerCase() === JobState.RUNNING).size;
    }
    return 0;
  }

  getFilterOptions(filters) {
    let filterString = '';
    filters.forEach((item) => {
      for (const key in item) {
        if (key !== 'sort' && key !== 'order' && key !== 'st' && key !== 'contains') {
          item[key].forEach((filterItem, indexItem) => {
            const firstBracket = indexItem === 0 ? '(' : '';
            const lastBracket = indexItem === item[key].length - 1 ? ')' : '';
            const separator = indexItem < item[key].length - 1 ? ',' : ';';
            filterString += `${firstBracket}${key}==${filterItem}${lastBracket}${separator}`;
          });
        }
      }
    });
    return filterString.slice(0, -1);
  }

  getSortOption(filters) {
    let filterString = '';
    filters.forEach((item) => {
      for (const key in item) {
        if (key === 'sort' || key === 'order') {
          filterString += `${key}=${item[key]}&`;
        }
      }
    });
    return filterString.slice(0, -1);
  }

  getTimeOption(filters) {
    let filterString = '';
    filters.forEach((item) => {
      for (const key in item) {
        if (key === 'st') {
          filterString = `(st=ge=${item[key][0]};ft=le=${item[key][1]})`;
        }
      }
    });
    return filterString;
  }

  getContainsTextOption(filters) {
    const containsObg = filters.find((item) => {
      return item.contains;
    });
    return containsObg ? containsObg.contains : '';
  }

  msToHHMMSS(ms) {
    return timeUtils.durationWithZero(moment.duration(ms));
  }

  getJobDuration(startTime, endTime) {
    const diff = moment(endTime).diff(moment(startTime));
    return this.formatJobDuration(diff);
  }

  formatJobDuration(duration) {
    return timeUtils.durationWithZero(moment.duration(duration, 'ms'));
  }

  getRunning(jobState) {
    return jobState === JobState.RUNNING;
  }

  getFinishTime(jobState, jobEndTime) {
    if (jobState === JobState.STARTING) {
      return '-';
    }
    if (this.getRunning(jobState)) {
      return 'In progress';
    }
    return timeUtils.formatTime(jobEndTime, 'In progress');
  }

  getFormattedRecords(records) {
    if ((!records && records !== 0) || isNaN(records) || isNaN(Number(records))) {
      return '';
    }

    if (records < RECORDS_IN_THOUSTHAND) {
      return records;
    }

    return records.toString().replace(/\B(?=(\d{3})+(?!\d))/g, ',');
  }

  isMetadataJob(requestType) {
    switch (requestType) {
    case 'GET_CATALOGS':
    case 'GET_COLUMNS':
    case 'GET_SCHEMAS':
    case 'GET_TABLES':
      return true;
    default:
      return false;
    }
  }

  navigationURLForJobId(id, createFullUrl) {
    const url = `/jobs#${encodeURIComponent(id)}`;
    return createFullUrl ? (window.location.origin + url) : url;
  }

  navigationURLForLayoutId(id, createFullUrl) {
    const url = `/jobs?filters=${encodeURIComponent(JSON.stringify({'contains':[id]}))}`;
    return createFullUrl ? (window.location.origin + url) : url;
  }

  getReflectionsByRelationship(jobDetails) {
    if (!jobDetails.get('acceleration')) { // failed planning, pre-1.3, etc guard
      return {};
    }

    const reflectionRelationships = jobDetails.get('acceleration').get('reflectionRelationships').toJS();
    const byRelationship = {};
    for (const reflectionRelationship of reflectionRelationships) {
      byRelationship[reflectionRelationship.relationship] = byRelationship[reflectionRelationship.relationship] || [];
      byRelationship[reflectionRelationship.relationship].push(reflectionRelationship);
    }
    return byRelationship;
  }
}

export default new JobsUtils();
