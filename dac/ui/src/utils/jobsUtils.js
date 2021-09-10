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
import moment from 'moment';

import localStorageUtils from '@app/utils/storageUtils/localStorageUtils';
import timeUtils from './timeUtils';

// see AttemptEvent.State
export const JobState = {
  PENDING: 'PENDING',
  METADATA_RETRIEVAL: 'METADATA_RETRIEVAL',
  PLANNING: 'PLANNING',
  QUEUED: 'QUEUED',
  ENGINE_START: 'ENGINE_START',
  EXECUTION_PLANNING: 'EXECUTION_PLANNING',
  STARTING: 'STARTING',
  RUNNING: 'RUNNING',
  COMPLETED: 'COMPLETED',
  CANCELED: 'CANCELED',
  FAILED: 'FAILED'
};

const RECORD_STEP = 1000;
const RECORDS_IN_THOUSTHAND = RECORD_STEP;

export class JobsUtils {

  getNumberOfRunningJobs(jobs) {
    if (jobs) {
      return jobs.filter((item) => item.get('state') &&
        item.get('state').toLowerCase() === JobState.RUNNING).size;
    }
    return 0;
  }

  isJobRunning(jobState) {
    return ![JobState.FAILED, JobState.CANCELED, JobState.COMPLETED].includes(jobState);
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
    if (ms === undefined) {
      return '-';
    }
    return timeUtils.durationWithZero(moment.duration(ms));
  }

  getJobDuration(startTime, endTime, isNumberFormat) {
    const diff = moment(endTime).diff(moment(startTime));
    return this.formatJobDuration(diff, isNumberFormat);
  }

  formatJobDuration(duration, isNumberFormat) {
    return timeUtils.durationWithZero(moment.duration(duration, 'ms'), isNumberFormat);
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

  getFormattedNumber(number) {
    const units = [
      { value: 1, symbol: '' },
      { value: 1e3, symbol: 'K' },
      { value: 1e6, symbol: 'M' },
      { value: 1e9, symbol: 'B' },
      { value: 1e12, symbol: 'T' },
      { value: 1e15, symbol: 'P' },
      { value: 1e18, symbol: 'E' }
    ];
    const rx = /\.0+$|(\.[0-9]*[1-9])0+$/;
    const filteredUnit = units.slice().reverse().find((unit) => number >= unit.value);
    return filteredUnit ? (number / filteredUnit.value).toFixed(3).replace(rx, '$1') + filteredUnit.symbol : '0';
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
    let url;

    if (!localStorageUtils.getUserData().admin) {
      url = `/jobs/reflection/${id}`;
    } else {
      url = `/jobs?filters=${encodeURIComponent(JSON.stringify({ 'contains': [id] }))}`;
    }
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

  moveArrayElement(array, fromIndex, toIndex) {
    const element = array[fromIndex];
    array[fromIndex] = array[toIndex];
    array[toIndex] = element;
    return array;
  }
}

export default new JobsUtils();
