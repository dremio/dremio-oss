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
import { Link } from 'react-router';

import config from '@inject/utils/config';
import localStorageUtils from '@app/utils/storageUtils/localStorageUtils';
import OverView from '@app/pages/JobDetailsPageNew/components/OverView/OverView.js';
import SQL from '@app/pages/JobDetailsPageNew/components/SQLTab/SQLTab.js';
import Profile from '@app/pages/JobDetailsPageNew/components/Profile/Profile.js';
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

export function getTabs() {
  return ['Overview', 'SQL', 'Profile'];
}

export function getIconName(tab) {
  switch (tab) {
  case 'Overview':
    return 'Shape_lite.svg';
  case 'SQL':
    return 'Union.svg';
  case 'Profile':
    return 'RawProfile.svg';
  default:
    return 'Shape_lite.svg';
  }
}

export function getTagClassName(tab) {
  return tab === 'Profile' ? 'topPanel-rawProfile__rawProfileIcon' : null;
}

export function renderExecutionContent() {
  return (<div></div>);
}

export function renderContent(contentPage, renderProps) {
  const {
    jobDetails,
    downloadJobFile,
    isContrast,
    setIsContrast,
    jobDetailsFromStore,
    showJobIdProfile,
    location
  } = renderProps;
  switch (contentPage) {
  case 'Overview':
    return (
      <OverView
        sql={jobDetails.get('queryText')}
        jobDetails={jobDetails}
        downloadJobFile={downloadJobFile}
        isContrast={isContrast}
        onClick={setIsContrast}
        status={
          jobDetailsFromStore
            ? jobDetailsFromStore.get('state')
            : jobDetails.get('jobStatus')
        }
        location={location}
      />
    );
  case 'SQL':
    return (
      <SQL
        submittedSql={jobDetails.get('queryText')}
        datasetGraph={jobDetails.get('datasetGraph')}
        algebricMatch={jobDetails.get('algebraicReflectionsDataset')}
        isContrast={isContrast}
        onClick={setIsContrast}
      />
    );
  case 'Profile':
    return (
      <Profile
        jobDetails={jobDetails}
        showJobProfile={showJobIdProfile}
      />
    );
  default:
    return (
      <OverView
        sql={jobDetails.get('queryText')}
        status={
          jobDetailsFromStore
            ? jobDetailsFromStore.get('state')
            : jobDetails.get('jobStatus')
        }
        location={location}
      />
    );
  }
}

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
    if (number === undefined || Number.isNaN(number)) {
      return '--';
    }
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
    return filteredUnit ? (number / filteredUnit.value).toFixed(1).replace(rx, '$1') + filteredUnit.symbol : '0';
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

  parseUrlForJobsPageVersion() { // This isn't presently used but was built for the possible issue of having to rediect a url to the old or new jobs page so the user can still view an individual job
    const parseForOldJobs = window.location.href.split('/job/');
    const parseForNewJobs = window.location.href.split('#');
    const isUrlForNewJobsPage = parseForNewJobs.length > parseForOldJobs.length;
    const result = {
      pathname: isUrlForNewJobsPage ? `/job/${parseForNewJobs[1]}` : '/jobs',
      search: isUrlForNewJobsPage ? null : '?filters=%7B"qt"%3A%5B"UI"%2C"EXTERNAL"%5D%7D&order=DESCENDING&sort=st',
      hash: isUrlForNewJobsPage ? null : `#${parseForOldJobs[1]}`,
      isUrlForNewJobsPage
    };
    return result;
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

  formatJobDurationWithMS(duration, isNumberFormat) {
    return timeUtils.durationWithMS(moment.duration(duration, 'ms'), isNumberFormat);
  }

  isNewJobsPage() {
    return config && config.showNewJobsPage;
  }

  getReflectionsLink(item, location) {
    const showLink = localStorageUtils.isUserAnAdmin() && (item.get('reflectionType') === 'RAW' || item.get('reflectionType') === 'AGGREGATE');
    return (
      showLink ?
        <Link to={{
          ...location,
          state: {
            modal: 'AccelerationModal',
            datasetId: item.get('datasetId'),
            layoutId: item.get('reflectionID')
          }
        }}>{item.get('reflectionName')}</Link>
        : item.get('reflectionName')
    );
  }

  getSortLabel = (queryMeasure) => {
    switch (queryMeasure) {
    case 'Runtime':
      return 'Time';
    case 'Total Memory':
      return 'Memory';
    case 'Bytes processed':
      return 'Bytes';
    case 'Parquet':
      return 'Parquet';
    case 'Records':
      return 'Records';
    case 'Thread Skew':
      return 'Thread Skew';
    default:
      return 'Time';
    }
  };

  getSortKey = (queryMeasure) => {
    switch (queryMeasure) {
    case 'Runtime':
      return 'runTime';
    case 'Total Memory':
      return 'totalMemory';
    case 'Bytes processed':
      return 'bytesProcessed';
    case 'Parquet':
      return 'Parquet';
    case 'Records':
      return 'recordsProcessed';
    case 'Thread Skew':
      return 'numThreads';
    default:
      return 'processingTime';
    }
  };

  getSortedArray(arrayToSort, path, order) {
    const pathSplitted = path.split('.');
    return arrayToSort.sort((a, b) => {
      let x = a;
      let y = b;

      pathSplitted && pathSplitted.forEach((key) => {
        x = x[key];
        y = y[key];
      });

      if (order === 'DESC') {
        return y - x;
      } else {
        return x - y;
      }
    });
  }

  bytesToSize(bytes) {
    const sizes = ['Bytes', 'KB', 'MB', 'GB', 'TB'];
    if (bytes === -1 || bytes === undefined || Number.isNaN(bytes)) {
      return '--';
    }
    if (bytes === 0) {
      return '0 Bytes';
    }
    const radix = 10;
    const decimals = 2;
    const i = parseInt(Math.floor(Math.log(bytes) / Math.log(1024)), radix);
    return Math.round(bytes / Math.pow(1024, i), decimals) + ' ' + sizes[i];
  }

  getMetrixValue = (node, sortValue) => {
    switch (sortValue) {
    case 'runtime':
      return timeUtils.nanoSecondsUpToHours(node[sortValue]);
    case 'totalMemory':
      return this.bytesToSize(node[sortValue]);
    case 'recordsProcessed':
      return this.getFormattedNumber(node[sortValue]);
    default:
      return timeUtils.nanoSecondsUpToHours(node[sortValue]);
    }
  };
}

export default new JobsUtils();
