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
import Immutable  from 'immutable';

import * as ActionTypes from 'actions/jobs/jobs';
import jobsMapper from 'utils/mappers/jobsMapper';
import StateUtils from 'utils/stateUtils';

const initialState = Immutable.fromJS({
  jobs: [],
  datasetsList: [],
  dataForFilter: {},
  jobDetails: {},
  filters: {},
  orderedColumn: {'columnName': null, 'order': 'desc'},
  isInProgress: false,
  isFailed: false
});

export default function jobs(state = initialState, action) {
  switch (action.type) {
  case ActionTypes.UPDATE_JOB_STATE: {
    const index = state.get('jobs').findIndex(job => job.get('id') === action.jobId);
    if (index !== -1) {
      const oldJob = state.getIn(['jobs', index]);
      if (!oldJob) return state;

      return state.setIn(['jobs', index], Immutable.Map(
        {
          // For performance, job-progress websocket message does not include these.
          // Can't just merge because jackson omits null fields
          // (there would be no way to override a null value)
          datasetPathList: oldJob.get('datasetPathList'),
          datasetType: oldJob.get('datasetType'),
          ...action.payload
        }
      ));
    }
    return state;
  }
  case ActionTypes.JOBS_DATA_REQUEST:
  case ActionTypes.SORT_JOBS_REQUEST:
  case ActionTypes.FILTER_JOBS_REQUEST:
    return StateUtils.request(state, ['jobs']);

  case ActionTypes.LOAD_NEXT_JOBS_REQUEST:
    return state.set('isNextJobsInProgress', true);

  case ActionTypes.LOAD_NEXT_JOBS_SUCCESS:
    return state.set('isNextJobsInProgress', false)
      .set('jobs', state.get('jobs').concat(Immutable.fromJS(jobsMapper.mapJobs(action.payload))))
      .set('next', action.payload.next);
  case ActionTypes.LOAD_NEXT_JOBS_FAILURE:
    return state.set('isNextJobsInProgress', false);

  case ActionTypes.JOBS_DATA_FAILURE:
  case ActionTypes.SORT_JOBS_FAILURE:
  case ActionTypes.FILTER_JOBS_FAILURE:
    return StateUtils.failed(state, ['jobs']).set('isFailed', true);

  case ActionTypes.JOBS_DATA_SUCCESS :
    return StateUtils.success(state, ['jobs'], action.payload, jobsMapper.mapJobs)
      .set('filters', new Immutable.Map())
      .set('orderedColumn', new Immutable.Map({'columnName': null, 'order': 'desc'}));

  case ActionTypes.SORT_JOBS_SUCCESS:
    return StateUtils.success(state, ['jobs'], action.payload, jobsMapper.mapJobs)
      .set('orderedColumn', action.meta.config);

  case ActionTypes.FILTER_JOBS_SUCCESS:
    return StateUtils.success(state, ['jobs'], action.payload, jobsMapper.mapJobs).set('next', action.payload.next);

  case ActionTypes.JOBS_DATASET_DATA_SUCCESS :
    return StateUtils.success(state, ['datasetsList'], action.payload, jobsMapper.mapDatasetsJobs);

  case ActionTypes.ITEMS_FOR_FILTER_JOBS_SUCCESS :
    return state.setIn(['dataForFilter', action.meta.tag], action.payload.items);

  default:
    return state;
  }
}
