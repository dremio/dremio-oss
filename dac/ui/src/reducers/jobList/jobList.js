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
import Immutable from 'immutable';
import jobsMapper from 'utils/mappers/jobsMapper';
import StateUtils from 'utils/stateUtils';
import * as ActionTypes from 'actions/joblist/jobList';

const initialState = Immutable.fromJS({
  jobList: [],
  datasetsList: [],
  dataForFilter: {},
  jobDetails: {},
  filters: {},
  orderedColumn: { 'columnName': null, 'order': 'desc' },
  isInProgress: false,
  isFailed: false,
  clusterType: 'NA',
  isSupport: false
});

export default function jobList(state = initialState, action) {
  switch (action.type) {
  case ActionTypes.FILTER_JOBS_LIST_SUCCESS:
    return StateUtils.success(state, ['jobList'], action.payload, jobsMapper.mapJobs)
      .set('filters', new Immutable.Map())
      .set('orderedColumn', new Immutable.Map({ 'columnName': null, 'order': 'desc' }));
  case ActionTypes.FILTER_JOBS_LIST_FAILURE: {
    return StateUtils.failed(state, ['jobList']).set('isFailed', true);
  }
  case ActionTypes.ITEMS_FOR_FILTER_JOBS_LIST_SUCCESS:
    return state.setIn(['dataForFilter', action.meta.tag], action.payload.items);
  case ActionTypes.SET_JOB_LIST_CLUSTER_TYPE:
    return state.set('clusterType', action.payload.clusterType).set('isSupport', action.payload.isSupport);
  default:
    return state;
  }
}
