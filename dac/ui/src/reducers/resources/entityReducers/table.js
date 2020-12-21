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
import { RUN_TABLE_TRANSFORM_START } from 'actions/explore/dataset/common';
import {
  LOAD_NEXT_ROWS_SUCCESS,
  UPDATE_EXPLORE_JOB_PROGRESS,
  INIT_EXPLORE_JOB_PROGRESS,
  FAILED_EXPLORE_JOB_PROGRESS,
  SET_EXPLORE_JOBID_IN_PROGRESS,
  UPDATE_EXPLORE_JOB_RECORDS } from 'actions/explore/dataset/data';

import {
  UPDATE_COLUMN_FILTER
} from 'actions/explore/view';
import {JOB_STATUS, isWorking} from '@app/pages/ExplorePage/components/ExploreTable/ExploreTableJobStatus';

export default function table(state, action) {
  switch (action.type) {
  case RUN_TABLE_TRANSFORM_START:
    if (action.meta.nextTable) {
      return state.setIn(['tableData', action.meta.nextTable.get('version')], action.meta.nextTable);
    }
    return state;
  case LOAD_NEXT_ROWS_SUCCESS: {
    const { rows, columns } = action.payload;
    const { offset, datasetVersion } = action.meta;
    const oldRows = state.getIn(['tableData', datasetVersion, 'rows']) || Immutable.List();
    const outputRecords = state.getIn(['tableData', datasetVersion, 'outputRecords']);
    return state.mergeIn(
      ['tableData', datasetVersion],
      {
        rows: oldRows.splice(offset, oldRows.size, ...Immutable.fromJS(rows)),
        columns,
        outputRecords
      }
    );
  }
  case UPDATE_COLUMN_FILTER:
    return state.setIn(['tableData', 'columnFilter'], action.columnFilter);
  case INIT_EXPLORE_JOB_PROGRESS:
    return state.setIn(['tableData', 'jobProgress'], {
      status: JOB_STATUS.starting,
      isRun: action.isRun,
      startTime: new Date().getTime()
      // endTime, jobId, isRun are not defined and are falsy at this time
    });
  case FAILED_EXPLORE_JOB_PROGRESS: {
    const jobProgress = state.getIn(['tableData', action.datasetVersion, 'jobProgress']);
    return state.setIn(['tableData', action.datasetVersion, 'jobProgress'], {
      ...jobProgress,
      status: JOB_STATUS.failed,
      endTime: new Date().getTime()
    });
  }
  case UPDATE_EXPLORE_JOB_PROGRESS: {
    const {jobUpdate} = action;
    // set jobProgress changes in state.tableData
    const jobProgress = state.getIn(['tableData', jobUpdate.datasetVersion, 'jobProgress']);
    const initJobProgress = state.getIn(['tableData', 'jobProgress']);
    const haveChange = !jobProgress
      || jobProgress.jobId !== jobUpdate.id
      || jobProgress.status !== jobUpdate.state
      || (jobProgress.endTime && jobProgress.endTime !== jobUpdate.endTime)
      || jobProgress.datasetVersion !== jobUpdate.datasetVersion;
    const newState = (haveChange) ? state.setIn(['tableData', jobUpdate.datasetVersion, 'jobProgress'], {
      ...jobProgress,
      jobId: jobUpdate.id,
      status: jobUpdate.state,
      // keep startTime as in INIT until job is done
      startTime: (isWorking(jobUpdate.state)) ? initJobProgress.startTime : jobUpdate.startTime,
      endTime: jobUpdate.endTime,
      datasetVersion: jobUpdate.datasetVersion
    }) : state;
    // set outputRecords in state.tableData[version]
    if (jobUpdate.outputRecords && jobUpdate.datasetVersion) {
      return newState.setIn(['tableData', jobUpdate.datasetVersion, 'outputRecords'], jobUpdate.outputRecords);
    }
    return newState;
  }
  case SET_EXPLORE_JOBID_IN_PROGRESS: {
    const previousJobStatus = state.getIn(['tableData', action.datasetVersion, 'jobProgress']);
    const jobProgress = previousJobStatus || state.getIn(['tableData', 'jobProgress']);
    return state.setIn(['tableData', action.datasetVersion, 'jobProgress'], {
      ...jobProgress,
      jobId: action.jobId,
      datasetVersion: action.datasetVersion
    });
  }
  case UPDATE_EXPLORE_JOB_RECORDS: {
    const jobProgress = state.getIn(['tableData', action.datasetVersion, 'jobProgress']);
    return state.setIn(['tableData', action.datasetVersion, 'jobProgress'], {
      ...jobProgress,
      recordCount: action.recordCount
    });
  }
  default:
    return state;
  }
}
