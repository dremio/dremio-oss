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
import { put, call, select } from 'redux-saga/effects';
import socket from 'utils/socket';
import { testWithHooks } from 'testUtil';
import Immutable from 'immutable';

import { updateViewState } from 'actions/resources';
import { updateHistoryWithJobState } from 'actions/explore/history';
import { addNotification } from 'actions/notification';
import { getViewStateFromAction } from '@app/reducers/resources/view';
import { getExplorePageLocationChangePredicate } from '@app/sagas/utils';

import {
  waitForRunToComplete,
  handleResumeRunDataset,
  getEntities
} from './runDataset';

describe('runDataset saga', () => {
  let gen;
  let next;

  const dataset = Immutable.fromJS({
    datasetVersion: 'version'
  });
  const jobId = 'job';
  const paginationUrl = 'pagination';
  const datasetVersion = '123';

  const successPayload = Immutable.fromJS({
    result: datasetVersion,
    entities: {
      fullDataset: {
        [datasetVersion]: {
          jobId: {
            id: jobId
          },
          paginationUrl
        }
      },
      datasetUI: {
        [datasetVersion]: {
          datasetVersion
        }
      }
    }
  });

  const fullDataset = successPayload.getIn(['entities', 'fullDataset', datasetVersion]);
  const datasetUI = successPayload.getIn(['entities', 'datasetUI', datasetVersion]);

  describe('handleResumeRunDataset', () => {
    beforeEach(() => {
      gen = handleResumeRunDataset(datasetVersion);
    });
    const customTest = testWithHooks({
      afterFn: () => {
        // check that generator is done and not empty view state is returned
        expect(next.done).to.be.true;
        // must be not empty to not break loadTableData saga
        expect(next.value).exist;
      }
    });
    customTest('should waitForRunToComplete if tableData is empty, paginationUrl and jobId exist', () => {
      next = gen.next();
      expect(next.value).to.eql(select(getEntities));
      next = gen.next(successPayload.get('entities'));
      expect(next.value).to.eql(call(
        waitForRunToComplete,
        datasetUI,
        fullDataset.get('paginationUrl'),
        jobId
      ));
      next = gen.next();
    });

    customTest('should waitForRunToComplete if tableData is not empty, but data reload is forced, paginationUrl and jobId exist', () => {
      gen = handleResumeRunDataset(datasetVersion, true);
      next = gen.next();
      expect(next.value).to.eql(select(getEntities));
      next = gen.next(successPayload.get('entities').setIn(['tableData', datasetVersion, 'rows'], []));
      expect(next.value).to.eql(call(
        waitForRunToComplete,
        datasetUI,
        fullDataset.get('paginationUrl'),
        jobId
      ));
      next = gen.next();
    });

    customTest('should not waitForRunToComplete if has rows', () => {
      next = gen.next();
      next = gen.next(successPayload.get('entities').setIn(['tableData', datasetVersion, 'rows'], []));
    });

    customTest('should not waitForRunToComplete if no jobId in location', () => {
      next = gen.next();
      next = gen.next(successPayload.get('entities').deleteIn(['fullDataset', datasetVersion, 'jobId']));
    });

    customTest('should not waitForRunToComplete if no paginationUrl', () => {
      next = gen.next();
      next = gen.next(successPayload.get('entities').deleteIn(['fullDataset', datasetVersion, 'paginationUrl']));
    });
  });

  describe('waitForRunToComplete', () => {
    it('should succeed', () => {
      gen = waitForRunToComplete(dataset, paginationUrl, jobId);
      next = gen.next();
      expect(next.value).to.eql(call([socket, socket.startListenToJobProgress], jobId, true));
      next = gen.next();
      expect(next.value).to.eql(put(updateViewState('run-' + jobId, {isInProgress: true})));
      next = gen.next();
      expect(next.value).to.eql(put(addNotification(Immutable.Map({code: 'WS_CLOSED'}), 'error')));
      next = gen.next();
      expect(next.value).to.be.eql(call(getExplorePageLocationChangePredicate));
      next = gen.next(() => false);
      expect(typeof next.value.RACE.jobDone).to.not.be.undefined;
      expect(typeof next.value.RACE.locationChange).to.not.be.undefined;
      next = gen.next({jobDone: {payload: {update: {state: true}}}});
      expect(next.value.PUT).to.not.be.undefined; // loadNextRows
      gen.next(); // yield promise of loadNextRows
      const lastAction = { payload: { somePayloadField: 'somePayloadField' } };
      next = gen.next(lastAction);
      expect(next.value).eql(call(getViewStateFromAction, lastAction));
      const resultViewState = { someProp: 'someProp' };
      next = gen.next(resultViewState);
      expect(next.value).to.eql(put(updateHistoryWithJobState(dataset, true)));
      next = gen.next();
      expect(next.value).to.eql(call([socket, socket.stopListenToJobProgress], jobId));
      next = gen.next();
      expect(next.value).to.eql(resultViewState);
    });
  });
});
