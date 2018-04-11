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
import Immutable from 'immutable';

import { updateViewState } from 'actions/resources';
import { updateHistoryWithJobState } from 'actions/explore/history';
import { addNotification } from 'actions/notification';

import {
  waitForRunToComplete,
  handleResumeRunDataset,
  getLocation,
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
    let location;
    beforeEach(() => {
      location = { query: { jobId }};
      gen = handleResumeRunDataset({ datasetId: datasetVersion });
    });
    it('should waitForRunToComplete if tableData is empty and paginationUrl exists', () => {
      next = gen.next();
      expect(next.value).to.eql(select(getEntities));
      next = gen.next(successPayload.get('entities'));
      expect(next.value).to.eql(select(getLocation));
      next = gen.next(location);  // waitForRunToComplete
      expect(next.value).to.eql(call(
        waitForRunToComplete,
        datasetUI,
        fullDataset.get('paginationUrl'),
        location.query.jobId
      ));
      next = gen.next();
      expect(next.done).to.be.true;
    });

    it('should not waitForRunToComplete if has data', () => {
      next = gen.next();
      next = gen.next(successPayload.get('entities').setIn(['tableData', datasetVersion, {data: []}]));
      next = gen.next(location);
      expect(next.done).to.be.true;
    });

    it('should not waitForRunToComplete if no jobId in location', () => {
      next = gen.next();
      next = gen.next(successPayload.get('entities'));
      next = gen.next({...location, query: {}});
      expect(next.done).to.be.true;
    });

    it('should not waitForRunToComplete if no paginationUrl', () => {
      next = gen.next();
      next = gen.next(successPayload.get('entities').deleteIn(['fullDataset', datasetVersion, 'paginationUrl']));
      expect(next.value).to.eql(select(getLocation));
      next = gen.next(location);
      expect(next.done).to.be.true;
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
      expect(typeof next.value.RACE.jobDone).to.not.be.undefined;
      expect(typeof next.value.RACE.locationChange).to.not.be.undefined;
      next = gen.next({jobDone: {payload: {update: {state: true}}}});
      expect(next.value.PUT).to.not.be.undefined; // loadNextRows
      gen.next(); // yield promise of loadNextRows
      next = gen.next();
      expect(next.value).to.eql(put(updateHistoryWithJobState(dataset, true)));
      next = gen.next();
      expect(next.value).to.eql(call([socket, socket.stopListenToJobProgress], jobId));
    });
  });
});
