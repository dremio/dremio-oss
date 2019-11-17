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
import { put, call, select, take, race } from 'redux-saga/effects';
import socket, { WS_CONNECTION_OPEN } from 'utils/socket';
import { testWithHooks } from 'testUtil';
import Immutable from 'immutable';

import { updateHistoryWithJobState } from 'actions/explore/history';
import { updateExploreJobProgress } from 'actions/explore/dataset/data';
import { LOGOUT_USER_SUCCESS } from '@app/actions/account';
import { getTableDataRaw } from '@app/selectors/explore';

import {
  waitForRunToComplete,
  handleResumeRunDataset,
  DataLoadError,
  explorePageChanged
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

  describe('handleResumeRunDataset', () => {
    beforeEach(() => {
      gen = handleResumeRunDataset(datasetVersion, jobId, false, paginationUrl);
    });
    const customTest = testWithHooks({
      afterFn: () => {
        // check that generator is done and not empty view state is returned
        expect(next.done).to.be.true;
      }
    });
    customTest('should waitForRunToComplete if tableData.rows is not presented', () => {
      // get table data
      next = gen.next();
      expect(next.value).to.eql(select(getTableDataRaw, datasetVersion));
      next = gen.next(Immutable.fromJS({ rows: null }));
      expect(next.value).to.eql(race({
        jobDone: call(
          waitForRunToComplete,
          datasetVersion,
          paginationUrl,
          jobId
        ),
        locationChange: call(explorePageChanged)
      }));
      next = gen.next();
    });

    customTest('should waitForRunToComplete if tableData.rows is presented, but data reload is forced', () => {
      gen = handleResumeRunDataset(datasetVersion, jobId, true, paginationUrl);
      // get table data
      next = gen.next();
      expect(next.value).to.eql(select(getTableDataRaw, datasetVersion));
      next = gen.next(Immutable.fromJS({ rows: [] }));
      expect(next.value).to.eql(race({
        jobDone: call(
          waitForRunToComplete,
          datasetVersion,
          paginationUrl,
          jobId
        ),
        locationChange: call(explorePageChanged)
      }));
      next = gen.next();
    });

    customTest('should not waitForRunToComplete if has rows', () => {
      next = gen.next();
      next = gen.next(Immutable.fromJS({ rows: [] }));
    });
  });

  describe('waitForRunToComplete', () => {

    const goToResponse = () => {
      gen = waitForRunToComplete(dataset, paginationUrl, jobId);
      // wait for socket to be open
      expect(gen.next().value).to.be.eql(race({
        socketOpen: take(WS_CONNECTION_OPEN),
        stop: take(LOGOUT_USER_SUCCESS)
      }));
      const socketOpenRaceResult = {
        socketOpen: true
      };
      // register web socket listener
      next = gen.next(socketOpenRaceResult);
      expect(next.value).to.eql(call([socket, socket.startListenToJobProgress], jobId, true));
      // race between jobCompletion listener and location change listener
      next = gen.next();
      expect(typeof next.value.RACE.jobDone).to.not.be.undefined;
      expect(typeof next.value.RACE.locationChange).to.not.be.undefined;
      // initiate data loading
      next = gen.next({jobDone: {payload: {update: {state: true}}}});
      expect(next.value.PUT).to.not.be.undefined; // loadNextRows
      gen.next();
    };

    const checkFinallyBlock = (response) => {
      // remove job listener
      next = gen.next(response);
      expect(next.value).to.eql(call([socket, socket.stopListenToJobProgress], jobId));
      next = gen.next();
      expect(next.done).to.be.true;
    };

    it('should succeed', () => {
      goToResponse();
      // change history item status to completed
      next = gen.next({ someProp: 'someProp' });
      expect(next.value).to.eql(put(updateHistoryWithJobState(dataset, true)));
      next = gen.next();
      expect(next.value).to.eql(put(updateExploreJobProgress({state: true})));
      checkFinallyBlock();
    });

    it('should throw an exception if a response is empty or has an error', () => {
      [null, { error: true }].map(response => {
        goToResponse();
        // throw an exceotion
        expect(() => {
          checkFinallyBlock(response);
        }).to.throw(DataLoadError);
      });
    });
  });

  describe('DataLoadError', () => {
    it('instanceof works for DataLoadError', () => {
      //it is important that new DataLoadError() instanceof DataLoadError was true
      // with babel that may not work if we try to inherit DataLoadError from Error class
      expect(new DataLoadError() instanceof DataLoadError).to.be.true;

    });
  });
});
