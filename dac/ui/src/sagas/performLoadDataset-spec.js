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
import { put, select, call, race, take, fork } from 'redux-saga/effects';
import { goBack } from 'react-router-redux';

import { getLocation } from 'selectors/routing';
import { updateViewState } from 'actions/resources';
import { handleResumeRunDataset } from 'sagas/runDataset';
import { EXPLORE_TABLE_ID } from 'reducers/explore/view';
import { loadTableData, CANCEL_TABLE_DATA_LOAD, resetTableViewStateOnPageLeave } from './performLoadDataset';

import {
  performWatchedTransform,
  TransformCanceledError,
  TransformCanceledByLocationChangeError
} from './transformWatcher';

import { handlePerformLoadDataset, loadDataset } from './performLoadDataset';

describe('performLoadDataset saga', () => {

  const datasetVersion = '123';
  const dataset = Immutable.fromJS({
    datasetVersion
  });
  const viewId = 'VIEW_ID';
  let location;
  let gen;
  let next;

  beforeEach(() => {
    location = {
      pathname: '/source/foo/path.to.dataset',
      query: {tipVersion: 'abc'}
    };
  });

  const datasetResponsePayload = Immutable.fromJS({
    entities: {
      fullDataset: {
        [datasetVersion]: {
          version: datasetVersion
        }
      }
    },
    result: datasetVersion
  });

  describe('handlePerformLoadDataset', () => {

    beforeEach(() => {
      gen = handlePerformLoadDataset({meta: {dataset, viewId}});
      next = gen.next();
      expect(next.value).to.eql(call(loadDataset, dataset, viewId));
    });

    it('should call loadDataset, then navigate if success and !dataset.datasetVersion', () => {
      gen = handlePerformLoadDataset({meta: {dataset: dataset.delete('datasetVersion')}});
      next = gen.next();
      next = gen.next({error: false, payload: Immutable.Map()});
      expect(next.value.PUT).to.not.be.undefined;
      next = gen.next();
      expect(next.done).to.be.true;
    });

    it('should not navigate if response.error', () => {
      next = gen.next({error: true});
      expect(next.done).to.be.true;
    });

    it('should not navigate if dataset has version', () => {
      const loadDatasetResponse = {
        error: false,
        payload: datasetResponsePayload
      };
      next = gen.next(loadDatasetResponse);
      expect(next.value).to.eql(call(loadTableData, datasetVersion));
      next = gen.next();
      expect(next.done).to.be.true;
    });


    it('should goBack if transform canceled', () => {
      next = gen.throw(new TransformCanceledError());
      expect(next.value).to.eql(put(goBack()));
      next = gen.next();
      expect(next.done).to.be.true;
    });

    it('should not goBack if transform canceled by location change', () => {
      next = gen.throw(new TransformCanceledByLocationChangeError());
      expect(next.done).to.be.true;
    });

    it('should set error in viewState if there is an error in the dataset', () => {
      const errorMessage = 'Dataset Error';
      next = gen.next({
        payload: Immutable.fromJS({
          entities: {
            fullDataset: {
              '123': {
                error: {
                  errorMessage
                }
              }
            }
          },
          result: '123'
        })});
      // no navigate because dataset has version
      expect(next.value).to.eql(put(updateViewState(viewId, {isFailed: true, error: {message: errorMessage}})));
      next = gen.next();
      expect(next.done).to.be.true;
    });
  });

  describe('loadDataset', () => {
    it('should put loadExistingDataset and return response if mode is edit, or if dataset has a version', () => {

      function shouldLoadExistingDataset(theLocation, theDataset) {
        gen = loadDataset(theDataset, viewId);
        next = gen.next();
        expect(next.value).to.eql(select(getLocation));
        next = gen.next(theLocation);
        expect(next.value.PUT).to.not.be.undefined; // loadExistingDataset
        const promise = Promise.resolve();
        next = gen.next(promise);
        expect(next.value).to.equal(promise);
        next = gen.next();
        expect(next.done).to.be.true;
      }

      shouldLoadExistingDataset(
        {...location, query: {...location.query, mode: 'edit'}},
        dataset.remove('datasetVersion')
      );
      shouldLoadExistingDataset(location, dataset);
    });

    it('should performWatchedTransform if neither of the above are true', () => {
      gen = loadDataset(dataset.remove('datasetVersion'), viewId);
      next = gen.next();
      expect(next.value).to.eql(select(getLocation));
      next = gen.next(location);
      expect(next.value.CALL.fn).to.equal(performWatchedTransform);
      next = gen.next();
      expect(next.done).to.be.true;
    });

  });

  describe('loadTableData', () => {
    let loadTableDataGen;

    beforeEach(() => {
      // common generator flow
      const forceLoad = false;
      loadTableDataGen = loadTableData(datasetVersion, forceLoad);
      next = loadTableDataGen.next();
      // cancelation of previous calls
      expect(next.value).to.be.eql(put({ type: CANCEL_TABLE_DATA_LOAD }));
      loadTableDataGen.next(); //update viewstate
      next = loadTableDataGen.next(); // race
      expect(next.value).to.be.eql(race({
        // data load saga was submitted
        loadRequestViewState: call(handleResumeRunDataset, datasetVersion, forceLoad),
        // listener for cancelation action is added
        isLoadCanceled: take(CANCEL_TABLE_DATA_LOAD),
        // load task would be canceled and view state would be reset if page for a current dataset is left
        locationChange: call(resetTableViewStateOnPageLeave)
      }));
    });
    afterEach(() => {
      // perform a last step and check that a generator is done
      next = loadTableDataGen.next();
      expect(next.done).to.be.true;
    });

    it('load mask is hidden if data loading is completed', () => {
      const viewState = {};
      next = loadTableDataGen.next({ loadRequestViewState: viewState });
      // reset load mask or show an error depending on result view state
      expect(next.value).to.be.eql(put(updateViewState(EXPLORE_TABLE_ID, viewState)));
      next = loadTableDataGen.next();
      // put a non-blocking listener for page leave to reset a view state.
      // it is useful in case if previous dataset had error
      expect(next.value).to.be.eql(fork(resetTableViewStateOnPageLeave));
    });

    it('no actions if load is canceled', () => {
      // simulate that load cancel action win a race
      next = loadTableDataGen.next({ isLoadCanceled: {} });
    });

    it('no actions if page leave event accured', () => {
      // simulate that page leave action win a race
      next = loadTableDataGen.next({ locationChange: {} });
    });
  });
});
