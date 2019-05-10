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
import { put, select, call, race, take } from 'redux-saga/effects';
import { goBack } from 'react-router-redux';

import { getLocation } from 'selectors/routing';
import { updateViewState } from 'actions/resources';
import { handleResumeRunDataset, DataLoadError } from 'sagas/runDataset';
import { loadExistingDataset } from 'actions/explore/dataset/edit';
import { getFullDataset } from '@app/selectors/explore';
import { newUntitled } from 'actions/explore/dataset/new';
import { EXPLORE_TABLE_ID } from 'reducers/explore/view';
import { focusSqlEditor } from '@app/actions/explore/view';
import { getViewStateFromAction } from '@app/reducers/resources/view';
import { TRANSFORM_PEEK_START } from '@app/actions/explore/dataset/peek';
import {
  loadTableData, CANCEL_TABLE_DATA_LOAD, hideTableSpinner, cancelDataLoad,
  resetTableViewStateOnPageLeave, focusSqlEditorSaga
} from './performLoadDataset';

import {
  transformThenNavigate,
  TransformCanceledError,
  TransformCanceledByLocationChangeError,
  TransformFailedError
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
      next = gen.next(); // loadDataset call
      expect(next.value).to.eql(call(loadDataset, dataset, viewId));
      const apiAction = 'an api action';
      next = gen.next(apiAction); // transformThenNavigate call
      // try to load a data and navigate to a received version
      expect(next.value).to.be.eql(call(transformThenNavigate, apiAction, viewId, {
        replaceNav: true,
        preserveTip: true
      }));
    });

    it('positive flow for case, when dataset metadata is loaded successfully', () => {
      const loadDatasetResponse = {
        error: false,
        payload: datasetResponsePayload
      };
      next = gen.next(loadDatasetResponse);
      expect(next.value).to.be.eql(call(focusSqlEditorSaga));
      next = gen.next();
      expect(next.value).to.eql(call(loadTableData, datasetVersion));
      next = gen.next();
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
      expect(next.value).to.be.eql(call(focusSqlEditorSaga));
      next = gen.next();
      // no navigate because dataset has version
      expect(next.value).to.eql(put(updateViewState(viewId, {isFailed: true, error: {message: errorMessage}})));
      next = gen.next();
      expect(next.done).to.be.true;
    });

    describe('exceptions', () => {
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

      it('should not goBack if data load is failed', () => {
        next = gen.throw(new DataLoadError());
        expect(next.done).to.be.true;
      });

      it('should not goBack if transformation is failed', () => {
        next = gen.throw(new TransformFailedError());
        expect(next.done).to.be.true;
      });
    });
  });

  describe('loadDataset', () => {
    function shouldWatchApiAction(theLocation, theDataset, apiPutEffect) {
      gen = loadDataset(theDataset, viewId);
      next = gen.next(); // getLocation call
      expect(next.value).to.eql(select(getLocation));
      next = gen.next(theLocation); // one of the call to api
      expect(next.value).to.be.eql(apiPutEffect);
      const apiAction = 'api action';
      next = gen.next(apiAction);
      expect(next.value).to.eql(apiAction); //  apiAction is returned
      expect(next.done).to.be.true;
    }

    it('should return a loadExistingDataset api action if mode is edit, or if dataset has a version', () => {

      const datasetWithoutVersion = dataset.remove('datasetVersion');
      shouldWatchApiAction(
        {...location, query: {...location.query, mode: 'edit'}},
        datasetWithoutVersion,
        call(loadExistingDataset, datasetWithoutVersion, viewId, location.query.tipVersion)
      );
      shouldWatchApiAction(location, dataset, call(loadExistingDataset, dataset, viewId,
        location.query.tipVersion));
    });

    it('should performWatchedTransform if neither of the above are true', () => {
      const datasetWithoutVersion = dataset.remove('datasetVersion');
      shouldWatchApiAction(
        location,
        datasetWithoutVersion,
        call(newUntitled, datasetWithoutVersion, 'foo.path.to.dataset', viewId)
      );
    });

  });

  describe('loadTableData', () => {
    let loadTableDataGen;
    const forceLoad = false;
    const paginationUrl = 'a pagination url';
    const jobId = 'a job id';

    beforeEach(() => {
      // common generator flow
      loadTableDataGen = loadTableData(datasetVersion, forceLoad);
      next = loadTableDataGen.next();
      // cancelation of previous calls
      expect(next.value).to.be.eql(call(cancelDataLoad));
      // check that full dataset is presented in redux store
      next = loadTableDataGen.next();
      expect(next.value).to.be.eql(select(getFullDataset, datasetVersion));
      const validDataset = Immutable.fromJS({
        paginationUrl,
        jobId: { id: jobId }
      });

      next = loadTableDataGen.next(validDataset); //update viewstate
    });

    afterEach(() => {
      // check that generator is done
      next = loadTableDataGen.next();
      expect(next.done).to.be.true;
    });


    describe('positive flow', () => {
      beforeEach(() => {
        next = loadTableDataGen.next();
        expect(next.value).to.be.eql(race({
          // data load saga was submitted
          dataLoaded: call(handleResumeRunDataset, datasetVersion, jobId, forceLoad, paginationUrl),
          // listener for cancelation action is added
          isLoadCanceled: take([CANCEL_TABLE_DATA_LOAD, TRANSFORM_PEEK_START]),
          // load task would be canceled and view state would be reset if page for a current dataset is left
          locationChange: call(resetTableViewStateOnPageLeave)
        }));
      });

      ['dataLoaded', 'isLoadCanceled', 'locationChange'].map(raceKey => {
        it(`load mask is hidden if data loading if ${raceKey} wins a race`, () => {
          // data is loaded, so hide a mask
          next = loadTableDataGen.next({ [raceKey]: {} });
          expect(next.value).to.be.eql(call(hideTableSpinner));
        });
      });
    });

    describe('negative flow', () => {
      it('An exception is re-thrown if it is no DataLoadError', () => {
        // race effect throws an error
        const error = new Error('test');
        next = loadTableDataGen.throw(error);
        expect(() => {
          next = loadTableDataGen.next();
        }).throw(error);
      });


      it('View state is updated with error in case of DataLoadError', () => {
        // race effect throws an error
        const error = new DataLoadError('test');
        next = loadTableDataGen.throw(error);
        // calculate a view state
        expect(next.value).to.be.eql(call(getViewStateFromAction, error.response));
        // update a view state with error
        const viewState = 'view state';
        next = loadTableDataGen.next(viewState);
        expect(next.value).to.be.eql(put(updateViewState(EXPLORE_TABLE_ID, viewState)));
      });
    });
  });

  it('focusSqlEditorSaga should put focusSqlEditor() action', () => {
    gen = focusSqlEditorSaga();
    next = gen.next();
    expect(next.value).to.be.eql(put(focusSqlEditor()));
    next = gen.next();
    expect(next.done).to.be.true;
  });
});
