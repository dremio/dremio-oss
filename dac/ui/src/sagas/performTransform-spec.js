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
import { call, put, spawn } from 'redux-saga/effects';
import Immutable from 'immutable';
import { runDataset, transformAndRunDataset } from 'actions/explore/dataset/run';
import { expandExploreSql } from 'actions/explore/ui';
import { newUntitledSql, newUntitledSqlAndRun } from 'actions/explore/dataset/new';
import { runTableTransform } from 'actions/explore/dataset/transform';
import { transformHistoryCheck } from 'sagas/transformHistoryCheck';

import apiUtils from 'utils/apiUtils/apiUtils';
import { transformThenNavigate } from './transformWatcher';
import { loadDataset, loadTableData, cancelDataLoad, focusSqlEditorSaga } from './performLoadDataset';

import {
  getTransformData,
  performTransform,
  handleRunDatasetSql,
  getFetchDatasetMetaAction,
  proceedWithDataLoad
} from './performTransform';

describe('performTransform saga', () => {
  let dataset;
  let transformData;
  let gen;
  let next;
  const viewId = 'VIEW_ID';
  const newDatasetVersion = 'newId';
  const oldDatasetVersion = 'oldId';
  const datasetResponse = {
    error: false,
    payload: Immutable.fromJS({
      entities: {
        datasetUI: {[newDatasetVersion]: { datasetVersion: newDatasetVersion }},
        fullDataset: {[newDatasetVersion]: {}}
      },
      result: newDatasetVersion
    })
  };

  beforeEach(() => {
    transformData = Immutable.fromJS({ href: '123' });
    dataset = Immutable.fromJS({
      displayFullPath: ['foo', 'bar'],
      datasetVersion: oldDatasetVersion,
      sql: 'select * from foo',
      context: ['fooContext']});
  });

  describe('performTransform', () => {
    const gotToCallback = () => {
      const apiAction = 'an api action';
      next = gen.next(); // getFetchDatasetMetaAction call
      next = gen.next({ apiAction }); // cancelDataLoad call
      expect(next.value).to.be.eql(call(cancelDataLoad));
      next = gen.next(); // transformThenNavigate call
      expect(next.value).to.be.eql(call(transformThenNavigate, apiAction, undefined, undefined));
      next = gen.next(datasetResponse); // loadTableData call
      // should take a version from reponse here
      expect(next.value).to.be.eql(spawn(loadTableData, newDatasetVersion, undefined));
      next = gen.next(); // focusSqlEditorSaga
      expect(next.value).to.be.eql(call(focusSqlEditorSaga));
      next = gen.next(); // callback call
    };

    it('should call callback with true if transform request succeeds', () => {
      const params = { dataset, currentSql: dataset.get('sql'), transformData, callback: sinon.spy() };

      gen = performTransform(params);

      gotToCallback();

      const nextDataset = apiUtils.getEntityFromResponse('datasetUI', datasetResponse);
      expect(next.value.CALL.fn).to.equal(params.callback);
      expect(next.value.CALL.args).to.eql([true, nextDataset]);
      next = gen.next();
      expect(next.done).to.be.true;
    });

    it('should call callback with false if no transform necessary', () => {
      const params = { dataset, currentSql: dataset.get('sql'), callback: sinon.spy() };

      gen = performTransform(params);
      next = gen.next(); // getFetchDatasetMetaAction call
      next = gen.next({ apiAction: null }); // callback call

      expect(next.value).to.eql(call(params.callback, false, dataset));
      next = gen.next();
      expect(next.done).to.be.true;
    });

    it('should expand sql if original dataset isNewQuery', () => {
      const payload = {
        dataset: Immutable.fromJS({ isNewQuery: true }),
        currentSql: dataset.get('sql')
      };

      gen = performTransform(payload);
      gotToCallback();
      expect(next.value).to.be.eql(put(expandExploreSql()));
      next = gen.next();
      expect(next.done).to.be.true;
    });
  });

  describe('handleRunDatasetSql', () => {
    let dataSet, exploreViewState, exploreState;
    beforeEach(() => {
      dataSet = Immutable.fromJS({datasetVersion: 1, sql: ''});
      exploreViewState = Immutable.fromJS({viewId: 'a'});
      exploreState = { view: { currentSql: '', queryContext: [] } };
    });

    it('should do nothing if proceedWithDataLoad yields false', () => {
      gen = handleRunDatasetSql({});
      next = gen.next();
      next = gen.next(dataSet);
      next = gen.next(exploreViewState);
      next = gen.next(exploreState);

      expect(next.value).to.be.eql(call(proceedWithDataLoad, dataSet, exploreState.view.queryContext,
        exploreState.view.currentSql));
      expect(gen.next(false).done).to.be.true;
    });

    it('should call perform transform with proper run parameters', () => {
      // needsTransform inside handleRunDatasetSql should return false with default items set in beforeEach
      const performTransformParam = {
        dataset: dataSet,
        currentSql: exploreState.view.currentSql,
        queryContext: exploreState.view.queryContext,
        viewId: exploreViewState.get('viewId'),
        isRun: true
      };
      gen = handleRunDatasetSql({});
      next = gen.next(); // yield dataset
      next = gen.next(dataSet); // use dataset, yield exploreViewState
      next = gen.next(exploreViewState); // use exploreViewState, yield exploreState
      next = gen.next(exploreState); // use exploreState, yield call performTransform
      next = gen.next(true); // result for proceedWithDataLoad
      expect(next.value).to.be.eql(call(performTransform, performTransformParam));
    });

    it('should call perform transform with proper preview parameters', () => {
      // needsTransform inside handleRunDatasetSql should return false with default items set in beforeEach
      const performTransformParam = {
        dataset: dataSet,
        currentSql: exploreState.view.currentSql,
        queryContext: exploreState.view.queryContext,
        viewId: exploreViewState.get('viewId'),
        forceDataLoad: true
      };
      gen = handleRunDatasetSql({ isPreview: true });
      next = gen.next(); // yield dataset
      next = gen.next(dataSet); // use dataset, yield exploreViewState
      next = gen.next(exploreViewState); // use exploreViewState, yield exploreState
      next = gen.next(exploreState); // use exploreState, yield call performTransform
      next = gen.next(true); // result for proceedWithDataLoad
      expect(next.value).to.be.eql(call(performTransform, performTransformParam));
    });
  });

  describe('getFetchDatasetMetaAction', () => {
    const currentSql = 'select * from foo';
    const queryContext = Immutable.fromJS(['@dremio']);

    it('should get sql from dataset if currentSql arg is falsy', () => {
      const payload = {
        dataset,
        currentSql: undefined
      };

      gen = getFetchDatasetMetaAction(payload);
      next = gen.next();
      expect(next.value).to.eql(call(getTransformData, payload.dataset, 'select * from foo', undefined, undefined));
    });

    const goToTransformData = transformDataResult => {
      next = gen.next(); // skip getTransformData
      next = gen.next(transformDataResult);
    };

    describe('Run dataset case', () => {

      it('should do newUntitledSqlAndRun if no dataset version', () => {
        gen = getFetchDatasetMetaAction({
          isRun: true,
          dataset: dataset.remove('datasetVersion'),
          currentSql,
          queryContext,
          viewId
        });
        goToTransformData();
        expect(next.value).to.be.eql(call(newUntitledSqlAndRun, currentSql, queryContext, viewId));

        const mockApiAction = 'mock api call';
        next = gen.next(mockApiAction);
        expect(next.value).to.be.eql({
          apiAction: mockApiAction,
          navigateOptions: { changePathname: true } //changePathname to navigate to newUntitled
        });
      });

      it('should do transformAndRun if transformData', () => {
        gen = getFetchDatasetMetaAction({
          isRun: true,
          dataset,
          currentSql,
          queryContext,
          viewId
        });
        goToTransformData(transformData);
        expect(next.value).to.be.eql(call(transformAndRunDataset, dataset, transformData, viewId));

        const mockApiAction = 'mock api call';
        next = gen.next(mockApiAction);
        expect(next.value).to.be.eql({
          apiAction: mockApiAction,
          navigateOptions: undefined
        });
      });

      it('should do runDataset if neither of the above are true', () => {
        gen = getFetchDatasetMetaAction({
          isRun: true,
          dataset,
          currentSql,
          queryContext,
          viewId
        });
        goToTransformData();
        expect(next.value).to.be.eql(call(runDataset, dataset, viewId));

        const mockApiAction = 'mock api call';
        next = gen.next(mockApiAction);
        expect(next.value).to.be.eql({
          apiAction: mockApiAction,
          navigateOptions: { replaceNav: true, preserveTip: true }
        });
      });

    });

    describe('Preview dataset case', () => {

      it('should call newUntitledSql if dataset has not been created', () => {
        gen = getFetchDatasetMetaAction({
          isRun: false,
          dataset: dataset.remove('datasetVersion'),
          currentSql,
          queryContext,
          viewId
        });
        goToTransformData();
        expect(next.value).to.be.eql(call(newUntitledSql, currentSql, queryContext.toJS(), viewId));

        const mockApiAction = 'mock api call';
        next = gen.next(mockApiAction);
        expect(next.value).to.be.eql({
          apiAction: mockApiAction,
          navigateOptions: { changePathname: true } //changePathname to navigate to newUntitled
        });
      });

      it('should call runTableTransform if transform data is provided', () => {
        gen = getFetchDatasetMetaAction({
          isRun: false,
          dataset,
          currentSql,
          queryContext,
          viewId
        });
        goToTransformData(transformData);
        expect(next.value).to.be.eql(call(runTableTransform, dataset, transformData, viewId, undefined));

        const mockApiAction = 'mock api call';
        next = gen.next(mockApiAction);
        expect(next.value).to.be.eql({
          apiAction: mockApiAction,
          navigateOptions: undefined
        });
      });

      it('should call loadDataset if it is existent dataset without transformation is passed as argument along with forceDataLoad = true', () => {
        gen = getFetchDatasetMetaAction({
          isRun: false,
          dataset,
          currentSql,
          queryContext,
          viewId,
          forceDataLoad: true
        });
        goToTransformData();
        expect(next.value).to.be.eql(call(loadDataset, dataset, viewId));

        const mockApiAction = 'mock api call';
        next = gen.next(mockApiAction);
        expect(next.value).to.be.eql({
          apiAction: mockApiAction,
          navigateOptions: { replaceNav: true, preserveTip: true }
        });
      });
    });
  });

  describe('getTransformData', () => {
    const sql = 'select * from foo';
    const context = Immutable.List(['@dremio']);
    it('should return supplied transformData if present', () => {
      expect(
        getTransformData(dataset, sql, context, transformData)
      ).to.equal(transformData);
    });

    it('should return undefined if dataset isNewQuery and no transformData supplied', () => {
      expect(
        getTransformData(dataset.set('isNewQuery', true), sql, context)
      ).to.be.undefined;
    });

    it('should return updateSQL only if dataset is not new, no transformData supplied, and sql has changed', () => {
      expect(
        getTransformData(dataset, sql, context)
      ).to.be.eql({type: 'updateSQL', sql, sqlContextList: context.toJS()});

      expect(getTransformData(dataset.set('isNewQuery', true), sql, context)).to.be.undefined;
      expect(getTransformData(dataset, sql, context, transformData)).to.equal(transformData);
      expect(getTransformData(dataset, null, dataset.get('context'))).to.be.undefined;
      expect(getTransformData(dataset, dataset.get('sql'), dataset.get('context'))).to.be.undefined;
    });

    it('should not fail if dataset is missing context', () => {
      expect(
        getTransformData(dataset.remove('context'), sql, context)
      ).to.be.eql({type: 'updateSQL', sql, sqlContextList: context.toJS()});
    });
  });

  describe('proceedWithDataLoad', () => {
    it('returns true if sql and context is not changed', () => {
      gen = proceedWithDataLoad(dataset, dataset.get('context'), dataset.get('sql'));
      expect(gen.next()).to.be.eql({
        done: true,
        value: true
      });
    });
    const testTransformHistoryCheck = resultValue => it(`return ${resultValue}, if transformHistoryCheck returns ${resultValue}`, () => {
      gen = proceedWithDataLoad(dataset, dataset.get('context'), 'select * from other_query');
      expect(gen.next().value).to.be.eql(call(transformHistoryCheck, dataset));
      expect(gen.next(resultValue)).to.be.eql({
        done: true,
        value: resultValue
      });
    });
    [true, false].map(testTransformHistoryCheck);
  });
});
