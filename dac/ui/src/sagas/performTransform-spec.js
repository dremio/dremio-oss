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
import { call, put } from 'redux-saga/effects';

import { CALL_API } from 'redux-api-middleware';

import { RUN_DATASET_START } from 'actions/explore/dataset/run';
import { expandExploreSql } from 'actions/explore/ui';

import { getNewDataset } from 'pages/ExplorePage/ExplorePageController';
import apiUtils from 'utils/apiUtils/apiUtils';

import { unwrapAction } from './utils';
import { performWatchedTransform } from './transformWatcher';
import { loadDataset, loadTableData } from './performLoadDataset';

import {
  getTransformData,
  performTransform,
  doRun,
  createDataset,
  transformThenNavigate,
  TransformFailedError
} from './performTransform';

describe('performTransform saga', () => {
  let dataset;
  let transformData;
  let gen;
  let next;
  const viewId = 'VIEW_ID';
  const version = 'fooId';
  const datasetResponse = {
    error: false,
    payload: Immutable.fromJS({
      entities: {
        datasetUI: {[version]: 'dataset'},
        fullDataset: {[version]: {}}
      },
      result: version
    })
  };

  beforeEach(() => {
    transformData = Immutable.fromJS({ href: '123' });
    dataset = Immutable.fromJS({
      displayFullPath: ['foo', 'bar'],
      datasetVersion: 1,
      sql: 'select * from foo',
      context: ['fooContext']});
  });

  describe('performTransform', () => {
    it('should call doRun when isRun', () => {
      const params = { dataset, currentSql: dataset.get('sql'), transformData, isRun: true };

      gen = performTransform(params);
      next = gen.next();
      expect(next.value).to.eql(call(getTransformData, dataset, 'select * from foo', undefined, transformData));
      next = gen.next(transformData);
      expect(next.value.CALL.fn).to.equal(doRun);
    });

    it('should call callback with true if transform request succeeds', () => {
      const params = { dataset, currentSql: dataset.get('sql'), transformData, callback: sinon.spy() };

      gen = performTransform(params);
      next = gen.next();
      next = gen.next(transformData);
      expect(next.value.CALL).to.not.be.undefined; // run transform
      next = gen.next(datasetResponse);
      const nextDataset = apiUtils.getEntityFromResponse('datasetUI', datasetResponse);
      expect(next.value.CALL.fn).to.equal(params.callback);
      expect(next.value.CALL.args).to.eql([true, nextDataset]);
      next = gen.next();
      expect(next.done).to.be.true;
    });

    it('should call callback with false if no transform necessary', () => {
      const params = { dataset, currentSql: dataset.get('sql'), callback: sinon.spy() };

      gen = performTransform(params);
      next = gen.next();
      next = gen.next(null); // no transformData
      expect(next.value.CALL.fn).to.equal(params.callback);
      expect(next.value.CALL.args).to.eql([false, dataset]);
      next = gen.next();
      expect(next.done).to.be.true;
    });


    it('should not call callback if transform request fails', () => {
      const payload = { dataset, currentSql: dataset.get('sql'), transformData, callback: sinon.spy() };

      gen = performTransform(payload);
      next = gen.next();
      next = gen.next(transformData);
      expect(next.value.CALL).to.not.be.undefined; // run transform
      next = gen.next({error: true});
      expect(next.done).to.be.true;
    });

    it('should expand sql if original dataset isNewQuery', () => {
      const payload = {
        dataset: getNewDataset({query: {context: '@dremio'}}),
        currentSql: dataset.get('sql')
      };

      gen = performTransform(payload);
      next = gen.next();
      next = gen.next(null); // transformData
      next = gen.next(dataset); // createDataset
      expect(next.value).to.eql(put(expandExploreSql()));
      next = gen.next();
      expect(next.done).to.be.true;
    });

    it('should get sql from dataset if currentSql arg is falsy', () => {
      const payload = {
        dataset
      };

      gen = performTransform(payload);
      next = gen.next();
      expect(next.value).to.eql(call(getTransformData, payload.dataset, 'select * from foo', undefined, undefined));
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
      expect(getTransformData(dataset, undefined, dataset.get('context'))).to.be.undefined;
      expect(getTransformData(dataset, dataset.get('sql'), dataset.get('context'))).to.be.undefined;
    });

    it('should not fail if dataset is missing context', () => {
      expect(
        getTransformData(dataset.remove('context'), sql, context)
      ).to.be.eql({type: 'updateSQL', sql, sqlContextList: context.toJS()});
    });
  });

  describe('doRun', () => {
    const currentSql = 'select * from foo';
    const queryContext = ['@dremio'];
    it('should do newUntitledSqlAndRun if no dataset version', () => {
      gen = doRun(dataset.remove('datasetVersion'), currentSql, queryContext, undefined, viewId);
      next = gen.next();
      expect(next.value.CALL.fn).to.eql(transformThenNavigate);
      const action = unwrapAction(next.value.CALL.args[0]);
      expect(action[CALL_API].endpoint).to.contain('/datasets/new_untitled_sql_and_run');
    });

    it('should do transformAndRun if transformData', () => {
      gen = doRun(dataset, currentSql, queryContext, transformData, viewId);
      next = gen.next();
      expect(next.value.CALL.fn).to.eql(transformThenNavigate);
      const action = unwrapAction(next.value.CALL.args[0]);
      expect(action[CALL_API].endpoint).to.contain('transformAndRun');
    });

    it('should do runDataset if neither of the above are true', () => {
      gen = doRun(dataset, currentSql, queryContext, undefined, viewId);
      next = gen.next();
      const action = unwrapAction(next.value.CALL.args[0]);
      expect(action[CALL_API].types[0].type).to.equal(RUN_DATASET_START);
    });

    it('should loadTableData only if initial request is successful', () => {
      gen = doRun(dataset, currentSql, queryContext, undefined, viewId);
      next = gen.next();
      next = gen.next(datasetResponse);
      //should call loadTableData with forceReload = true
      expect(next.value).to.eql(call(loadTableData, version, true));
      next = gen.next();
      expect(next.done).to.be.true;

      // error case
      gen = doRun(dataset, currentSql, queryContext, undefined, viewId);
      next = gen.next();
      next = gen.next({error: true});
      expect(next.done).to.be.true;
    });
  });

  describe('createDataset', () => {
    const sql = 'sql';
    const context = Immutable.List(['context']);

    it(`should call performWatchedTransform with newUntitledSql if dataset has not been created,
        and return created dataset`, () => {
      gen = createDataset(dataset.delete('datasetVersion'), sql, context);
      next = gen.next();
      expect(next.value.CALL).to.not.be.undefined; // call(transformThenNavigate, newUntitledSql)
      const datasetVersion = 'someId';
      const payload = Immutable.fromJS({
        entities: {
          datasetUI: {
            [datasetVersion]: {
              datasetVersion
            }
          }
        },
        result: datasetVersion
      });
      next = gen.next({
        payload
      });
      expect(next.done).to.be.true;
      expect(next.value).to.equal(payload.getIn(['entities', 'datasetUI', datasetVersion]));
    });

    it('should call loadDataset if dataset.needsLoad', () => {
      gen = createDataset(dataset.set('needsLoad', true), sql, context);
      next = gen.next();
      expect(next.value.CALL.fn).to.equal(loadDataset);
      const promise = Promise.resolve;
      next = gen.next(promise);
      next = gen.next(datasetResponse);
      expect(next.value.PUT).to.not.be.undefined; // navigateToNextDataset
      next = gen.next();
      expect(next.done).to.be.true;
    });

    it('should throw error if newUntitledSql failed', () => {
      gen = createDataset(dataset.delete('datasetVersion'), sql, context);
      next = gen.next();
      expect(() => {
        next = gen.next({error: true});
      }).to.throw(TransformFailedError);
    });
  });

  describe('transformThenNavigate', () => {
    it('should performWatchedTransform, then navigate, and return response', () => {
      gen = transformThenNavigate('action', 'viewId');
      next = gen.next();
      expect(next.value).to.eql(call(performWatchedTransform, 'action', 'viewId'));
      const response = {
        payload: Immutable.fromJS({})
      };
      next = gen.next(response);
      expect(next.value.PUT).to.not.be.undefined; // navigateToNextDataset
      next = gen.next();
      expect(next.value).to.equal(response);
    });

    it('should throw if response.error', () => {
      gen = transformThenNavigate('action', 'viewId');
      next = gen.next();
      expect(next.value).to.eql(call(performWatchedTransform, 'action', 'viewId'));
      const response = {
        error: true
      };
      expect(() => {
        next = gen.next(response);
      }).to.throw(TransformFailedError);
    });

  });
});
