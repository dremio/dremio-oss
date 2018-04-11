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
import { push } from 'react-router-redux';

import * as Actions from './common';

const datasetVersion = '123';
const datasetLink = `/dataset/foo?version=${datasetVersion}`;

const responsePayload = Immutable.fromJS({
  entities: {
    fullDataset: {
      [datasetVersion]: {

      }
    },
    datasetUI: {
      [datasetVersion]: {
        datasetVersion,
        links: {
          self: datasetLink
        }
      }
    }
  },
  result: datasetVersion
});

describe('common', () => {

  describe('navigateToNextDataset', () => {

    let getStore;
    let location;
    beforeEach(() => {
      location = {
        query: {tipVersion: 'tip123'},
        state: {},
        pathname: ''
      };
      getStore = () => ({
        routing: {locationBeforeTransitions: location}
      });
    });

    it('should return push action', () => {
      const result = Actions.navigateToNextDataset({payload: responsePayload})(obj => obj, getStore);
      expect(result).to.eql(push({
        pathname: datasetLink.split('?')[0],
        query: {
          version: datasetVersion,
          tipVersion: '123'
        },
        state: {}
      }));
    });

    it('should return push action with pathname to graph', () => {
      const getStoreLocal = () => ({
        routing: {locationBeforeTransitions: {
          state: {},
          pathname: 'foo/graph'
        }}
      });
      const result = Actions.navigateToNextDataset({payload: responsePayload})(obj => obj, getStoreLocal);
      expect(result).to.eql(push({
        pathname: '/dataset/foo/graph',
        query: {
          version: datasetVersion,
          tipVersion: '123'
        },
        state: {}
      }));
    });

    it('should set jobId from getNextJobId', () => {
      const jobId = 'someJobId';
      sinon.stub(Actions, '_getNextJobId').returns(jobId);
      const payload = responsePayload.setIn(
        ['entities', 'fullDataset', datasetVersion, 'jobId'], Immutable.Map({id: jobId}));
      const result = Actions.navigateToNextDataset({payload})(obj => obj, getStore);
      expect(result).to.eql(push({
        pathname: datasetLink.split('?')[0],
        query: {
          jobId,
          version: datasetVersion,
          tipVersion: '123'
        },
        state: {}
      }));
      Actions._getNextJobId.restore();
    });

    it('should fail when no dataset in response', () => {
      expect(
        () => Actions.navigateToNextDataset({payload: undefined})(obj => obj, getStore)
      ).to.throw(Error);

      expect(
        () => Actions.navigateToNextDataset(
          {payload: responsePayload.set('result', undefined)}, false
        )(obj => obj, getStore)
      ).to.throw(Error);
    });

    describe('_getNextJobId', () => {
      it('should only include jobId if fullDataset is a run result (not approximate)', () => {
        const fullDataset = Immutable.fromJS({
          jobId: {id: 'someJobId'}
        });
        expect(Actions._getNextJobId(fullDataset.set('approximate', true))).to.be.undefined;
        expect(
          Actions._getNextJobId(fullDataset.set('approximate', false))
        ).to.equal(fullDataset.getIn(['jobId', 'id']));
      });
    });

    describe('isSaveAs param', () => {
      it('should change query.mode to edit', () => {
        const result = Actions.navigateToNextDataset({
          payload: responsePayload}, {isSaveAs: true})(obj => obj, getStore);
        expect(result).to.eql(push({
          pathname: datasetLink.split('?')[0],
          query: {
            version: datasetVersion,
            mode: 'edit',
            tipVersion: '123'
          },
          state: {}
        }));
      });
    });

    describe('preserveTip param', () => {
      it('should preserve tipVersion from location', () => {
        location.query = {tipVersion: 'abcde'};
        const result = Actions.navigateToNextDataset({
          payload: responsePayload}, {preserveTip: true})(obj => obj, getStore);
        expect(result).to.eql(push({
          pathname: datasetLink.split('?')[0],
          query: {
            version: datasetVersion,
            tipVersion: 'abcde'
          },
          state: {}
        }));

      });

    });

  });
});
