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
import Immutable from 'immutable';
import { CALL_API } from 'redux-api-middleware';
import { explorePageLocationChanged } from '@app/actions/explore/dataset/data';
import { PageTypes } from '@app/pages/ExplorePage/pageTypes';

import {
  LOCATION_CHANGE,
  getLocationChangePredicate,
  getExplorePageLocationChangePredicate,
  getActionPredicate,
  getApiCallCompletePredicate,
  getApiActionTypes,
  getApiActionEntity,
  needsTransform,
  isSqlChanged
} from './utils';

const apiAction = {
  [CALL_API]:  {
    types: ['START', 'SUCCESS', 'FAILURE']
  }
};
const apiActionWithEntity = {
  [CALL_API]: {
    types: [
      'START',
      {type: 'SUCCESS', meta: {entity: 'entity'}},
      'FAILURE'
    ]
  }
};

describe('saga utils', () => {
  describe('getLocationChangePredicate', () => {
    it('should return predicate that tests action type and that location has changed', () => {
      const oldLocation = {pathname: 'foo', query: {bar: 'bar'}};
      const predicate = getLocationChangePredicate(oldLocation);
      expect(predicate({type: 'foo'})).to.be.false;
      expect(predicate({type: 'foo', payload: {pathname: 'foo'}})).to.be.false;
      expect(predicate({type: LOCATION_CHANGE, payload: oldLocation})).to.be.false;
      expect(predicate({type: LOCATION_CHANGE, payload: {pathname: 'foo', query: {bar: 'different'}}})).to.be.true;
      expect(predicate({type: LOCATION_CHANGE, payload: {pathname: 'foo', state: {}}})).to.be.true;

      const locationWithState = {pathname: 'foo', query: {bar: 'bar'}, state: {a: 'b'}};
      const predicate2 = getLocationChangePredicate(locationWithState);
      expect(predicate2(
        {type: LOCATION_CHANGE, payload: {pathname: 'foo', query: {bar: 'bar'}, state: {a: 'b'}}})).to.be.false;
      expect(predicate2(
        {type: LOCATION_CHANGE, payload: {pathname: 'foo', query: {bar: 'bar'}, state: {a: 'different'}}})).to.be.true;
    });
  });

  describe('getExplorePageLocationChangePredicate', () => {
    const generateRouteState = ({
      path,
      pageType,
      query,
      state
    }) => ({
      location: {
        pathname: path + (pageType ? `/${pageType}` : ''),
        query,
        state
      },
      params: {
        pageType
      }
    });

    const originalParams = {
      path: '/resources/resourceId/foo',
      query: { bar: 'bar'},
      state: { a: 'b' }
    };
    const differentParams = {
      path: '/resources/resourceId/different',
      query: { bar: 'different'},
      state: { a: 'different'}
    };
    const predicate = getExplorePageLocationChangePredicate(generateRouteState(originalParams));

    const testPredicate = (newParams, expected) => {
      expect(predicate(explorePageLocationChanged(generateRouteState(newParams)))).to.be.equal(expected);
    };

    it('returns false if location is not changed', () => {
      testPredicate(originalParams, false);
    });

    it('returns false if any action, except EXPLORE_PAGE_LOCATION_CHANGED action is received', () => {
      expect(predicate({ ...explorePageLocationChanged(originalParams, differentParams), type: 'foo' })).to.be.false;
      expect(predicate({ type: 'foo' })).to.be.false;
      testPredicate(differentParams, true);
    });

    it('returns true if location.path is changed', () => {
      testPredicate({
        ...originalParams,
        path: differentParams.path
      }, true);
      // treat navigation to details page as location change
      testPredicate({
        ...originalParams,
        pageType: PageTypes.details
      }, true);
      // should return false if we are navigating to graph/wiki tab
      testPredicate({
        ...originalParams,
        pageType: PageTypes.graph
      }, false);
      testPredicate({
        ...originalParams,
        pageType: PageTypes.wiki
      }, false);
    });

    it('returns true if location.query is changed', () => {
      testPredicate({
        ...originalParams,
        query: differentParams.query
      }, true);

      testPredicate({
        ...originalParams,
        query: null
      }, true);

      testPredicate({
        ...originalParams,
        query: {}
      }, true);
    });

    it('returns false if location.state is changed', () => {
      testPredicate({
        ...originalParams,
        state: differentParams.state
      }, false);
      testPredicate({
        ...originalParams,
        state: null
      }, false);
      testPredicate({
        ...originalParams,
        state: {}
      }, false);
    });

    it('returns false if location is changed due to the fact, that a dataset was saved as other dataset', () => {
      // In that case we set state.afterDatasetSave = true
      testPredicate({
        ...differentParams,
        state: {
          ...differentParams.state,
          afterDatasetSave: true
        }
      }, false);
    });
  });

  describe('getActionPredicate', () => {
    describe('without entity', () => {
      it('should return true only if action.type is in actionTypes', () => {
        expect(getActionPredicate('SUCCESS')({type: 'FOO'})).to.be.false;
        expect(getActionPredicate(['SUCCESS'])({type: 'FOO'})).to.be.false;
        expect(getActionPredicate('SUCCESS')({type: 'SUCCESS'})).to.be.true;
        expect(getActionPredicate(['SUCCESS'])({type: 'SUCCESS'})).to.be.true;
      });
    });

    describe('with entity', () => {
      it('should return true only if entity === action entity, when action type matches', () => {
        const entity = 'entity';
        expect(getActionPredicate('SUCCESS', entity)({type: 'SUCCESS'})).to.be.false;
        expect(getActionPredicate('SUCCESS', entity)({type: 'SUCCESS', meta: {entity: 'other'}})).to.be.false;
        expect(getActionPredicate('SUCCESS', entity)({type: 'SUCCESS', meta: {entity}})).to.be.true;
      });

      it('should return false despite entity when action type does not matches', () => {
        const entity = 'entity';
        expect(getActionPredicate('SUCCESS', entity)({type: 'FOO', meta: {entity}})).to.be.false;
      });
    });
  });

  describe('getApiCallCompletePredicate', () => {
    it('should return true if action.error', () => {
      expect(getApiCallCompletePredicate(apiAction)({type: 'START', error: true})).to.be.true;
      expect(getApiCallCompletePredicate(apiAction)({type: 'START'})).to.be.false;
      expect(getApiCallCompletePredicate(apiAction)({type: 'SUCCESS', error: true})).to.be.true;
      expect(getApiCallCompletePredicate(apiAction)({type: 'SUCCESS'})).to.be.true;
      expect(getApiCallCompletePredicate(apiAction)({type: 'FAILURE', error: true})).to.be.true;
      expect(getApiCallCompletePredicate(apiAction)({type: 'FAILURE'})).to.be.true;
    });
  });


  describe('getApiCallCompletePredicate', () => {
    const noEntityPredicate = getApiCallCompletePredicate(apiAction);
    const entityPredicate = getApiCallCompletePredicate(apiActionWithEntity);

    it('should return true when START action with error', () => {
      expect(noEntityPredicate({type: 'START'})).to.be.false;
      expect(noEntityPredicate({type: 'START', error: true})).to.be.true;

      expect(entityPredicate({type: 'START', error: true, meta: {entity: 'different'}})).to.be.false;
      expect(entityPredicate({type: 'START', error: true, meta: {entity: 'entity'}})).to.be.true;
    });

    it('should return true when SUCCESS or FAILURE', () => {
      expect(noEntityPredicate({type: 'SUCCESS'})).to.be.true;

      expect(entityPredicate({type: 'SUCCESS', meta: {entity: 'different'}})).to.be.false;
      expect(entityPredicate({type: 'SUCCESS', meta: {entity: 'entity'}})).to.be.true;
    });
  });

  describe('getApiActionTypes', () => {
    const action = {
      [CALL_API]: {
        types: [
          'type1',
          {type: 'type2'},
          'type3'
        ]
      }
    };
    const actionTypes = ['type1', 'type2', 'type3'];

    it('should extract type strings as well as types in objects', () => {
      expect(getApiActionTypes(action)).to.eql(actionTypes);
    });

    it('should extract action types from dispatch wrapped action', () => {
      expect(getApiActionTypes((dispatch) => dispatch(action))).to.eql(actionTypes);
    });
  });

  describe('getApiActionEntity', () => {
    let action;
    beforeEach(() => {
      action = {
        [CALL_API]: {
          types: [
            'type1',
            {type: 'type2', meta: {entity: 'entity'}},
            'type3'
          ]
        }
      };
    });

    it('should extract entity from success meta', () => {
      expect(getApiActionEntity(action)).to.eql('entity');
    });

    it('should not die if no entity', () => {
      delete action[CALL_API].types[1].meta.entity;
      expect(getApiActionEntity(action)).to.be.undefined;

      delete action[CALL_API].types[1].meta;
      expect(getApiActionEntity(action)).to.be.undefined;

      action[CALL_API].types[1] = 'type2';
      expect(getApiActionEntity(action)).to.be.undefined;
    });
  });

  it('isSqlChanged should return true only if currentSql !== null and it !== savedSql', () => {
    expect(isSqlChanged('some sql', null)).to.be.false;
    expect(isSqlChanged('some sql', undefined)).to.be.false;
    expect(isSqlChanged('some sql', 'some sql')).to.be.false;
    expect(isSqlChanged('some sql', 'different sql')).to.be.true;
    expect(isSqlChanged()).to.be.false;
    expect(isSqlChanged(null, 'some sql')).to.be.true;
    expect(isSqlChanged(undefined, 'some sql')).to.be.true;
  });

  describe('needsTransform', () => {
    it('should return false if no changes for existing dataset', () => {
      const dataset = Immutable.fromJS({datasetVersion: 1});
      const result = needsTransform(dataset);
      expect(result).to.equal(false);
    });
    it('should return true if sql changed', () => {
      const dataset = Immutable.fromJS({datasetVersion: 1});
      const result = needsTransform(dataset, [], 'SELECT');
      expect(result).to.equal(true);
    });
    it('should return false if sql is not changed', () => {
      const dataset = Immutable.fromJS({datasetVersion: 1, sql: 'SELECT'});
      const result = needsTransform(dataset, [], 'SELECT');
      expect(result).to.equal(false);
    });
    it('should return true if context changed', () => {
      const dataset = Immutable.fromJS({datasetVersion: 1, sql: 'SELECT', context: ['Prod', '1']});
      const result = needsTransform(dataset, ['Prod', '2'], 'SELECT');
      expect(result).to.equal(true);
    });
    it('should return false if context not changed', () => {
      const dataset = Immutable.fromJS({datasetVersion: 1, sql: 'SELECT', context: ['Prod', '1']});
      const result = needsTransform(dataset, ['Prod', '1'], 'SELECT');
      expect(result).to.equal(false);
    });
  });
});
