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
import { CALL_API } from 'redux-api-middleware';

import {
  LOCATION_CHANGE,
  getLocationChangePredicate,
  getExplorePageLocationChangePredicateImpl,
  getActionPredicate,
  getApiCallCompletePredicate,
  getApiActionTypes,
  getApiActionEntity
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

  describe('getExplorePageLocationChangePredicateImpl', () => {
    const oldLocation = {
      pathname: '/resources/resourceId/foo',
      query: { bar: 'bar'},
      state: { a: 'b' }
    };
    const differentLocation = {
      pathname: '/resources/resourceId/different',
      query: { bar: 'different'},
      state: { a: 'different'}
    };
    const predicate = getExplorePageLocationChangePredicateImpl(oldLocation);

    it('returns false if location is not changed', () => {
      expect(predicate({type: LOCATION_CHANGE, payload: oldLocation})).to.be.false;
    });

    it('returns false if any action, except location changed action is resecived', () => {
      expect(predicate({ type: 'foo', payload: differentLocation })).to.be.false;
      expect(predicate({ type: 'foo' })).to.be.false;
      expect(predicate({ type: LOCATION_CHANGE, payload: differentLocation })).to.be.true;
    });

    it('returns true if location.path is changed', () => {
      expect(predicate({type: LOCATION_CHANGE, payload: {
        ...oldLocation,
        pathname: differentLocation.pathname
      }})).to.be.true;
      // should return false if we are navigating to graph/wiki tab
      expect(predicate({type: LOCATION_CHANGE, payload: {
        ...oldLocation,
        pathname: oldLocation.pathname + '/graph'
      }})).to.be.false;
    });

    it('returns true if location.query is changed', () => {
      expect(predicate({type: LOCATION_CHANGE, payload: {
        ...oldLocation,
        query: differentLocation.query
      }})).to.be.true;

      expect(predicate({type: LOCATION_CHANGE, payload: {
        ...oldLocation,
        query: null
      }})).to.be.true;

      expect(predicate({type: LOCATION_CHANGE, payload: {
        ...oldLocation,
        query: {}
      }})).to.be.true;
    });

    it('returns false if location.state is changed', () => {
      expect(predicate({type: LOCATION_CHANGE, payload: {
        ...oldLocation,
        state: differentLocation.state
      }})).to.be.false;
      expect(predicate({type: LOCATION_CHANGE, payload: {
        ...oldLocation,
        state: null
      }})).to.be.false;
      expect(predicate({type: LOCATION_CHANGE, payload: {
        ...oldLocation,
        state: {}
      }})).to.be.false;
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
});
