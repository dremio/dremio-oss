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
import Immutable  from 'immutable';

import * as ActionTypes from 'actions/account';
import * as AccountTypes from 'actions/admin';
import accountMapper from 'utils/mappers/accountMapper';
import objectUtils from 'utils/objectUtils';
import localStorageUtils from 'utils/storageUtils/localStorageUtils';

function getInitialState() {
  return Immutable.fromJS({
    sourceCredentialList: {
      items: [],
      isInProgress: false,
      isFailed: false
    },
    user: { // TODO: no fake objects: this should be of User type and null by default
      ...(localStorageUtils ? localStorageUtils.getUserData() : {}),
      isInProgress: false,
      isFailed: false,
      name: ''
    },
    apiKey: '',
    allUsers: {
      users: [],
      isInProgress: false,
      isFailed: false
    }
  });
}

export default function users(state = getInitialState(), action) {
  switch (action.type) {
  case ActionTypes.SOURCE_CREDENTIAL_START:
    return objectUtils.merge({}, state, {
      sourceCredentialList: {
        items: [],
        isInProgress: true,
        isFailed: false
      }
    });

  case ActionTypes.SOURCE_CREDENTIAL_SUCCESS :
    return objectUtils.merge({}, state, {
      sourceCredentialList: {
        items: accountMapper.mapSourceCredentialList(action.payload),
        isInProgress: false,
        isFailed: false
      }
    });

  case ActionTypes.SOURCE_CREDENTIAL_FAILURE :
    return objectUtils.merge({}, state, {
      sourceCredentialList: {
        items: [],
        isInProgress: false,
        isFailed: true
      }
    });

  case ActionTypes.ADD_SOURCE_CREDENTIAL :
    return objectUtils.merge( {}, state, {
      sourceCredentialList: {
        items: [
          ...state.sourceCredentialList.items,
          {
            name: 'dremio-s3',
            type:'amazon-s3',
            properties: [
              {name: 'accessKeyId', value:'vletnyanchyn'},
              {name: 'secretAccessKey', value:'ertyhhj'}
            ]
          }
        ]
      }
    } );

  case ActionTypes.REMOVE_SOURCE_CREDENTIAL :
    return objectUtils.merge( {}, state, {
      sourceCredentialList: {
        items: [
          ...state.sourceCredentialList.items.slice(0, action.index),
          ...state.sourceCredentialList.items.slice(action.index + 1)
        ]
      }
    } );

  case ActionTypes.GET_API_KEY_START:
    return objectUtils.merge({}, state, {
      apiKey: ''
    });

  case ActionTypes.GET_API_KEY_SUCCESS :
    return objectUtils.merge({}, state, {
      apiKey: action.payload.apiKey
    });

  case ActionTypes.GET_API_KEY_FAILURE :
    return objectUtils.merge({}, state, {
      apiKey: ''
    });

  case ActionTypes.LOGIN_USER_START: {
    return state.setIn(['user', 'isInProgress'], !action.error)
                .setIn(['user', 'isFailed'], action.error)
                .setIn(['user', 'name'], action.meta.form.userName);
  }

  case ActionTypes.LOGIN_USER_SUCCESS: {
    return state.set('user', Immutable.fromJS({...action.payload, inProgress: false, isFailed: false}));
  }

  case ActionTypes.LOGIN_USER_FAILURE: {
    return state.setIn(['user', 'isInProgress'], false)
                .setIn(['user', 'isFailed'], true)
                .setIn(['user', 'name'], action.meta.form.userName);
  }
  case AccountTypes.EDIT_ACCOUNT_SUCCESS:
    return state.set('user', Immutable.fromJS({
      ...state.get('user').toJS(),
      ...action.payload.userConfig
    }));
  default:
    return state;
  }
}
