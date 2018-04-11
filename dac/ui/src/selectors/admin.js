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
import userUtils from 'utils/userUtils';
import { createSelector } from 'reselect';

// USERS

export function getUsers(state) {
  return state.admin.get('users').map((userId) => state.resources.entities.getIn(['user', userId]));
}

export function getUser(state, userName) { // todo: really need to normalize userName v username
  console.warn('using deprecated selectors/admin getUser(state, userName) API - should reference users by id instead');
  return state.resources.entities.get('user').find(user => user.get('userName') === userName);
}

export function getIsLoggedInUserAdmin(state) {
  return userUtils.isAdmin(state.account.get('user'));
}

function getAccelerationsList(state) {
  const { entities } = state.resources;
  const accelerations = state.admin.get('accelerations');
  return accelerations.map(accelerationId => {
    return entities.getIn(['acceleration', accelerationId]);
  });
}

export const getAccelerations = createSelector(
  [ getAccelerationsList ],
  acceleration => acceleration
);


