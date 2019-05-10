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
import PropTypes from 'prop-types';
import userUtils from '@app/utils/userUtils';
import config from '@app/utils/config';
import { getUser } from '@app/reducers';

export const Capabilities = {
  manageSpaces: 'MANAGE_SPACES'
};

export const authInfoPropType = PropTypes.shape({
  isAdmin: PropTypes.bool,
  allowSpaceManagement: PropTypes.bool
});

//state selector
export const getAuthInfoSelector = state => {
  const user = getUser(state);
  //should corresponds to authInfoPropType
  return {
    isAdmin: userUtils.isAdmin(user),
    allowSpaceManagement: config.allowSpaceManagement
  };
};

export const check = (capability, authInfo) => {
  let checkResult = false;

  switch (capability) {
  case Capabilities.manageSpaces:
    checkResult = authInfo.isAdmin || !!authInfo.allowSpaceManagement;
    break;
  default:
    throw `Not supported capability type: '${capability}'`;
  }

  return checkResult;
};

export const rulePropType = PropTypes.shape({
  capabilities: PropTypes.arrayOf(PropTypes.oneOf(Object.values(Capabilities))),
  isAdmin: PropTypes.bool
});

export const isAuthorized = (/* rule */ {
  capabilities = [],
  isAdmin = false
}, // see authInfoPropType for format
  authInfo // see rulePropType for format
) => {
  let result = false;

  if (isAdmin) {
    if (authInfo.isAdmin) {
      result = true;
    }
  }

  if (!result && capabilities && capabilities.length) { // check capabilities if any presented in the rule
    result = true;
    capabilities.forEach(capability => {
      if (!check(capability, authInfo)) {
        result = false;
        return false;
      }
    });
  }

  return result;
};

/**
 * Admins and users with manage space capability are allowed
 */
export const manageSpaceRule = {
  capabilities: [Capabilities.manageSpaces],
  isAdmin: true
};

export const manageSourceRule = {
  isAdmin: true // only admins are allowed to edit/remove source
};
