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
// handles a state for expandable and resizable slidebar
import { MIN_SIDEBAR_WIDTH, SET_SIDEBAR_SIZE } from '@app/actions/home';
import localStorageUtils from 'utils/storageUtils/localStorageUtils';

const slidebarSize = (state, /* action */{ type, size }) => {
  switch (type) {
  case SET_SIDEBAR_SIZE:
    localStorageUtils.setWikiSize(size);
    return size;
  default:
    // As root reducer reset state on logout we need to read default value from local storage every
    // time in initialization phase (state === undefined)
    return state !== undefined ? state : (parseInt(localStorageUtils.getWikiSize(), 10) || MIN_SIDEBAR_WIDTH);
  }
};

export default slidebarSize;
