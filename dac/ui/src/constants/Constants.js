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
import uuid from 'uuid';

export const HISTORY_LOAD_DELAY = 2500;
export const MAIN_HEADER_HEIGHT = '55';
export const LEFT_TREE_WIDTH = '220';
export const SEARCH_BAR_WIDTH = '600';
export const SEARCH_BAR_HEIGHT = '600';
export const AUTO_PREVIEW_DELAY = 1000;
export const EXPLORE_PROGRESS_STATES = ['STARTED', 'NOT STARTED', 'RUNNING']; //TODO put back NOT_SUBMITTED when it's working
export const CONTAINER_ENTITY_TYPES = new Set(['HOME', 'FOLDER', 'SPACE', 'SOURCE']);
export const HOME_SPACE_NAME = `@home-${uuid.v4()}`; // better to have Symbol here, but there is several problems with it

export const EXTRA_POPPER_CONFIG = {
  modifiers: {
    preventOverflow: {
      escapeWithReference: true
    }
  }
};

export const ENTITY_TYPES = {
  home: 'home',
  space: 'space',
  source: 'source',
  folder: 'folder'
};
