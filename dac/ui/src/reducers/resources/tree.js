/*
 * Copyright (C) 2017 Dremio Corporation
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
import * as ActionTypes from 'actions/resources/spaceDetails';
import Immutable from 'immutable';

const uiProps = Immutable.Map({
  arrow: false,
  opened: false,
  selected: false,
  icon: ''
});

import { decorators } from './entities';
export default function tree(state = Immutable.List(), action) {
  switch (action.type) {
  case ActionTypes.LOAD_SPACE_FOR_TREE_SUCCESS: {
    const entities = action.payload.get('entities');
    const formatedEntities = entities.map((value, key) => {
      return decorators[key]
        ? value.map(decorators[key])
        : value;
    });
    const datasets = (formatedEntities.get('dataset') || Immutable.Map())
      .toList().map(item => item.merge(uiProps).set('icon', 'VirtualDataset'));
    const folders = (formatedEntities.get('folder') || Immutable.Map())
      .map(folder => folder.merge(formatedEntities.get(folder)))
      .toList().map(item => item.merge(uiProps).set('icon', 'Folder').set('children', []));
    const files = (formatedEntities.get('file') || Immutable.Map())
      .map(file => file.merge(formatedEntities.get('fileConfig').get(file.get('fileConfig'))))
      .toList().map(item => item.merge(uiProps).set('icon', 'File'));
    const physicalDatasets = (formatedEntities.get('physicalDataset') || Immutable.Map())
      .toList().map(item => item.merge(uiProps).set('icon', 'PhysicalDataset'));
    return datasets.concat(folders, files, physicalDatasets);
  }
  default:
    return state;
  }
}
