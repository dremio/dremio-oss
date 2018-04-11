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
import { LOAD_RESOURCE_TREE_SUCCESS } from 'actions/resources/tree';
import { constructFullPath, splitFullPath } from 'utils/pathUtils';

const buildPath = (tree, payloadNodes) => {
  let resources = tree || Immutable.List();
  const paths = [];
  const nodes = payloadNodes.slice().map((node, index, curArr) => {
    const delimeter = index ? '.' : '';
    return curArr.slice(0, index).join('.') + delimeter + node;
  });

  while (nodes.length && resources.size) {
    const index = resources.findIndex(node => constructFullPath(node.get('fullPath'), true) === nodes[0]);
    paths.push(index, 'resources');
    resources = resources.getIn([index, 'resources']) || Immutable.List();
    nodes.shift();
  }

  return paths;
};

export default (state, action) => {
  switch (action.type) {
  case LOAD_RESOURCE_TREE_SUCCESS: {
    const { path, isExpand } = action.meta;
    const nodes = path.length ? splitFullPath(path) : [];
    const payloadResources = Immutable.fromJS(action.payload.resources);
    const builedPath = buildPath(state.get('tree'), nodes);
    const resources = payloadResources.sort(
      (prevRes, res) => prevRes.get('type') !== 'HOME' && res.get('type') === 'HOME' ||
                        prevRes.get('type') === 'HOME' && res.get('type') !== 'HOME' && -1
    );
    return builedPath.length && !isExpand
      ? state.setIn(['tree', ...builedPath], resources)
      : state.set('tree', resources);
  }
  default:
    return state;
  }
};
