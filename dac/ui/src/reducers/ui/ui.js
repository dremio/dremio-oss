/*
 * Copyright (C) 2017-2019 Dremio Corporation
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
import Immutable from "immutable";

import * as ActionTypes from "@inject/actions/ui/ui";

const initialState = Immutable.fromJS({
  rightTreeVisible: false,
  externalSourcesExpanded: false,
  metastoreSourcesExpanded: false,
  objectStorageSourcesExpanded: false,
  dataplaneSourcesExpanded: false,
  datasetsExpanded: false,
  dremioCatalogsExpanded: false,
  lakehouseSourcesExpanded: false,
  resourceTree: {
    path: [],
    nodes: {},
  },
});

export default function ui(state = initialState, action) {
  switch (action.type) {
    case ActionTypes.UPDATE_RIGHT_PANEL_VISIBILITY:
      return state.set("rightTreeVisible", action.visible);

    case ActionTypes.RESET_RESOURCE_TREE:
      return state.set("resourceTree", Immutable.Map());
    case ActionTypes.UPDATE_RESOURCE_TREE_PATH:
      return state.setIn("resourceTree", "path", action.path);
    case ActionTypes.TOGGLE_RESOURCE_TREE_NODE_EXPANDED: {
      const expanded = state.getIn([
        "resourceTree",
        "nodes",
        action.id,
        "expanded",
      ]);
      return state.mergeIn(["resourceTree", "nodes", action.id], {
        expanded: !expanded,
      });
    }
    case ActionTypes.TOGGLE_EXTERNAL_SOURCES_EXPANDED:
      return state.set(
        "externalSourcesExpanded",
        !state.get("externalSourcesExpanded"),
      );
    case ActionTypes.TOGGLE_METASTORE_EXPANDED:
      return state.set(
        "metastoreSourcesExpanded",
        !state.get("metastoreSourcesExpanded"),
      );
    case ActionTypes.TOGGLE_OBJECT_STORAGE_EXPANDED:
      return state.set(
        "objectStorageSourcesExpanded",
        !state.get("objectStorageSourcesExpanded"),
      );
    case ActionTypes.TOGGLE_DATAPLANE_SOURCES_EXPANDED:
      return state.set(
        "dataplaneSourcesExpanded",
        !state.get("dataplaneSourcesExpanded"),
      );
    case ActionTypes.TOGGLE_DATASETS_EXPANDED:
      return state.set("datasetsExpanded", !state.get("datasetsExpanded"));
    case ActionTypes.TOGGLE_LAKEHOUSE_SOURCES_EXPANDED:
      return state.set(
        "lakehouseSourcesExpanded",
        !state.get("lakehouseSourcesExpanded"),
      );
    case ActionTypes.TOGGLE_DREMIO_CATALOGS_EXPANDED:
      return state.set(
        "dremioCatalogsExpanded",
        !state.get("dremioCatalogsExpanded"),
      );
    default:
      return state;
  }
}

export function getResourceTreePath(state) {
  return state.ui.getIn("resourceTree", "path");
}
