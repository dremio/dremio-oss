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
import { normalize } from "normalizr";
import { contentLoadActions } from "@app/actions/home";
import { applyDecorators } from "@app/utils/decorators";

/**
 * A reducer that maintain a content, such as files, folder and dataset for current
 * selected entity on home page
 * @param {object} state
 * @param {object} action
 */
export default function content(state = null, action) {
  switch (action.type) {
    case contentLoadActions.start:
      return null;
    case contentLoadActions.success: {
      const {
        meta: { entitySchema },
        payload,
      } = action;
      const entityType = entitySchema.getKey();
      const data = normalize(payload, entitySchema);
      return {
        entityType,
        entityId: data.result,
        entities: applyDecorators(Immutable.fromJS(data.entities)),
      };
    }
    case contentLoadActions.failure:
    default:
      return state;
  }
}

// selectors
export const getEntityId = (state) => state.entityId;
/**
 * Returns a current entity for home page, i.e. selected space/source/folder
 *
 * @param {object} state a redux state for content reducer
 */
export const getEntity = (state) => {
  if (!state) return null;

  const { entityType, entityId, entities } = state;

  return entityId ? entities.getIn([entityType, entityId]) : null;
};
/**
 * Return a entity that belongs to current home entity.
 *
 * @param {object} state a redux state for content reducer
 * @param {string} entityId
 * @param {string} entityType
 */
export const getEntityOrChild = (state, entityId, entityType) => {
  if (!state) return null;

  const { entities } = state;

  return entityId ? entities.getIn([entityType, entityId]) : null;
};

export const getCanTagsBeSkipped = (state) => {
  const entity = getEntity(state);
  return entity ? entity.getIn(["contents", "canTagsBeSkipped"], false) : false;
};

export const getEntityWithContent = (state) => {
  const entity = getEntity(state);

  if (!entity) return null;

  return entity.set("contents", denormalizeContents(state));
};

const denormalizeContents = (state) => {
  if (!state) {
    return Immutable.Map();
  }
  const entities = state.entities;
  const entity = getEntity(state);
  const contents = entity.get("contents");

  return Immutable.Map({
    datasets: contents
      .get("datasets")
      .map((key) => entities.getIn(["dataset", key])),
    files: contents.get("files").map((key) => denormalizeFile(state, key)),
    folders: contents
      .get("folders")
      .map((key) => entities.getIn(["folder", key])),
    physicalDatasets: contents
      .get("physicalDatasets")
      .map((key) => entities.getIn(["physicalDataset", key])),
  });
};

export function denormalizeFile(state, fileId) {
  const { entities } = state;
  const file = entities.getIn(["file", fileId]);
  if (file) {
    return file.get("fileFormat")
      ? file.set(
          "fileFormat",
          entities.getIn(["fileFormat", file.get("fileFormat")])
        )
      : file;
  }
}
