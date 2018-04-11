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
import invariant from 'invariant';

const FORMATTED_ENTITY_TYPES = new Set(['file', 'folder']);

// Certain kinds of DS can't have certain operations performed on them.
// This replicates the server's constraints so that we know what one
// can or cannot do with a given DS.
export function abilities(
  entity,
  entityType = entity.get('entityType'),
  isHomeFile = entity.get('isHomeFile') || false
) {
  invariant(entityType, 'entityType is required');

  const canEditFormat = FORMATTED_ENTITY_TYPES.has(entityType);
  const canRemoveFormat = canEditFormat && !isHomeFile;
  const isPhysical = FORMATTED_ENTITY_TYPES.has(entityType) || entityType === 'physicalDataset';
  const canEdit = !isPhysical;
  const canMove = !isPhysical; // future: allow move file in home
  const canDelete = !isPhysical || isHomeFile;

  // the "Acceleration Updates" tab should only be visible for physical datasets/folder
  // https://dremio.atlassian.net/browse/DX-5019
  // https://dremio.atlassian.net/browse/DX-5689
  // (doesn't make sense for home files though)
  const canSetAccelerationUpdates = isPhysical && !isHomeFile;

  return {
    canEditFormat,
    canRemoveFormat,
    canEdit,
    canMove,
    canDelete,
    canSetAccelerationUpdates
  };
}

export function hasDatasetChanged(nextDataset, prevDataset) {
  const haveNewDatasetVersion = !prevDataset ||
    nextDataset.get('datasetVersion') !== prevDataset.get('datasetVersion');

  return haveNewDatasetVersion || nextDataset.get('isNewQuery') !== prevDataset.get('isNewQuery');
}
