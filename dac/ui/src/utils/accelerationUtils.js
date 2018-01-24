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
export const mapStateToIcon = status => ({
  NEW: 'Ellipsis',
  RUNNING: 'Loader fa-spin',
  DONE: 'OKSolid',
  FAILED: 'ErrorSolid',
  DELETED: 'Warning-Solid', // deleted and un-replaced layout is probably bad

  // Acceleration#state:
  OUT_OF_DATE: 'Warning-Solid',

  // synthetic:
  DISABLED: 'Disabled',
  EXPIRED: 'Warning-Solid',
  FAILED_FINAL: 'ErrorSolid',
  FAILED_NONFINAL: 'Warning-Solid'
}[status] || 'Warning-Solid');

export const mapStateToText = status => ({
  NEW: la('New'),
  RUNNING: la('In progress'),
  DONE: la('Ready'),
  FAILED: la('Error'),
  DELETED: la('Deleted'),

  // Acceleration#state:
  OUT_OF_DATE: la('Outdated'),

  // synthetic:
  DISABLED: la('Disabled'),
  EXPIRED: la('Expired'),
  FAILED_FINAL: la('Multiple attempts to build Reflection failed, will not reattempt.'),
  FAILED_NONFINAL: la('Attempt to build Reflection failed, will reattempt.')
}[status] || la('Unknown: ') + status);

export function summarizeState(acceleration) {
  if (acceleration.errorList && acceleration.errorList.length) {
    return 'FAILED';
  }

  if (acceleration.state === 'OUT_OF_DATE') { // todo: eventually replace Acceleration#state with other flags
    return 'OUT_OF_DATE';
  }

  // generating suggestions
  if (acceleration.state === 'NEW') { // todo: eventually replace Acceleration#state with a #isGeneratingSuggestions bool
    return 'NEW';
  }

  let eitherEnabled = false;

  const layouts = [];

  if (acceleration.aggregationLayouts.enabled) {
    eitherEnabled = true;
    layouts.push(...acceleration.aggregationLayouts.layoutList);
  }
  if (acceleration.rawLayouts.enabled) {
    eitherEnabled = true;
    layouts.push(...acceleration.rawLayouts.layoutList);
  }

  if (!eitherEnabled) return 'DISABLED';

  const states = new Set(layouts.map(syntheticLayoutState));

  for (const state of [
    'FAILED_FINAL',
    'FAILED_NONFINAL',
    'EXPIRED',
    'RUNNING',
    'NEW'
  ]) {
    if (states.has(state)) return state;
  }

  return 'DONE';
}

export function syntheticLayoutState(layout) {
  if (layout.state === 'FAILED') return 'FAILED_FINAL';
  if (layout.latestMaterializationState === 'FAILED' && layout.state === 'ACTIVE') return 'FAILED_NONFINAL';
  if (layout.latestMaterializationState === 'DONE' && !layout.hasValidMaterialization) return 'EXPIRED';
  // todo: future: show a "RUNNING but it is a reattempt"
  return layout.latestMaterializationState;
}

export function summarizeByteSize(acceleration) {
  const layouts = [...acceleration.aggregationLayouts.layoutList, ...acceleration.rawLayouts.layoutList];

  let currentByteSize = 0;
  let totalByteSize = 0;
  for (const layout of layouts) {
    currentByteSize += layout.currentByteSize || 0;
    totalByteSize += layout.totalByteSize;
  }
  return {currentByteSize, totalByteSize};
}
