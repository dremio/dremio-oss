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
import uuid from 'uuid';
import { additionalColumns } from '@inject/pages/JobPageNew/AdditionalJobPageColumns';

export const HISTORY_LOAD_DELAY = 2500;
export const MAIN_HEADER_HEIGHT = '55';
export const LEFT_TREE_WIDTH = '220';
export const SEARCH_BAR_WIDTH = '600';
export const SEARCH_BAR_HEIGHT = '600';
export const AUTO_PREVIEW_DELAY = 1000;
export const MAX_UPLOAD_FILE_SIZE = 500 * 1024 * 1024; // 500MB
export const EXPLORE_PROGRESS_STATES = ['STARTED', 'NOT STARTED', 'RUNNING']; //TODO put back NOT_SUBMITTED when it's working
export const CONTAINER_ENTITY_TYPES = new Set(['HOME', 'FOLDER', 'SPACE', 'SOURCE']);
export const HOME_SPACE_NAME = `@home-${uuid.v4()}`; // better to have Symbol here, but there is several problems with it
export const MSG_CLEAR_DELAY_SEC = 3;

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

export const PROJECT_TYPE = {
  queryEngine: 'QUERY_ENGINE',
  dataPlane: 'DATA_PLANE'
};

export const CLIENT_TOOL_ID = {
  powerbi: 'client.tools.powerbi',
  tableau: 'client.tools.tableau',
  qlik: 'client.tools.qlik',
  qlikEnabled: 'support.dac.qlik'
};

export const TableColumns = [
  { key: 'jobStatus', id: 'jobStatus', label: '', disableSort: true, isSelected: true, width: 20, height: 40, flexGrow: '0', flexShrink: '0', isFixedWidth: true, isDraggable: false, headerClassName: '', disabled:true },
  { key: 'job', id: 'job', label: 'Job ID', disableSort: false, isSelected: true, width: 122, height: 40, flexGrow: '0', flexShrink: '0', isFixedWidth: false, isDraggable: true, headerClassName: '', disabled:true },
  { key: 'usr', id: 'usr', label: 'User', disableSort: false, isSelected: true, width: 167, height: 40, flexGrow: '0', flexShrink: '0', isFixedWidth: false, isDraggable: true, headerClassName: '', disabled:true },
  { key: 'acceleration', id: 'acceleration', label: 'Accelerated', disableSort: true, isSelected: true, width: 40, height: 40, flexGrow: '0', flexShrink: '0', isFixedWidth: true, isDraggable: false, headerClassName: '', disabled:true },
  { key: 'ds', id: 'ds', label: 'Dataset', disableSort: true, isSelected: true, width: 205, height: 40, flexGrow: '0', flexShrink: '0', isFixedWidth: false, isDraggable: true, headerClassName: '', disabled:false },
  { key: 'qt', id: 'qt', label: 'Query Type', disableSort: true, width: 113, isSelected: true, height: 40, flexGrow: '0', flexShrink: '0', isFixedWidth: false, isDraggable: true, headerClassName: '', disabled:true },
  ...additionalColumns,
  { key: 'st', id: 'st', label: 'Start Time', disableSort: false, width: 152, isSelected: true, height: 40, flexGrow: '0', flexShrink: '0', isFixedWidth: false, isDraggable: true, headerClassName: '', disabled:true },
  { key: 'dur', id: 'dur', label: 'Duration', disableSort: false, width: 95, isSelected: true, height: 40, flexGrow: '0', flexShrink: '0', isFixedWidth: false, isDraggable: true, headerClassName: 'jobsContent__tableHeaders', disabled:false },
  { key: 'sql', id: 'sql', label: 'SQL', disableSort: true, isSelected: true, width: 432, height: 40, flexGrow: '0', flexShrink: '0', isFixedWidth: false, isDraggable: true, headerClassName: '', disabled:false },
  { key: 'cost', id: 'cost', label: 'Planner Cost Estimate', disableSort: true, isSelected: false, width: 180, height: 40, flexGrow: '0', flexShrink: '0', isFixedWidth: false, isDraggable: true, headerClassName: 'jobsContent__tableHeaders', disabled:false },
  { key: 'planningTime', id: 'planningTime', disableSort: true, isSelected: false, label: 'Planning Time', width: 130, height: 40, flexGrow: '0', flexShrink: '0', isFixedWidth: false, isDraggable: true, headerClassName: 'jobsContent__tableHeaders', disabled:false },
  { key: 'rowsScanned', id: 'rowsScanned', disableSort: true, isSelected: false, label: 'Rows Scanned', width: 134, height: 40, flexGrow: '0', flexShrink: '0', isFixedWidth: false, isDraggable: true, headerClassName: 'jobsContent__tableHeaders', disabled:false },
  { key: 'rowsReturned', id: 'rowsReturned', disableSort: true, isSelected: false, label: 'Rows Returned', width: 137, height: 40, flexGrow: '0', flexShrink: '0', isFixedWidth: false, isDraggable: true, headerClassName: 'jobsContent__tableHeaders', disabled:false }
];

export const ScansForFilter = [
  { key: 'datasetType', label: 'Scans.SourceType', content: 'Managed Reflection(Parque)' },
  { key: 'nrScanThreads', label: 'Scans.ScanThread', content: '115' },
  { key: 'ioWaitDurationMs', label: 'Scans.IoWaitTime', content: '00:00:00.75' },
  { key: 'nrScannedRows', label: 'Scans.RowScanned', content: '143K' }
];

export const ReflectionsUsed = [
  {
    icon: 'ReflectionsUsed.svg',
    label: 'Reflection.CustomersRaw',
    subScript: 'transaction_analysis.base_views.Customers'
  },
  {
    icon: 'ReflectionsUsed.svg',
    label: 'Reflection.TransactionsRaw',
    subScript: 'lake_source.incoming_data.transactions'
  }
];
export const ReflectionsNotUsed = [
  {
    icon: 'ReflectionsNotUsed.svg',
    label: 'Reflection.TransactionAgg',
    subScript: 'lake_source.incoming_data.transactions'
  }
];

export const EXECUTION_DROPDOWN_OPTIONS = [
  { option: 'Runtime', label: 'Runtime' },
  { option: 'Total Memory', label: 'Total Memory' },
  { option: 'Records', label: 'Records' }
];
