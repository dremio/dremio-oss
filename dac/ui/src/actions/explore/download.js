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
import { CALL_API } from 'redux-api-middleware';
import { push } from 'react-router-redux';
import { API_URL_V2 } from 'constants/Api';
import { saveAsDataset } from 'actions/explore/dataset/save';

import { showConfirmationDialog, hideConfirmationDialog } from 'actions/confirmation';
import { POWER_BI_MANUAL } from 'constants/links.json';

import FileUtils from 'utils/FileUtils';
import config from 'utils/config';
import jobsUtils from 'utils/jobsUtils';
import { constructFullPathAndEncode } from 'utils/pathUtils';

export const START_DATASET_DOWNLOAD = 'START_DATASET_DOWNLOAD';

export const startDownloadDataset = (dataset, format) => {
  return {type: START_DATASET_DOWNLOAD, meta: {dataset, format}};
};

export const downloadDataset = (dataset, format) => (dispatch) => dispatch(fetchDownloadDataset(dataset, format));

export const DOWNLOAD_DATASET_REQUEST = 'DOWNLOAD_DATASET_REQUEST';
export const DOWNLOAD_DATASET_SUCCESS = 'DOWNLOAD_DATASET_SUCCESS';
export const DOWNLOAD_DATASET_FAILURE = 'DOWNLOAD_DATASET_FAILURE';

const fetchDownloadDataset = (dataset, format) => {
  const meta = { dataset, notification: true };
  const href = dataset.getIn(['apiLinks', 'self']) + '/download';
  return {
    [CALL_API]: {
      types: [
        {type: DOWNLOAD_DATASET_REQUEST, meta},
        {type: DOWNLOAD_DATASET_SUCCESS, meta},
        {type: DOWNLOAD_DATASET_FAILURE, meta}
      ],
      method: 'GET',
      headers: {Accept: 'application/json'},
      endpoint: `${API_URL_V2}${href}?downloadFormat=${format}`
    }
  };
};

export const showDownloadModal = (jobId, confirm) => (dispatch) => {
  const jobsHref = jobsUtils.navigationURLForJobId(jobId);
  const onClick = (e) => {
    e.preventDefault();
    confirm(true);
    dispatch(hideConfirmationDialog());
    dispatch(push(jobsHref));
  };
  const currentJobLink = <a href={jobsHref} onClick={onClick}>
    {la('Go to Job')}
  </a>;
  return dispatch(
    showConfirmationDialog({
      title: la('Preparing Download…'),
      confirm: () => confirm(true),
      showOnlyConfirm: true,
      confirmText: la('Dismiss'),
      text: [
        <span>{la('Your download will start when ready.')}</span>,
        currentJobLink
      ]
    })
  );
};

export const OPEN_QLIK_SENSE = 'OPEN_QLIK_SENSE';

function needsSaveBeforeBI(dataset) {
  // if the dataset is a datasetUI and has no apiLinks.namespaceEntity, then we need to save it before doing BI as its
  // an unsaved dataset
  return dataset.get('entityType') === 'datasetUI' && !dataset.getIn(['apiLinks', 'namespaceEntity']);
}

/**
 * Triggers qlik saga
 * @param dataset
 */
export const openQlikSense = (dataset) => {
  return (dispatch) => {
    if (needsSaveBeforeBI(dataset)) {
      return dispatch(saveAsDataset('OPEN_QLIK_AFTER'));
    }

    return dispatch({
      type: OPEN_QLIK_SENSE,
      payload: dataset
    });
  };
};

export const LOAD_QLIK_APP_START   = 'LOAD_QLIK_APP_START';
export const LOAD_QLIK_APP_SUCCESS = 'LOAD_QLIK_APP_SUCCESS';
export const LOAD_QLIK_APP_FAILURE = 'LOAD_QLIK_APP_FAILURE';

export const downloadTableau = ({ href }) => (dispatch) => dispatch(fetchDownloadTableau({ href }));

export const LOAD_TABLEAU_START   = 'LOAD_TABLEAU_START';
export const LOAD_TABLEAU_SUCCESS = 'LOAD_TABLEAU_SUCCESS';
export const LOAD_TABLEAU_FAILURE = 'LOAD_TABLEAU_FAILURE';

const fetchDownloadTableau = ({ href }) => ({
  [CALL_API]: {
    types: [
      LOAD_TABLEAU_START,
      {type: LOAD_TABLEAU_SUCCESS, payload: (action, state, res) => FileUtils.getFileDownloadConfigFromResponse(res)},
      {
        type: LOAD_TABLEAU_FAILURE,
        meta: {
          notification: {
            message: la('There was an error preparing for Tableau.'),
            level: 'error'
          }
        }
      }
    ],
    headers: { Accept: config.tdsMimeType },
    method: 'GET',
    endpoint: `${API_URL_V2}${href}`
  }
});

export const openTableau = (dataset) => {
  return (dispatch) => {
    if (needsSaveBeforeBI(dataset)) {
      return dispatch(saveAsDataset('OPEN_TABLEAU'));
    }

    const displayFullPath = dataset.get('displayFullPath') || dataset.get('fullPathList');
    const href = `/tableau/${constructFullPathAndEncode(displayFullPath)}`;
    return dispatch(downloadTableau({ href }))
      .then((response) => {
        if (!response.error) {
          FileUtils.downloadFile(response.payload);
        }
      });
  };
};

export const openPowerBI = () => {
  return (dispatch) => {
    return dispatch(showConfirmationDialog({
      hideCancelButton: true,
      title: la('Power BI'),
      text: (
        <span>
          {la('Please follow the') + ' '}
          <a href={POWER_BI_MANUAL} target='_blank'>
            {la('instructions in the documentation')}
          </a>
          {' ' + la('to connect to this Dataset with Power BI.')}
        </span>
      )
    }));
  };
};
