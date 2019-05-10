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
import { call, put, takeEvery, select, race, take } from 'redux-saga/effects';
import qsocks from 'qsocks';

import { API_URL_V2 } from 'constants/Api';
import localStorageUtils from 'utils/storageUtils/localStorageUtils';

import * as Actions from 'actions/explore/download';
import * as QlikActions from 'actions/qlik';
import { showQlikError, showQlikModal, showQlikProgress, hideQlikError } from 'actions/explore/ui';
import { showConfirmationDialog } from 'actions/confirmation';

import { getUserName } from 'selectors/account';
import { getLocation } from 'selectors/routing';

import { constructFullPath, constructFullPathAndEncode, getUniqueName } from 'utils/pathUtils';
import { getLocationChangePredicate } from './utils';

export const DSN = 'Dremio Connector';

export default function *watchQlikOpen() {
  yield takeEvery((action) => {
    if (action.type === Actions.OPEN_QLIK_SENSE) {
      return true;
    }
  }, openQlikSense);
}

export function* fetchQlikApp(dataset) {
  yield put({type: Actions.LOAD_QLIK_APP_START});

  const headers = new Headers();
  const displayFullPath = dataset.get('displayFullPath') || dataset.get('fullPathList');
  const href = `/qlik/${constructFullPathAndEncode(displayFullPath)}`;

  headers.append('Accept', 'text/plain+qlik-app');
  if (localStorageUtils) {
    headers.append('Authorization', localStorageUtils.getAuthToken());
  }
  const response = yield call(fetch, `${API_URL_V2}${href}`, {method: 'GET', headers});
  const responseText = yield response.text();
  if (!response.ok) {
    throw new Error(responseText);
  }
  return responseText;
}

export function* checkDsnList(qlikGlobal, dataset) {
  const dsns = yield call([qlikGlobal, qlikGlobal.getOdbcDsns]);
  let dsnDetected = false;
  for (let i = 0; i < dsns.length; i++) {
    if (dsns[i].qName === DSN) {
      dsnDetected = true;
      break;
    }
  }

  return dsnDetected;
}

function doesDocumentExist(docName, docList) {
  return docList && docList.some((doc) => {
    return (doc.qTitle === docName);
  });
}

export function santizeAppName(name) {
  // qlik app names cannot contain certain chars
  return name.replace(/[\/\"\\\*\:\>\<]+/g, '_');
}

export function getUniqueAppName(docName, docList) {
  const sanitizedName = santizeAppName(docName);

  return getUniqueName(sanitizedName, (name) => {
    return !doesDocumentExist(name, docList);
  });
}

export function* openQlikSense(action) {
  const username = yield select(getUserName);
  const password = yield call(requestPassword);
  if (!password) {
    return;
  }

  const dataset = action.payload;
  // fullPathList on homepage, displayFullPath on ExplorePage
  const displayFullPath = dataset.get('displayFullPath') || dataset.get('fullPathList');
  let applicationName = `Dremio ${constructFullPath(displayFullPath)}`;

  let qlikConfig;
  let qlikGlobal;
  yield put(showQlikModal(dataset));
  yield put(showQlikProgress());
  yield put(hideQlikError());
  let errorKey;
  try {
    errorKey = 'QLIK_GET_APP';
    qlikConfig = yield fetchQlikApp(dataset);
    errorKey = 'QLIK_CONNECT_FAILED';
    qlikGlobal = yield call([qsocks, qsocks.Connect]);
  } catch (error) {
    error && console.error(error);
    return yield put(showQlikErrorWrapper(dataset, errorKey));
  }

  try {
    const dsnDetected = yield checkDsnList(qlikGlobal, dataset);
    if (!dsnDetected) {
      return yield put(showQlikErrorWrapper(dataset, 'QLIK_DSN'));
    }

    const docList = yield call([qlikGlobal, qlikGlobal.getDocList]);
    applicationName = getUniqueAppName(applicationName, docList);

    const qlikApp = yield call([qlikGlobal, qlikGlobal.createApp], applicationName);
    const qlikDoc = yield call([qlikGlobal, qlikGlobal.openDoc], applicationName, null, null, null, true);
    yield call([qlikDoc, qlikDoc.createConnection], getConnectionConfig(
      window.location.hostname, username, password
    ));
    yield call([qlikDoc, qlikDoc.setScript], qlikConfig);

    yield call(createQlikSheet, qlikDoc);
    yield call([qlikDoc, qlikDoc.doSave]);
    qlikGlobal.connection.close();
    yield put({
      type: QlikActions.QLIK_APP_CREATION_SUCCESS,
      info: {
        appName: applicationName,
        appId: qlikApp.qAppId
      }
    });
  } catch (err) {
    return yield put(showQlikCustomErrorWrapper(dataset, err));
  }
}

export function* requestPassword() {
  let action;
  const passwordPromise = new Promise((resolve, reject) => {
    action = showConfirmationDialog({
      title: la('Qlik Sense'),
      confirmText: la('Continue'),
      text: la('Qlik Sense requires your Dremio password to continue:'),
      showPrompt: true,
      promptFieldProps: {
        type: 'password'
      },
      confirm: resolve,
      cancel: reject
    });
  });
  yield put(action);

  const location = yield select(getLocation);
  try {
    const {password} = yield race({
      password: passwordPromise,
      locationChange: take(getLocationChangePredicate(location))
    });


    if (password) {
      return password;
    }
  } catch (e) {
    // modal was canceled and promise rejected
    // do nothing
  }
}

/*
 * Helpers
 */

export function getConnectionConfig(hostname, username, password) {
  return {
    qName: 'Dremio',
    qConnectionString: `ODBC CONNECT TO "${DSN};` +
    'ADVANCEDPROPERTIES={HandshakeTimeout=5;QueryTimeout=180;TimestampTZDisplayTimezone=utc;'
    + 'ExcludedSchemas=sys,INFORMATION_SCHEMA};'
    + 'AUTHENTICATIONTYPE=PLAIN;AUTHMECH=0;CATALOG=DREMIO;CONNECTIONTYPE=Direct;'
    + `DESCRIPTION=Sample Dremio DSN;FASTSQLPREPARE=0;HOST=${hostname};";`,
    qType: 'ODBC',
    qUserName: username,
    qPassword: password
  };
}

function createQlikSheet(qlikDoc) {
  return qlikDoc.createObject({
    rank: '0',
    columns: 24,
    rows: 12,
    cells: [],
    title: la('Default'),
    description: la('Default sheet created by Dremio'),
    qInfo: {
      qId: 'sheet01',
      qType: 'sheet'
    },
    qMetaDef: {
      title: la('Default'),
      description: la('Default sheet created by Dremio')
    }
  });
}

export function getQlikAppUrl(qlikAppName) {
  return `http://localhost:4848/sense/app/${encodeURIComponent(qlikAppName)}`;
}

export function showQlikErrorWrapper(dataset, messageKey) {
  return showQlikError(Immutable.fromJS({code: messageKey}));
}

export function showQlikCustomErrorWrapper(dataset, error) {
  return showQlikError(Immutable.fromJS({code: 'QLIK_CUSTOM_ERROR', moreInfo: error.message}));
}
