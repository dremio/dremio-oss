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
import invariant from 'invariant';
import { WEB_SOCKET_URL } from '@app/constants/Api';
import localStorageUtils from 'utils/storageUtils/localStorageUtils';
import { addNotification, removeNotification } from 'actions/notification';
import Immutable from 'immutable';
import uuid from 'uuid';

const PING_INTERVAL = 15000;
const CHECK_INTERVAL = 5000;

const WS_MESSAGE_PING = 'ping';
export const WS_MESSAGE_JOB_DETAILS = 'job-details';
export const WS_MESSAGE_JOB_DETAILS_LISTEN = 'job-details-listen';
export const WS_MESSAGE_REFLECTION_JOB_DETAILS_LISTEN = 'reflection-job-details-listen';
export const WS_MESSAGE_JOB_PROGRESS = 'job-progress';
export const WS_MESSAGE_QV_JOB_PROGRESS = 'job-progress-newListingUI';
export const WS_MESSAGE_JOB_PROGRESS_LISTEN = 'job-progress-listen';
export const WS_MESSAGE_QV_JOB_PROGRESS_LISTEN = 'qv-job-progress-listen';
export const WS_MESSAGE_REFLECTION_JOB_PROGRESS_LISTEN = 'reflection-job-progress-listen';
export const WS_MESSAGE_JOB_RECORDS = 'job-records';
export const WS_MESSAGE_JOB_RECORDS_LISTEN = 'job-records-listen';

export const WS_CONNECTION_OPEN = 'WS_CONNECTION_OPEN';
export const WS_CONNECTION_CLOSE = 'WS_CONNECTION_CLOSE';
export const WS_CLOSED = 'WS_CLOSED';

export class Socket {
  dispatch = null;
  _socket = null;
  _listenMessages = {};
  _pingId = 0;
  _checkId = 0;
  _failureCount = 0;
  _listeners = {};

  get isOpen() {
    return !!this._socket && this._socket.readyState === WebSocket.OPEN;
  }

  get exists() {
    return !!this._socket;
  }

  open() {
    invariant(!this._socket, 'socket already open');
    invariant(this.dispatch, 'socket requires #dispatch to be assigned');

    this._createConnection();
    this._pingId = setInterval(this._ping, PING_INTERVAL);
    this._checkId = setInterval(this._checkConnection, CHECK_INTERVAL);
  }

  close() {
    if (this._socket) this._socket.close();
    this._socket = null;
    this._listenMessages = {};
    this._failureCount = 0;
    clearInterval(this._pingId);
    clearInterval(this._checkId);
  }

  _createConnection() {
    const authToken = localStorageUtils && localStorageUtils.getAuthToken();
    window.dremioSocket = this._socket = new WebSocket(WEB_SOCKET_URL, [authToken]);
    this._socket.onopen = this._handleConnectionEstablished;
    this._socket.onclose = this._handleConnectionClose;
    this._socket.onerror = this._handleConnectionError;
    this._socket.onmessage = this._handleMessage;
  }

  _checkConnection = () => { // if the connection dies, keep trying to reopen it
    if (this._socket.readyState === WebSocket.CLOSED) {
      this._createConnection();
    }
  };

  _handleConnectionError = (e) => {
    console.error('SOCKET CONNECTION ERROR', e);
    this._failureCount++;
    if (this._failureCount === 6) this.dispatch(addNotification(Immutable.Map({code: WS_CLOSED, messageType: WS_CLOSED}), 'error'));
  };

  _handleConnectionClose = () => {
    console.info('SOCKET CONNECTION CLOSE');
    setTimeout(() => { // defer because can't dispatch inside a reducer
      this.dispatch({type: WS_CONNECTION_CLOSE});
    });
  };

  _handleConnectionEstablished = () => {
    console.info('SOCKET CONNECTION OPEN');
    this._failureCount = 0;
    setTimeout(() => { // defer because can't dispatch inside a reducer
      this.dispatch({type: WS_CONNECTION_OPEN});
      this.dispatch(removeNotification(WS_CLOSED));
    });

    const keys = Object.keys(this._listenMessages);
    for (let i = 0; i < keys.length; i++) {
      this._sendMessage(this._listenMessages[keys[i]].message);
    }
  };

  _handleMessage = (e) => {
    try {
      const data = JSON.parse(e.data);
      if (data.type === 'connection-established') {
        console.info('SOCKET CONNECTION SUCCESS');
      } else {
        console.info(data);
      }
      this.dispatch({type: data.type, payload: data.payload});
      this._notifyListeners({type: data.type, payload: data.payload});
    } catch (error) {
      console.error('SOCKET CONNECTION MESSAGE HANDLING ERROR', error);
    }
  };

  _ping = () => {
    this._sendMessage({type: WS_MESSAGE_PING, payload: {}});
  };

  sendListenMessage(message, forceSend) {
    const messageKey = message.type + '-' + message.payload.id;
    if (!this._listenMessages[messageKey]) {
      this._listenMessages[messageKey] = {
        message,
        listenCount: 1
      };
      this._sendMessage(message);
    } else {
      this._listenMessages[messageKey].listenCount++;
      if (forceSend) {
        this._sendMessage(message);
      }
    }
  }

  stopListenMessage(message) {
    const messageKey = message.type + '-' + message.payload.id;
    if (this._listenMessages[messageKey]) {
      this._listenMessages[messageKey].listenCount--;
      if (!this._listenMessages[messageKey].listenCount) {
        delete this._listenMessages[messageKey];
      }
    }
  }

  _startListenToJob = (jobId, type, forceSend) => {
    invariant(jobId, `Must provide jobId to listen to. Received ${jobId}`);
    const message = {
      type,
      payload: {
        id: jobId
      }
    };
    this.sendListenMessage(message, forceSend);
  };

  _stopListenToJob = (jobId, type, forceSend) => {
    invariant(jobId, `Must provide jobId to listen to. Received ${jobId}`);
    const message = {
      type,
      payload: {
        id: jobId
      }
    };
    this.stopListenMessage(message, forceSend);
  };


  _startListenToReflectionJob = (jobId, reflectionId, type, forceSend) => {
    invariant(jobId, `Must provide jobId to listen to. Received ${jobId}`);
    const message = {
      type,
      payload: {
        id: jobId,
        reflectionId
      }
    };
    this.sendListenMessage(message, forceSend);
  };

  _stopListenToReflectionJob = (jobId, reflectionId, type) => {
    invariant(jobId, `Must provide jobId to listen to. Received ${jobId}`);
    const message = {
      type,
      payload: {
        id: jobId,
        reflectionId
      }
    };
    this.stopListenMessage(message);
  };

  startListenToJobChange(jobId, forceSend) {
    this._startListenToJob(jobId, WS_MESSAGE_JOB_DETAILS_LISTEN, forceSend);
  }

  stopListenToJobChange(jobId) {
    this._stopListenToJob(jobId, WS_MESSAGE_JOB_DETAILS_LISTEN);
  }

  startListenToReflectionJobChange(jobId, reflectionId, forceSend) {
    this._startListenToReflectionJob(jobId, reflectionId, WS_MESSAGE_REFLECTION_JOB_DETAILS_LISTEN, forceSend);
  }

  stopListenToReflectionJobChange(jobId, reflectionId) {
    this._stopListenToReflectionJob(jobId, reflectionId, WS_MESSAGE_REFLECTION_JOB_DETAILS_LISTEN);
  }

  startListenToJobProgress(jobId, forceSend) {
    this._startListenToJob(jobId, WS_MESSAGE_JOB_PROGRESS_LISTEN, forceSend);
  }

  stopListenToJobProgress(jobId) {
    this._stopListenToJob(jobId, WS_MESSAGE_JOB_PROGRESS_LISTEN);
  }

  startListenToQVJobProgress(jobId, forceSend) {
    this._startListenToJob(jobId, WS_MESSAGE_QV_JOB_PROGRESS_LISTEN, forceSend);
  }

  stoptListenToQVJobProgress(jobId) {
    this._stopListenToJob(jobId, WS_MESSAGE_QV_JOB_PROGRESS_LISTEN);
  }

  startListenToReflectionJobProgress(jobId, reflectionId, forceSend) {
    this._startListenToReflectionJob(jobId, reflectionId, WS_MESSAGE_REFLECTION_JOB_PROGRESS_LISTEN, forceSend);
  }

  stopListenToReflectionJobProgress(jobId, reflectionId) {
    this._stopListenToReflectionJob(jobId, reflectionId, WS_MESSAGE_REFLECTION_JOB_PROGRESS_LISTEN);
  }

  startListenToJobRecords(jobId) {
    this._startListenToJob(jobId, WS_MESSAGE_JOB_RECORDS_LISTEN, true);
  }

  stopListenToJobRecords(jobId) {
    this._stopListenToJob(jobId, WS_MESSAGE_JOB_RECORDS_LISTEN);
  }

  _sendMessage(message) {
    if (this.isOpen) {
      this._socket.send(JSON.stringify(message));
    }
  }

  addListener(listener) {
    const id = uuid.v4();
    this._listeners[id] = listener;
    return id;
  }

  removeListener(id) {
    delete this._listeners[id];
  }

  _notifyListeners(msg) {
    for (const id in this._listeners) {
      this._listeners[id](msg);
    }
  }
}

export default new Socket();
