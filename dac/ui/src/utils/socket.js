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
import { WEB_SOCKET_URL } from 'constants/Api';
import localStorageUtils from 'utils/storageUtils/localStorageUtils';
import { addNotification, removeNotification } from 'actions/notification';
import Immutable from 'immutable';

const PING_INTERVAL = 15000;
const CHECK_INTERVAL = 5000;

const WS_MESSAGE_PING = 'ping';
export const WS_MESSAGE_JOB_DETAILS = 'job-details';
export const WS_MESSAGE_JOB_DETAILS_LISTEN = 'job-details-listen';
export const WS_MESSAGE_JOB_PROGRESS = 'job-progress';
export const WS_MESSAGE_JOB_PROGRESS_LISTEN = 'job-progress-listen';

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

  get isOpen() {
    return !!this._socket && this._socket.readyState === WebSocket.OPEN;
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
  }

  _handleConnectionError = (e) => {
    console.error('SOCKET CONNECTION ERROR', e);
    this._failureCount++;
    if (this._failureCount === 6) this.dispatch(addNotification(Immutable.Map({code: WS_CLOSED, messageType: WS_CLOSED}), 'error'));
  }

  _handleConnectionClose = () => {
    console.info('SOCKET CONNECTION CLOSE');
    setTimeout(() => { // defer because can't dispatch inside a reducer
      this.dispatch({type: WS_CONNECTION_CLOSE});
    });
  }

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
  }

  _handleMessage = (e) => {
    try {
      const data = JSON.parse(e.data);
      if (data.type === 'connection-established') {
        console.info('SOCKET CONNECTION SUCCESS');
      } else {
        console.info(data);
      }
      this.dispatch({type: data.type, payload: data.payload});
    } catch (error) {
      console.error('SOCKET CONNECTION MESSAGE HANDLING ERROR', error);
    }
  }

  _ping = () => {
    this._sendMessage({type: WS_MESSAGE_PING, payload: {}});
  }

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

  startListenToJobChange(jobId, forceSend) {
    invariant(jobId, `Must provide jobId to listen to. Received ${jobId}`);
    const message = {
      type: WS_MESSAGE_JOB_DETAILS_LISTEN,
      payload: {
        id: jobId
      }
    };
    this.sendListenMessage(message, forceSend);
  }

  stopListenToJobChange(jobId) {
    invariant(jobId, `Must provide jobId to stop listen to. Received ${jobId}`);
    const message = {
      type: WS_MESSAGE_JOB_DETAILS_LISTEN,
      payload: {
        id: jobId
      }
    };
    this.stopListenMessage(message);
  }

  startListenToJobProgress(jobId, forceSend) {
    invariant(jobId, `Must provide jobId to listen to. Received ${jobId}`);
    const message = {
      type: WS_MESSAGE_JOB_PROGRESS_LISTEN,
      payload: {
        id: jobId
      }
    };
    this.sendListenMessage(message, forceSend);
  }

  stopListenToJobProgress(jobId) {
    invariant(jobId, `Must provide jobId to stop listen to. Received ${jobId}`);
    const message = {
      type: WS_MESSAGE_JOB_PROGRESS_LISTEN,
      payload: {
        id: jobId
      }
    };
    this.stopListenMessage(message);
  }

  _sendMessage(message) {
    if (this._socket.readyState === WebSocket.OPEN) {
      this._socket.send(JSON.stringify(message));
    }
  }
}

export default new Socket();
