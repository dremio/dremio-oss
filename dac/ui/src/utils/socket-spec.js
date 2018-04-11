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
import { Socket } from './socket';

describe('Socket', () => {
  let socket;
  let mockWs;
  const message = {type: 'foo', payload: { id: 123 }};
  beforeEach(() => {
    socket = new Socket();
    mockWs = {
      send: sinon.spy(),
      close: sinon.spy(),
      readyState: WebSocket.OPEN
    };
    socket.dispatch = sinon.stub();
  });

  describe('#open', () => {
    let clock;
    beforeEach(() => {
      clock = sinon.useFakeTimers();
      sinon.stub(socket, '_createConnection');
      sinon.stub(socket, '_ping');
      sinon.stub(socket, '_checkConnection');
    });

    afterEach(() => {
      clock.restore();
    });

    it('should call _createConnection', () => {
      socket.open();
      expect(socket._createConnection).to.be.calledOnce;
    });

    it('should set ping and check intervals', () => {
      socket.open();
      expect(socket._ping).to.not.be.called;
      expect(socket._checkConnection).to.not.be.called;

      clock.tick(60000);
      expect(socket._ping).to.be.called;
      expect(socket._checkConnection).to.be.called;
    });
  });

  describe('#close', () => {
    it('should close the WS and clear out local state', () => {
      Object.assign(socket, { // eslint-disable-line no-restricted-properties
        _socket: mockWs,
        _checkId: 5,
        _pingId: 10,
        _listenMessages: {a: 1}
      });
      sinon.spy(global, 'clearInterval');

      socket.close();

      expect(mockWs.close).to.have.been.called;
      expect(socket._socket).to.be.null;
      expect(socket._listenMessages).to.be.eql({});
      expect(global.clearInterval).to.have.been.calledWith(5);
      expect(global.clearInterval).to.have.been.calledWith(10);

      global.clearInterval.restore();
    });

    it('should not throw in not open', () => {
      socket.close();
    });
  });

  describe('#_checkConnection', () => {
    it('should call _createConnection if socket not OPEN', () => {
      sinon.stub(socket, '_createConnection');

      socket._socket = mockWs;
      socket._createConnection();
      expect(socket._createConnection).to.be.calledOnce;

      mockWs.readyState = WebSocket.CLOSED;
      socket._createConnection();
      expect(socket._createConnection).to.be.calledTwice;
    });
  });

  describe('#sendListenMessage', () => {
    it('should send listen message and adds to _listenMessages', () => {
      sinon.stub(socket, '_sendMessage').returns(true);
      socket.sendListenMessage(message);
      expect(socket._sendMessage).to.be.calledWith(message);
    });

    it('should do nothing for repeated listens, unless forceSend is set', () => {
      sinon.stub(socket, '_sendMessage').returns(true);
      socket.sendListenMessage(message);
      expect(socket._sendMessage).to.be.calledOnce;
      socket.sendListenMessage(message);
      expect(socket._sendMessage).to.be.calledOnce;

      socket.sendListenMessage(message, true);
      expect(socket._sendMessage).to.be.calledTwice;
    });
  });

  describe('#_handleConnectionEstablished', () => {
    it('should resend listen messages', () => {
      sinon.stub(socket, '_sendMessage');
      socket.sendListenMessage(message);
      socket._sendMessage.reset();

      socket._handleConnectionEstablished();
      expect(socket._sendMessage).to.be.calledOnce;
      expect(socket._sendMessage).to.be.calledWith(message);
    });
  });

  describe('#stopListenMessage', () => {
    it('should remove message from _listenMessages if listenCount === 1', () => {
      socket._listenMessages = {
        'foo-123': {
          message: 'message',
          listenCount: 1
        }
      };
      expect(socket._listenMessages).to.have.property('foo-123');
      socket.stopListenMessage(message);
      expect(socket._listenMessages).to.not.have.property('foo-123');
    });
    it('should decrease listenCount for message if listenCount > 1', () => {
      socket._listenMessages = {
        'foo-123': {
          message: 'message',
          listenCount: 2
        }
      };
      expect(socket._listenMessages).to.have.property('foo-123');
      socket.stopListenMessage(message);
      expect(socket._listenMessages).to.have.property('foo-123');
      expect(socket._listenMessages['foo-123'].listenCount).to.eql(1);
    });
  });

  describe('#_sendMessage', () => {
    it('should send if socket is open', () => {
      socket._socket = mockWs;
      socket._sendMessage(message);
      expect(mockWs.send).to.be.calledWith(JSON.stringify(message));
    });
  });
});
