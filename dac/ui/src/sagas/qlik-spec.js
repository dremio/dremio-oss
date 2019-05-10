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
import { call, put } from 'redux-saga/effects';
import qsocks from 'qsocks';

import { API_URL_V2 } from 'constants/Api';
import * as Actions from 'actions/explore/download';
import { SHOW_CONFIRMATION_DIALOG } from 'actions/confirmation';
import * as QlikActions from 'actions/qlik';
import localStorageUtils from 'utils/storageUtils/localStorageUtils';


import { openQlikSense, fetchQlikApp, checkDsnList, requestPassword, getConnectionConfig, DSN,
  getUniqueAppName, santizeAppName } from './qlik';

describe('qlik saga', () => {

  const dataset = Immutable.fromJS({
    displayFullPath: ['myspace', 'foo'],
    apiLinks: {
      qlik: '/qlik/myspace.foo'
    }
  });

  const qlikGlobal = {
    createApp: () => {},
    openDoc: () => {},
    getOdbcDsns: () => {},
    connection: {
      close: () => {}
    },
    getDocList: () => {}
  };

  const qlikConfig = 'qlikConfig';

  const qlikApp = {
    qAppId: 'qAppId'
  };

  const qlikDoc = {
    createConnection: () => {},
    setScript: () => {},
    createObject: () => {},
    doSave: () => {}
  };

  describe('fetchQlikApp', () => {
    let gen;
    beforeEach(() => {
      gen = fetchQlikApp(dataset);
    });

    it('should put actions, call fetch and getFileDownloadConfigFromResponse', () => {
      let next = gen.next();
      // denis s notes: when we run tests in browser, looks like chai compares "next.value" with
      // "put({type: Actions.LOAD_QLIK_APP_START})" by prototype too.
      //So, when we use JSON.stringify, we cut prototypes
      expect(JSON.stringify(next.value)).to.eql(JSON.stringify(put({type: Actions.LOAD_QLIK_APP_START})));

      next = gen.next();
      const headers = new Headers();
      headers.append('Accept', 'text/plain+qlik-app');
      if (localStorageUtils) {
        headers.append('Authorization', localStorageUtils.getAuthToken());
      }
      expect(JSON.stringify(next.value)).to.eql(
        JSON.stringify(call(fetch, `${API_URL_V2}/qlik/myspace.foo`, {method: 'GET', headers}))
      );

      next = gen.next({
        ok: true,
        text() {
          return 'foo';
        }
      });
      expect(next.value).to.eql('foo');
    });

    it('should throw if the fetch response is not ok', () => {
      gen.next();
      gen.next();
      gen.next({
        ok: false,
        text() {
          return 'foo';
        }
      });
      expect(() => {
        gen.next();
      }).to.throw(Error);
    });
  });

  describe('checkDsnList', () => {
    let gen;
    beforeEach(() => {
      gen = checkDsnList(qlikGlobal, dataset);
    });

    it('should call getOdbcDsns', () => {
      let next = gen.next();
      expect(next.value).to.eql(call([qlikGlobal, qlikGlobal.getOdbcDsns]));
      next = gen.next([{}, {qName: DSN}]);
      expect(next.value).to.eql(true);
      expect(next.done).to.eql(true);
    });
  });

  describe('openQlikSense', () => {
    let gen;
    beforeEach(() => {
      gen = openQlikSense({payload: dataset});
    });

    it('should return if no password', () => {
      let next = gen.next(); //select username
      next = gen.next('theUser'); //requestPassword
      next = gen.next('');
      expect(next.done).to.be.true;
    });

    it('should succeed', () => {
      let next = gen.next(); //select username
      next = gen.next('theUser'); //requestPassword
      next = gen.next('thePassword'); //showQlikModal
      next = gen.next(); // showQlikProgress
      next = gen.next(); // hideQlikError
      next = gen.next(); // fetchQlikApp
      next = gen.next(qlikConfig);
      expect(next.value).to.eql(call([qsocks, qsocks.Connect]));
      next = gen.next(qlikGlobal);
      next = gen.next(true); // checkDsnList
      next = gen.next(); // getDocList
      expect(next.value).to.eql(
        call([qlikGlobal, qlikGlobal.createApp], 'Dremio ' + dataset.get('displayFullPath').join('.')));
      next = gen.next(qlikApp); // createApp
      next = gen.next(qlikDoc); // openDoc
      next = gen.next(); // createConnection
      expect(next.value).to.eql(call([qlikDoc, qlikDoc.setScript], qlikConfig));
      next = gen.next(); // setScript
      next = gen.next(); // createObject
      next = gen.next(); // doSave
      expect(next.value).to.eql(put({
        type: QlikActions.QLIK_APP_CREATION_SUCCESS,
        info: {
          appId: 'qAppId',
          appName: 'Dremio myspace.foo'
        }
      }));
    });

    describe('errors', () => {
      let next;
      beforeEach(() => {
        next = gen.next(); //select username
        next = gen.next('theUser'); //requestPassword
        next = gen.next('thePassword'); //showQlikModal
        next = gen.next(); // showQlikProgress
        next = gen.next(); // hideQlikError
      });

      it('should showQlikErrorModal when fetchClickApp fails', () => {
        next = gen.next(); // fetchQlikApp
        next = gen.throw('error');
        expect(next.value.PUT.action.payload.error.get('code')).to.eql('QLIK_GET_APP');
        next = gen.next();
        expect(next.done).to.be.true;
      });

      it('should showQlikErrorModal when qsocks.Connect fails', () => {
        next = gen.next(); // fetchQlikApp
        next = gen.next(); // qsocks.Connect
        next = gen.throw('error');
        expect(next.value.PUT.action.payload.error.get('code')).to.eql('QLIK_CONNECT_FAILED');
        next = gen.next();
        expect(next.done).to.be.true;
      });

      it('should showQlikErrorModal when dsn fails', () => {
        next = gen.next(); // fetchQlikApp
        next = gen.next(); // qsocks.Connect
        next = gen.next(); // checkDsnList
        next = gen.next(false); // dsn not detected
        expect(next.value.PUT.action.payload.error.get('code')).to.eql('QLIK_DSN');
        next = gen.next();
        expect(next.done).to.be.true;
      });

      it('should showQlikErrorModal when Qlik request throws', () => {
        next = gen.next(); // fetchQlikApp
        next = gen.next(); // qsocks.Connect
        next = gen.next(); // dsn
        next = gen.throw({message: 'foo'});
        expect(next.value.PUT.action.payload.error.get('code')).to.eql('QLIK_CUSTOM_ERROR');
        expect(next.value.PUT.action.payload.error.get('moreInfo')).to.eql('foo');
        next = gen.next();
        expect(next.done).to.be.true;
      });

    });
  });

  describe('requestPassword', () => {
    it('should showConfirmationDialog with prompt and return the password', () => {
      const gen = requestPassword();
      let next = gen.next();
      const action = next.value.PUT.action;
      expect(action.type).to.eq(SHOW_CONFIRMATION_DIALOG);
      expect(action.showPrompt).to.be.true;

      next = gen.next(); // select(getLocation)

      next = gen.next();
      expect(next.value.RACE).to.not.be.undefined;
      next = gen.next({password: 'thePassword'});
      expect(next.value).to.be.equal('thePassword');
      expect(next.done).to.be.true;
    });

    it('should return undefined if prompt is cancelled', () => {
      const gen = requestPassword();
      let next = gen.next();
      const action = next.value.PUT.action;
      expect(action.type).to.eq(SHOW_CONFIRMATION_DIALOG);
      expect(action.showPrompt).to.be.true;

      next = gen.next(); // select(getLocation)

      next = gen.next();
      expect(next.value.RACE).to.not.be.undefined;
      next = gen.next({locationChange: true});
      expect(next.value).to.be.undefined;
      expect(next.done).to.be.true;
    });
  });

  describe('getConnectionConfig', () => {
    let params;
    let result;
    beforeEach(() => {
      params = [
        'theHostname',
        'theUsername',
        'thePassword'
      ];
      result = getConnectionConfig(...params);
    });

    it('should include hostname in qConnectionString', () => {
      expect(result.qConnectionString).to.contain(`HOST=${params[0]}`);
    });

    it('should include username and password', () => {
      expect(result.qUserName).to.equal(params[1]);
      expect(result.qPassword).to.equal(params[2]);
    });
  });

  describe('santizeAppName', () => {
    it('should remove the correct characters', () => {
      expect(santizeAppName('test name')).equals('test name');
      expect(santizeAppName('*test""name\\:><')).equals('_test_name_');
    });
  });

  describe('getUniqueAppName', () => {
    it('should return hello (2)', () => {
      const docList = [{qTitle: 'hello'}, {qTitle: 'hello (1)'}];

      expect(getUniqueAppName('hello', docList)).equals('hello (2)');
    });
  });
});
