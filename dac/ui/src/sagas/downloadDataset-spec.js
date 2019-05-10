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
import { call } from 'redux-saga/effects';
import socket from 'utils/socket';

import { handleStartDatasetDownload } from './downloadDataset';
import { handleDownloadFile } from './downloadFile';

describe('downloadDataset saga', () => {

  describe('handleStartDatasetDownload', () => {
    let gen;
    let next;
    beforeEach(() => {
      gen = handleStartDatasetDownload({meta: {dataset: 'foo', format: 'text/csv'}});
    });

    it('should not show modal when fast or should show and hide when download completes if it is opened', () => {
      next = gen.next();
      expect(next.value.PUT).to.not.be.undefined; //start download
      next = gen.next();
      expect(typeof next.value.RACE.response).to.not.be.undefined;
      expect(typeof next.value.RACE.locationChange).to.not.be.undefined;

      const response = {payload: {downloadUrl: 'fooUrl', jobId: {id: 'fooId'}}};
      next = gen.next({response});
      expect(next.value).to.eql(call([socket, socket.startListenToJobProgress], 'fooId'));

      next = gen.next();
      expect(typeof next.value.RACE.jobDone).to.not.be.undefined;
      expect(typeof next.value.RACE.modalShownAndClosed).to.not.be.undefined;

      next = gen.next({jobDone: {payload: {update: {state: 'COMPLETED'}}}});
      expect(next.value.PUT).to.not.be.undefined; // hide modal
      next = gen.next();
      expect(next.value).to.eql(call(handleDownloadFile, { meta: { url: 'fooUrl' }}));
      next = gen.next();
      expect(next.done).to.be.true;
    });
    it('should show modal when slow and do not hide when it already closed', () => {
      next = gen.next();
      expect(next.value.PUT).to.not.be.undefined; //start download
      next = gen.next();
      expect(typeof next.value.RACE.response).to.not.be.undefined;
      expect(typeof next.value.RACE.locationChange).to.not.be.undefined;

      const response = {payload: {downloadUrl: 'fooUrl', jobId: {id: 'fooId'}}};
      next = gen.next({response});
      expect(next.value).to.eql(call([socket, socket.startListenToJobProgress], 'fooId'));

      next = gen.next();
      expect(typeof next.value.RACE.jobDone).to.not.be.undefined;
      expect(typeof next.value.RACE.modalShownAndClosed).to.not.be.undefined;

      next = gen.next({modalShownAndClosed: true});
      expect(next.value.TAKE).to.not.be.undefined; // getJobDoneActionFilter
      next = gen.next();
      expect(next.value).to.eql(call(handleDownloadFile, { meta: { url: 'fooUrl' }}));
      next = gen.next();
      expect(next.done).to.be.true;
    });
    it('should not download job in case websocket returns job error', () => {
      next = gen.next();
      expect(next.value.PUT).to.not.be.undefined; //start download
      next = gen.next();
      expect(typeof next.value.RACE.response).to.not.be.undefined;
      const response = {payload: {downloadUrl: 'fooUrl', jobId: {id: 'fooId'}}};
      next = gen.next({response});
      expect(next.value).to.eql(call([socket, socket.startListenToJobProgress], 'fooId')); //ws listens
      next = gen.next(); //ws response jobDone
      expect(typeof next.value.RACE.jobDone).to.not.be.undefined;
      expect(typeof next.value.RACE.modalShownAndClosed).to.not.be.undefined;

      next = gen.next({jobDone: {payload: {update: {state: 'ERROR'}}}});
      expect(next.value.PUT).to.not.be.undefined; // hide modal
      next = gen.next();
      expect(next.value.PUT.action.type).to.equal('ADD_NOTIFICATION');
      next = gen.next();
      expect(next.done).to.equal(true); //
    });
  });
});
