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
import { put } from 'redux-saga/effects';
import { updateViewState } from 'actions/resources';

import { addNotification } from 'actions/notification';
import { handleDownloadFile } from './downloadFile';

describe('downloadFile saga', () => {

  describe('handleDownloadFile', () => {
    let gen;
    let next;
    beforeEach(() => {
      gen = handleDownloadFile({
        meta: {
          downloadUrl: 'foo',
          viewId: 'viewId'
        }
      });
    });

    it('should call fetch, getFileDownloadConfigFromResponse, and downloadFile', () => {
      next = gen.next();
      expect(next.value).to.eql(put(updateViewState('viewId', { isInProgress: true })));
      next = gen.next();
      expect(next.value.CALL).to.not.be.undefined; // call fetch
      next = gen.next();
      expect(next.value.CALL).to.not.be.undefined; // call getFileDownloadConfigFromResponse
      next = gen.next();
      expect(next.value.CALL).to.not.be.undefined; // call downloadFile
      next = gen.next();
      expect(next.value).to.eql(put(updateViewState('viewId', { isInProgress: false })));
      next = gen.next();
      expect(next.done).to.be.true;
    });

    it('should put(addNotification) if download fails', () => {
      next = gen.next();
      expect(next.value).to.eql(put(updateViewState('viewId', { isInProgress: true })));
      next = gen.next();
      expect(next.value.CALL).to.not.be.undefined; // call fetch
      next = gen.next();
      expect(next.value.CALL).to.not.be.undefined; // call getFileDownloadConfigFromResponse
      next = gen.throw(new Error('someMessage'));
      expect(next.value).to.eql(put(addNotification('someMessage', 'error')));
      next = gen.next();
      expect(next.value).to.eql(put(updateViewState('viewId', { isInProgress: false, isFailed: true })));
      next = gen.next();
      expect(next.done).to.be.true;
    });
  });
});
