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
import actionUtils from './actionUtils';

describe('actionUtils', () => {
  describe('shouldLoad', () => {
    it('should return false when isInProgress', () => {
      expect(actionUtils.shouldLoad({isInProgress: true, isInvalid: true})).to.be.false;
    });
    it('should return true when not isInProgress and isInvalid', () => {
      expect(actionUtils.shouldLoad({isInProgress: false, isInvalid: true})).to.be.true;
    });
    it('should accept Immutable map', () => {
      expect(actionUtils.shouldLoad(Immutable.fromJS({isInProgress: false, isInvalid: true}))).to.be.true;
    });
  });
  describe('humanizeNotificationMessage', () => {

    it('should return function on first call', () => {
      const errorMessage = 'error';
      expect(actionUtils.humanizeNotificationMessage(errorMessage)).to.be.a('function');
    });

    it('should set notification when provide errorMessage on first call and moreInfo from payload', () => {
      const errorMessage = 'error';
      const moreInfo = 'more info';
      const payload = { response: { errorMessage: 'message', moreInfo } };
      const result = {
        message: Immutable.Map({ message: errorMessage, moreInfo }),
        level: 'error'
      };
      expect(actionUtils.humanizeNotificationMessage(errorMessage)(payload)).to.be.eql(result);
    });

    it('should set notification with errorMessage and moreInfo from payload when no errorMessage on first call', () => {
      const moreInfo = 'more info';
      const payload = { response: { errorMessage: 'message', moreInfo } };
      const result = {
        message: Immutable.Map({ message: 'message', moreInfo }),
        level: 'error'
      };
      expect(actionUtils.humanizeNotificationMessage()(payload)).to.be.eql(result);
    });

    it('should set notification with default when there is no errorMessage and payload is empty ', () => {
      const defaultMessage = 'Something went wrong.';
      const payload = {};
      const result = {
        message: Immutable.Map({ message: defaultMessage }),
        level: 'error'
      };
      expect(actionUtils.humanizeNotificationMessage()(payload)).to.be.eql(result);
    });

    it('should set notification with message and moreInfo to "data has changed" when status = 409', () => {
      const errorMessage = 'error';
      const defaultMessage409 = 'The data has been changed since you last accessed it. Please reload the page.';
      const payload = { status: 409 };
      const result = {
        message: Immutable.Map({ message: errorMessage, moreInfo: defaultMessage409 }),
        level: 'error'
      };
      expect(actionUtils.humanizeNotificationMessage(errorMessage)(payload)).to.be.eql(result);
    });
  });
});
