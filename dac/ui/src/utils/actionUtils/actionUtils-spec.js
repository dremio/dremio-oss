/*
 * Copyright (C) 2017 Dremio Corporation
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
});
