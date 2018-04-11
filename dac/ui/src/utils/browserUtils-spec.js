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
import browserUtils from './browserUtils';

describe('browserUtils', () => {
  describe('hasSupportedBrowserVersion', () => {
    let getPlatformStub;
    beforeEach(() => {
      getPlatformStub = sinon.stub(browserUtils, 'getPlatform');
    });
    afterEach(() => {
      getPlatformStub.restore();
    });
    it('should return true if browser is one of the supported browsers', () => {
      getPlatformStub.returns({
        name: 'Chrome',
        version: '55.0.2883.87'
      });

      expect(browserUtils.hasSupportedBrowserVersion()).to.be.true;
    });
    it('should return false if browser is unsupported', () => {
      getPlatformStub.returns({
        name: 'Chrome',
        version: '41.0.2353.56'
      });

      expect(browserUtils.hasSupportedBrowserVersion()).to.be.false;
    });
    it('should return false if browser version is not last', () => {
      getPlatformStub.returns({
        name: 'IE',
        version: '10.0'
      });

      expect(browserUtils.hasSupportedBrowserVersion()).to.be.false;
    });
  });

  describe('compareVersions', () => {
    it('should return positive number if first version greater than second', () => {
      expect(browserUtils.compareVersions('54.0.2353.56', '40.0.301.0.2')).to.be.above(0);
      expect(browserUtils.compareVersions('54.1.2353.56', '54.0.301.0.2')).to.be.above(0);
      expect(browserUtils.compareVersions('54.1.302.56', '54.1.301.0.2')).to.be.above(0);
      expect(browserUtils.compareVersions('54.1.302.56', '54.1.302')).to.be.above(0);
    });
    it('should return negative number if first version less than second', () => {
      expect(browserUtils.compareVersions('40.0.301.0.2', '54.0.2353.56')).to.be.below(0);
      expect(browserUtils.compareVersions('54.0.301.0.2', '54.1.2353.56')).to.be.below(0);
      expect(browserUtils.compareVersions('54.0.301.0.2', '54.1.302.56')).to.be.below(0);
      expect(browserUtils.compareVersions('54.1.302', '54.1.302.56')).to.be.below(0);
    });
    it('should return 0 if versions are equal', () => {
      expect(browserUtils.compareVersions('40.0.301.0.2', '40.0.301.0.2')).to.be.eql(0);
      expect(browserUtils.compareVersions('54.0.301.0.2', '54.0.301.0.2')).to.be.eql(0);
      expect(browserUtils.compareVersions('54.0.301.0.2', '54.0.301.0.2')).to.be.eql(0);
    });
  });
});
