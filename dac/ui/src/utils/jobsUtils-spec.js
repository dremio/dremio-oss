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
import jobsUtils from './jobsUtils';

describe('jobsUtils', () => {
  describe('getFormattedRecords check', () => {
    it('should return simple number if input < 1000', () => {
      expect(jobsUtils.getFormattedRecords(990)).to.be.eql(990);
      expect(jobsUtils.getFormattedRecords(880)).to.be.eql(880);
      expect(jobsUtils.getFormattedRecords(1)).to.be.eql(1);
      expect(jobsUtils.getFormattedRecords(0)).to.be.eql(0);
      expect(jobsUtils.getFormattedRecords(12)).to.be.eql(12);
    });

    it('should return ~ number of thousand if input < 1000 000 but bigger 1000', () => {
      expect(jobsUtils.getFormattedRecords(1000)).to.be.eql('1,000');
      expect(jobsUtils.getFormattedRecords(2000)).to.be.eql('2,000');
      expect(jobsUtils.getFormattedRecords(995500)).to.be.eql('995,500');
      expect(jobsUtils.getFormattedRecords(995600)).to.be.eql('995,600');
      expect(jobsUtils.getFormattedRecords(995400)).to.be.eql('995,400');
      expect(jobsUtils.getFormattedRecords(1200)).to.be.eql('1,200');
      expect(jobsUtils.getFormattedRecords(34400)).to.be.eql('34,400');
    });

    it('should return ~ number of million if bigger 1000 000', () => {
      expect(jobsUtils.getFormattedRecords(1000000)).to.be.eql('1,000,000');
      expect(jobsUtils.getFormattedRecords(12000000)).to.be.eql('12,000,000');
      expect(jobsUtils.getFormattedRecords(99500000)).to.be.eql('99,500,000');
      expect(jobsUtils.getFormattedRecords(99400000)).to.be.eql('99,400,000');
      expect(jobsUtils.getFormattedRecords(99000000)).to.be.eql('99,000,000');
    });

    it('should return empty string if we have invalide input', () => {
      expect(jobsUtils.getFormattedRecords(NaN)).to.be.eql('');
      expect(jobsUtils.getFormattedRecords()).to.be.eql('');
      expect(jobsUtils.getFormattedRecords('blabla')).to.be.eql('');
    });
  });
});
