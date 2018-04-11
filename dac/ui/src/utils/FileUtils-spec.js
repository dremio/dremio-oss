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
import FileUtils, { FileDownloadError } from './FileUtils';

describe('FileUtils', () => {

  describe('getFileNameFromResponse', () => {
    it('should default to "download" when content disposition is missing or does not match', () => {
      expect(FileUtils.getFileNameFromResponse({
        headers: new Map()
      })).to.equal('download');

      expect(FileUtils.getFileNameFromResponse({
        headers: new Map([['Content-Disposition', 'non-matching']])
      })).to.equal('download');
    });

    it('should geturn filename from contentDisposition', () => {
      expect(FileUtils.getFileNameFromResponse({
        headers: new Map([['Content-Disposition', 'attachment; filename="theFileName"']])
      })).to.equal('theFileName');
    });

  });

  describe('getFileDownloadConfigFromResponse', () => {
    beforeEach(() => {
      sinon.stub(FileUtils, 'getFileNameFromResponse').returns('theFileName');
    });

    afterEach(() => {
      FileUtils.getFileNameFromResponse.restore();
    });

    describe('!response.ok', () => {
      it('should throw response body is response is json with .errorMessage', () => {
        return expect(FileUtils.getFileDownloadConfigFromResponse({
          ok: false,
          json: () => {
            return Promise.resolve({errorMessage: 'foo'});
          }
        })).to.be.rejectedWith(FileDownloadError, Immutable.fromJS({errorMessage: 'foo'}));
      });

      it('should throw FileDownloadError with Download  if response json is missing .errorMessage', () => {
        return expect(FileUtils.getFileDownloadConfigFromResponse({
          ok: false,
          statusText: 'theStatusText',
          json: () => {
            return Promise.resolve({foo: 'bar'});
          }
        })).to.be.rejectedWith(FileDownloadError, 'Download failed: theStatusText');
      });

      it('should throw FileDownloadError with Download failed of json() fails', () => {
        return expect(FileUtils.getFileDownloadConfigFromResponse({
          ok: false,
          statusText: 'theStatusText',
          json: () => {
            return Promise.reject(new Error('foo'));
          }
        })).to.be.rejectedWith(FileDownloadError, 'Download failed: theStatusText');
      });
    });


    it('should throw if response.status is 204 or 205', () => {
      expect(() => FileUtils.getFileDownloadConfigFromResponse({ok: true, status: 204})).to.throw;
      expect(() => FileUtils.getFileDownloadConfigFromResponse({ok: true, status: 205})).to.throw;
    });

    it('should return promise, resolving to blob and filename', () => {
      return expect(FileUtils.getFileDownloadConfigFromResponse({
        ok: true,
        status: 200,
        headers: new Map([['Content-Type', 'theContentType'], ['Content-Disposition', 'non-matching']]),
        blob: () => Promise.resolve('theBlob')})
      ).to.eventually.eql({
        blob: 'theBlob', fileName: 'theFileName'
      });
    });
  });

  describe('getFormattedBytes', () => {
    it('should return number of Bytes if input < 1024', () => {
      expect(FileUtils.getFormattedBytes(1023)).to.be.eql('1023 B');
      expect(FileUtils.getFormattedBytes(100)).to.be.eql('100 B');
      expect(FileUtils.getFormattedBytes(1)).to.be.eql('1 B');
      expect(FileUtils.getFormattedBytes(0)).to.be.eql('0 B');
      expect(FileUtils.getFormattedBytes(12)).to.be.eql('12 B');
    });

    it('should return ~ number KB if input < 1 MB but >= 1 KB, should be two numbers ofter dot', () => {
      expect(FileUtils.getFormattedBytes(1024)).to.be.eql('1.00 KB');
      expect(FileUtils.getFormattedBytes(2048)).to.be.eql('2.00 KB');
      expect(FileUtils.getFormattedBytes(4096)).to.be.eql('4.00 KB');
      expect(FileUtils.getFormattedBytes(995600)).to.be.eql('972.27 KB');
      expect(FileUtils.getFormattedBytes(995400)).to.be.eql('972.07 KB');
      expect(FileUtils.getFormattedBytes(1200)).to.be.eql('1.17 KB');
      expect(FileUtils.getFormattedBytes(1025)).to.be.eql('1.00 KB');
    });

    it('should return ~ number MB if input >= 1 MB but < 1 GB, should be two numbers ofter dot', () => {
      expect(FileUtils.getFormattedBytes(2000000)).to.be.eql('1.91 MB');
      expect(FileUtils.getFormattedBytes(12000000)).to.be.eql('11.44 MB');
      expect(FileUtils.getFormattedBytes(99500000)).to.be.eql('94.89 MB');
    });

    it('should return ~ number GB if input >= 1 GB but < 1 TB, should be two numbers ofter dot', () => {
      expect(FileUtils.getFormattedBytes(2000000000)).to.be.eql('1.86 GB');
      expect(FileUtils.getFormattedBytes(12000000000)).to.be.eql('11.18 GB');
      expect(FileUtils.getFormattedBytes(99500000000)).to.be.eql('92.67 GB');
    });

    it('should return ~ number TB if input >= 1 TB, should be two numbers ofter dot', () => {
      expect(FileUtils.getFormattedBytes(2000000000000)).to.be.eql('1.82 TB');
      expect(FileUtils.getFormattedBytes(12000000000000)).to.be.eql('10.91 TB');
      expect(FileUtils.getFormattedBytes(99500000000000000)).to.be.eql('90494.72 TB');
    });

    it('should return empty string if we have invalide input', () => {
      expect(FileUtils.getFormattedBytes(NaN)).to.be.eql('');
      expect(FileUtils.getFormattedBytes()).to.be.eql('');
      expect(FileUtils.getFormattedBytes('blabla')).to.be.eql('');
    });
  });
});
