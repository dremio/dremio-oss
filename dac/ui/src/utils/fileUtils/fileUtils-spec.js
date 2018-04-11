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
import FileUtils from './fileUtils';

describe('#FileUtils', () => {

  describe('#convertFileSize', () => {

    it('should return kb when size less then 1 kb', function() {
      expect(FileUtils.convertFileSize(1023)).to.be.equal('1.0KB');
    });

    it('should return 1kb when size is 1024 bytes', function() {
      expect(FileUtils.convertFileSize(1024)).to.be.equal('1.0KB');
    });

    it('should return kb when size less then 1 mb', function() {
      expect(FileUtils.convertFileSize(1024 * 1023)).to.be.equal('1023.0KB');
    });

    it('should return 1mb when size is 1024kb', function() {
      expect(FileUtils.convertFileSize(1024 * 1024)).to.be.equal('1.0MB');
    });

    it('should return mb when size less then 1 gb', function() {
      expect(FileUtils.convertFileSize(1024 * 1024 * 1023)).to.be.equal('1023.0MB');
    });

    it('should return gb when size more then 1 gb', function() {
      expect(FileUtils.convertFileSize(1025 * 1024 * 1024)).to.be.equal('1.0GB');
    });
  });
});
