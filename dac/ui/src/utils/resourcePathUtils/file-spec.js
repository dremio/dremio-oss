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
import file from './file';

const sourceResourcePath = '/source/LocalFS1/file/LocalFS1.bin.bash';
const homeResourcePath = '/file/"@test_user".folder.test';
const escapedResourcePath = '/source/LocalFS1/file/LocalFS1.Users."tests_nightwatch".files."test.csv"';

describe('file resourcePath util', () => {
  it('should convert to fullPath', () => {
    expect(file.toFullPath(sourceResourcePath)).to.eql('LocalFS1.bin.bash');
    expect(file.toFullPath(homeResourcePath)).to.eql('"@test_user".folder.test');
    expect(file.toFullPath(escapedResourcePath)).to.eql('LocalFS1.Users."tests_nightwatch".files."test.csv"');
  });

  it('should convert to href', () => {
    expect(file.toHref(sourceResourcePath)).to.eql('/source/LocalFS1.bin/bash');
    expect(file.toHref(homeResourcePath)).to.eql('/space/"@test_user".folder/test');
    expect(file.toHref(escapedResourcePath)).to.eql('/source/LocalFS1.Users."tests_nightwatch".files/test.csv');
  });
});
