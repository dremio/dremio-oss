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
import dataset from './dataset';

const resourcePath = '/dataset/myspace.folder.ds1';
const homeResourcePath = '/dataset/"@test_user".folder.ds1';

describe('dataset resourcePath util', () => {
  it('should convert to fullPath', () => {
    expect(dataset.toFullPath(resourcePath)).to.eql('myspace.folder.ds1');
    expect(dataset.toFullPath(homeResourcePath)).to.eql('"@test_user".folder.ds1');
  });

  it('should convert to href', () => {
    expect(dataset.toHref(resourcePath)).to.eql('/space/myspace.folder/ds1');
    expect(dataset.toHref(homeResourcePath)).to.eql('/space/"@test_user".folder/ds1');
  });
});
