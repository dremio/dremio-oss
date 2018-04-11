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
import history from './history';

const previousLocation = {pathname: '/space/myspace/ds1', query: {version: '1234567890'}};
const resourcePath = '/dataset/myspace."folder.name".ds1/version/00a7024a0e8d4519';
const homeResourcePath = '/dataset/"@test_user".folder.ds1/version/00a7024a0e8d4519';

/**
 * History hrefs are hacky right now because it uses the href of the dataset with different versions instead of one of
 * its own. Therefore in order to generate the client url, it needs the dataset's href, which is passed in as
 * 'previousLocation'
 */

describe('history resourcePath util', () => {
  it('can convert to fullPath', () => {
    expect(history.toFullPath(resourcePath, previousLocation)).to.eql('myspace."folder.name".ds1');
    expect(history.toFullPath(homeResourcePath, previousLocation)).to.eql('"@test_user".folder.ds1');
  });

  it('can convert to href', () => {
    expect(history.toHref(resourcePath, previousLocation)).to.eql(
      '/space/myspace/ds1?version=00a7024a0e8d4519&history=true');
    expect(history.toHref(homeResourcePath, previousLocation)).to.eql(
      '/space/myspace/ds1?version=00a7024a0e8d4519&history=true');
  });
});
