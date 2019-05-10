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
import { addParameterToUrl } from '@app/utils/urlUtils';

describe('addParameterToUrl', () => {
  it('adds parameter to url without query params', () => {
    expect(addParameterToUrl('http://localhost:3005/page', 'test', 'aa')).to.be.
      equal('http://localhost:3005/page?test=aa');

  });

  it('adds parameter to url with query params', () => {
    expect(addParameterToUrl('http://localhost:3005/page?p1=1', 'test', 'aa')).to.be.
      equal('http://localhost:3005/page?p1=1&test=aa');
  });

  const rawValue = 'test_;,/?:@&=+$';
  const encodedValue = encodeURIComponent(rawValue);

  it('encodes param name', () => {
    expect(addParameterToUrl('http://localhost:3005/page?p1=1', rawValue, 'aa')).to.be.
      equal(`http://localhost:3005/page?p1=1&${encodedValue}=aa`);
  });

  it('encodes param value', () => {
    expect(addParameterToUrl('http://localhost:3005/page?p1=1', 'test', rawValue)).to.be.
      equal(`http://localhost:3005/page?p1=1&test=${encodedValue}`);
  });
});
