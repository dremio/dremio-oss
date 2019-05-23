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
import { useTestIntercomApp } from './intercomUtils';
import config from './config';

const testCase = ([valueToTest, expectedResult]) => {
  it(`'${valueToTest}' should${expectedResult ? '' : ' NOT'} use test intercom app`, () => {
    expect(useTestIntercomApp(valueToTest)).to.be.equal(expectedResult);
  });
};

// see DX-16408 for details
const matchTestPattern = [
  'a@dremio.com',
  'b@dremio.test',
  'sss@test.com',
  'asd@Dremio.com',
  'ff@dreMio.test',
  'asdfsadf@test.COM',
  '    asdfsadf@test.COM  '
];

const doesNotMatchTestPattern = [
  'asd',
  'a@drem1io.com',
  'a@dremio.ru'
];

const setEnvironmentMode = isReleaseBuild => {
  let prevValue;
  beforeEach(() => {
    prevValue = config.isReleaseBuild;
    config.isReleaseBuild = isReleaseBuild;
  });

  afterEach(() => {
    config.isReleaseBuild = prevValue;
    prevValue = null;
  });
};


describe('useTestIntercomApp', () => {
  describe('Dev environment', () => {
    setEnvironmentMode(false);

    [
      ...matchTestPattern,
      ...doesNotMatchTestPattern
    ].map(email => [email, true]) // all emails on dev environment should use test app
      .map(testCase);
  });

  describe('Release environment', () => {
    setEnvironmentMode(true);

    describe('positive cases', () => {
      matchTestPattern.map(email => [email, true])
        .map(testCase);
    });
    describe('negative cases', () => {
      doesNotMatchTestPattern.map(email => [email, false])
        .map(testCase);
    });
  });
});
