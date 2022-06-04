/*
 * Copyright (C) 2017-2019 Dremio Corporation
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

/* eslint no-useless-escape: 0 */

import { SqlStringUtils } from './sqlStringUtils';

describe('SqlStringUtils', () => {
  it('should return an empty array', () => {
    expect(SqlStringUtils('')).to.deep.equal([]);
  });

  it('should return a simple statement', () => {
    expect(SqlStringUtils('SELECT * FROM FOO')).to.deep.equal([
      'SELECT * FROM FOO'
    ]);
  });

  it('should parse two statements', () => {
    expect(
      SqlStringUtils('SELECT * FROM FOO; SELECT * FROM BAR')
    ).to.deep.equal(['SELECT * FROM FOO', ' SELECT * FROM BAR']);
  });

  it('should parse two statements that have an ending semicolon', () => {
    expect(
      SqlStringUtils('SELECT * FROM FOO; SELECT * FROM BAR;')
    ).to.deep.equal(['SELECT * FROM FOO', ' SELECT * FROM BAR']);
  });

  it('should parse two statements with double quotes', () => {
    expect(
      SqlStringUtils('SELECT * FROM \"FOO;BAR\"; SELECT * FROM BAR;')
    ).to.deep.equal(['SELECT * FROM \"FOO;BAR\"', ' SELECT * FROM BAR']);
  });

  it('should parse two statements with escaped double quotes', () => {
    expect(
      SqlStringUtils('SELECT * FROM \"FOO\"\";BAR\"; SELECT * FROM BAR;')
    ).to.deep.equal(['SELECT * FROM \"FOO\"\";BAR\"', ' SELECT * FROM BAR']);
  });

  it('should parse two statements with brackets', () => {
    expect(
      SqlStringUtils('SELECT * FROM [FOO]];BAR]; SELECT * FROM BAR;')
    ).to.deep.equal(['SELECT * FROM [FOO]];BAR]', ' SELECT * FROM BAR']);
  });

  it('should parse statements with a comment', () => {
    expect(
      SqlStringUtils('SELECT * FROM [FOO] -- COMMENT ; TEST')
    ).to.deep.equal(['SELECT * FROM [FOO] -- COMMENT ; TEST']);
  });

  it('should parse statements with multi-line comments', () => {
    expect(
      SqlStringUtils('SELECT * FROM /** COMMENT ; TEST */ FOO')
    ).to.deep.equal(['SELECT * FROM /** COMMENT ; TEST */ FOO']);
  });

  it('should parse two statements with comments', () => {
    expect(
      SqlStringUtils(
        'SELECT * FROM [FOO] -- COMMENT ; TEST\r\n WHERE A = 1; SELECT * FROM BAR'
      )
    ).to.deep.equal([
      'SELECT * FROM [FOO] -- COMMENT ; TEST\r\n WHERE A = 1',
      ' SELECT * FROM BAR'
    ]);
  });
});
