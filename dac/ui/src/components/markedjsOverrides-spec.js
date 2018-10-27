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
import '@app/components/markedjsOverrides.js';

const marked = require('marked');

const getCleanMarkup = markeup => marked(markeup).replace(/\r\n|\r|\n/g, ''); // remove new lines charactes

describe('markedjsOverrides', () => {
  it('link is rendered with blank target', () => {
    const inputString = '[dremio](www.dremio.com)';
    const expectedMarkup = '<p><a target=\'_blank\' href="www.dremio.com">dremio</a></p>';

    expect(getCleanMarkup(inputString)).to.be.equal(expectedMarkup);

  });

  const testNewLine = (numberOfLines) => {
    it(`${numberOfLines} new lines are preserved in markup after simple text`, () => {
      const expectedMarkup = `<p>a</p>${'<br>'.repeat(numberOfLines)}<p>b</p>`;
      expect(getCleanMarkup('a' + '\n'.repeat(numberOfLines + 1) + 'b')).to.be.equal(expectedMarkup);
    });
  };

  testNewLine(1);
  testNewLine(5);

  it('New lines are preserved after heading', () => {
    const linesNumber = 3;
    const headingText = 'heading';
    const inputString = `# ${headingText}
    ${'\n'.repeat(linesNumber)}`;
    const expectedMarkup = `<h1 id="${headingText}">heading</h1>${'<br>'.repeat(linesNumber)}`;

    expect(getCleanMarkup(inputString)).to.be.equal(expectedMarkup);
  });
});
