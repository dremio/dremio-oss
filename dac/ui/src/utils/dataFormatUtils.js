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
import { MAP, LIST, BOOLEAN, TEXT, FLOAT } from 'constants/DataTypes';
import {padStart} from 'lodash/string';

// todo: loc
import './dataFormatUtils.less';

export const UNMATCHED_CELL_VALUE = '???';
export const EMPTY_NULL_VALUE = 'null';
export const EMPTY_STRING_VALUE = 'empty text';


// https://en.wikipedia.org/wiki/Whitespace_character
const INVISIBLES = {
  CHARACTER_TABULATION: '\t',
  LINE_FEED: '\n',
  LINE_TABULATION: '\u000B',
  FORM_FEED: '\f',
  CARRIAGE_RETURN: '\r',
  SPACE: '\u0020', // include so that we force showing initial space
  NEXT_LINE: '\u0085',
  NO_BREAK_SPACE: '\u00A0',
  OGHAM_SPACE_MARK: '\u1680',
  EN_QUAD: '\u2000',
  EM_QUAD: '\u2001',
  EN_SPACE: '\u2002',
  EM_SPACE: '\u2003',
  THREE_PER_EM_SPACE: '\u2004',
  FOUR_PER_EM_SPACE: '\u2005',
  SIX_PER_EM_SPACE: '\u2006',
  FIGURE_SPACE: '\u2007',
  PUNCTUATION_SPACE: '\u2008',
  THIN_SPACE: '\u2009',
  HAIR_SPACE: '\u200A',
  LINE_SEPARATOR: '\u2028',
  PARAGRAPH_SEPARATOR: '\u2029',
  NARROW_NO_BREAK_SPACE: '\u202F',
  MEDIUM_MATHEMATICAL_SPACE: '\u205F',
  IDEOGRAPHIC_SPACE: '\u3000',

  MONGOLIAN_VOWEL_SEPARATOR: '\u180E',
  ZERO_WIDTH_SPACE: '\u200B',
  ZERO_WIDTH_NON_JOINER: '\u200C',
  ZERO_WIDTH_JOINER: '\u200D',
  WORD_JOINER: '\u2060',

  //ZERO_WIDTH_NON_BREAKING_SPACE: "\uFEFF" // (deprecated)

  // More stuff:
  NULL: '\u0000', // side note: "\0" is not valid JSON
  INFORMATION_SEPARATOR_FOUR: '\u001C' // DX-10179
};

const INVISIBLE_NAMES = {};
Object.keys(INVISIBLES).map(k => INVISIBLE_NAMES[INVISIBLES[k]] = k);
export const INVISIBLE_CHARS = new Set(Object.keys(INVISIBLES).map(k => INVISIBLES[k]));

(function() {
  const style = document.createElement('style');

  for (const char of INVISIBLE_CHARS) {
    const name = INVISIBLE_NAMES[char];
    if (name === 'SPACE') continue;
    let content = JSON.stringify(char).slice(1, -1);
    if (content.length === 1) content = `\\u${padStart(char.charCodeAt(0).toString(16), 4, '0')}`; // aggressive escaping
    content = JSON.stringify(content);
    const css = `
      .special-char[data-char=${name}] {
        width: ${(content.length - 3) === 2 ? 1.5 : 4}em;
      }
      .special-char[data-char=${name}]::before {
        content: ${content};
      }
    `;
    style.appendChild(document.createTextNode(css));
  }

  document.head.appendChild(style);
}());

class DataFormatUtils {
  _formatValue(value, columnType, row) {
    if (value === undefined) { // note: some APIs don't express null correctly (instead they drop the field)
      return EMPTY_NULL_VALUE;
    }
    if (value === '') {
      return EMPTY_STRING_VALUE;
    }
    if (value === null) {
      if (row && row.get('isDeleted')) {
        return UNMATCHED_CELL_VALUE;
      }
      return EMPTY_NULL_VALUE;
    }
    switch (columnType) {
    case MAP:
    case LIST:
    case BOOLEAN:
      if (typeof value === 'string') {
        return value;
      }
      return JSON.stringify(value);
    case FLOAT:
      return String(value); // Edge renders float inconsistently, so explicitly convert to String here
    case TEXT:
    default: // eslint-disable-line no-fallthrough
      return value;
    }
  }

  formatValue(value, columnType, row, withInvisibles = false) {
    const str = this._formatValue(...arguments);
    if (!withInvisibles || columnType !== TEXT) return str;

    const out = [];
    for (const char of str) {
      const charName = INVISIBLE_NAMES[char];
      if (!INVISIBLE_CHARS.has(char) || charName === 'OGHAM_SPACE_MARK') { // OGHAM_SPACE_MARK already shows something in Menlo
        if (out.length && typeof out[out.length - 1] === 'string') {
          out[out.length - 1] += char;
        } else {
          out.push(char);
        }
      } else {
        // we keep the placeholder text out of the real DOM so that selection/copy/paste works
        out.push(
          <span className='special-char' title={charName.replace(/_/g, ' ')} data-char={charName}>
            <span className='special-char-inner' data-char={charName}>{char}</span>
          </span>,
        );
      }
    }
    return <span children={out} />;
  }
}

export default new DataFormatUtils();
