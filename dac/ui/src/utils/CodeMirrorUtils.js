/*
 * Copyright (C) 2017 Dremio Corporation
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
class CodeMirrorUtils {

  fromIndexTo2D(editor, index) {
    let i = 0;
    let ind = index;
    while (editor.getLine(i) !== undefined && editor.getLine(i).length < ind) {
      ind -= editor.getLine(i).length;
      i++;
    }
    if (editor.getLine(i)) {
      return {
        ch: ind,
        line: i
      };
    }
    // last symbol of the line
    if (editor.getLine(i - 1)) {
      return {
        ch: editor.getLine(i - 1).length,
        line: i - 1
      };
    }
    return null;
  }

  findWordsOffset(editor) {
    const text = editor.getValue();
    let i = 0;
    const words = [];
    let begin = -1;
    for (i; i < text.length; i++) {
      if ( /\w/.test(text[i]) && begin === -1) {
        begin = i;
      }
      if ( !/\w/.test(text[i])) {
        const word = text.substring(begin, i);
        words.push({
          word,
          begin,
          end: i - 1
        });
        begin = -1;
      }
    }
    if (begin !== -1) {
      const word = text.substring(begin, text.length);
      words.push({
        word,
        begin,
        end: text.length - 1
      });
    }
    return words;
  }

  /**
   * Check if char at index surrounded by quoute
   *
   * @param {String} text string to search in
   * @param {String} char surround quoute character e.g. ' or "
   * @param {Number} index index of char to check surround
   * @return {Boolean} true - when surround
   */
  isSurround(text, char, index) {
    return text.indexOf(char) <= index && text.indexOf(char, index) > index;
  }

  /**
   * @param {CodeMirror} editor
   * @param {string} text text to insert
   * @param {object} marker position to insert
   */
  insertTextAtPos(editor, text, marker) {
    const {line, ch} = marker;
    const linesArray = editor.doc.getValue().split('\n');
    const targetLine = linesArray[line];
    let newLine;
    if (ch >= targetLine.length) {
      if (targetLine.length === 0) {
        newLine = text;
      } else {
        newLine = targetLine[targetLine.length - 1] === ' ' ?
          targetLine + text : targetLine + ' ' + text;
      }
    } else {
      const firstPart = targetLine.slice(0, ch);
      const secondPart = targetLine.slice(ch);
      let newCode = text;
      if (this.isSurround(targetLine, '"', ch)) {
        const whiteSpaceIndex = secondPart.indexOf(' ', secondPart.indexOf('"'));
        const newIndex = whiteSpaceIndex >= 0 ?
          firstPart.length + whiteSpaceIndex :
          targetLine.length;
        return this.insertTextAtPos(editor, text, {line, ch: newIndex});
      }
      if (targetLine[ch] === ' ') {
        if (targetLine[ch + 1] !== ' ') {
          newCode = ' ' + text;
        }
        newLine = [
          firstPart,
          newCode,
          secondPart
        ].join('');
      } else {
        const whiteSpaceIndex = secondPart.search(/\s/);
        const newIndex = whiteSpaceIndex > -1 ?
         firstPart.length + whiteSpaceIndex :
         targetLine.length;
        return this.insertTextAtPos(editor, text, {line, ch: newIndex});
      }
    }
    linesArray[line] = newLine;
    return linesArray.join('\n');
  }
}

const codeMirrorUtils = new CodeMirrorUtils();

export default codeMirrorUtils;
