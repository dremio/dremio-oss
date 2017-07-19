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
import codeMirrorUtils from './CodeMirrorUtils';

describe('#insertTextAtPos', () => {
  let editor;
  beforeEach(() => {
    editor = {
      doc: {
        getValue: sinon.stub()
      }
    };
  });
  it('should return "code" when editor is empty', () => {
    editor.doc.getValue.returns('');
    expect(codeMirrorUtils.insertTextAtPos(editor, 'code', { ch: 0, line: 0})).to.equal('code');
  });
  it('FRO|M "Prod-Sample space".ds1 -> FROM [drop] "Prod-Sample space".ds1', () => {
    editor.doc.getValue.returns('FROM "Prod-Sample space".ds1');
    expect(codeMirrorUtils.insertTextAtPos(editor, '[drop]', { ch: 2, line: 0}))
    .to.equal('FROM [drop] "Prod-Sample space".ds1');
  });
  it('FROM| "Prod-Sample space".ds1 -> FROM [drop] "Prod-Sample space".ds1', () => {
    editor.doc.getValue.returns('FROM "Prod-Sample space".ds1');
    expect(codeMirrorUtils.insertTextAtPos(editor, '[drop]', { ch: 3, line: 0}))
    .to.equal('FROM [drop] "Prod-Sample space".ds1');
  });
  it('FROM |"Prod-Sample space".ds1 -> FROM [drop] "Prod-Sample space".ds1', () => {
    editor.doc.getValue.returns('FROM "Prod-Sample space".ds1');
    expect(codeMirrorUtils.insertTextAtPos(editor, '[drop]', { ch: 4, line: 0}))
    .to.equal('FROM [drop] "Prod-Sample space".ds1');
  });
  it('FROM "|Prod-Sample space".ds1 -> FROM "Prod-Sample space".ds1 [drop]', () => {
    editor.doc.getValue.returns('FROM "Prod-Sample space".ds1');
    expect(codeMirrorUtils.insertTextAtPos(editor, '[drop]', { ch: 6, line: 0}))
    .to.equal('FROM "Prod-Sample space".ds1 [drop]');
  });
  it('FROM "Prod-Sample| space".ds1 -> FROM "Prod-Sample space".ds1 [drop]', () => {
    editor.doc.getValue.returns('FROM "Prod-Sample space".ds1');
    expect(codeMirrorUtils.insertTextAtPos(editor, '[drop]', { ch: 17, line: 0}))
    .to.equal('FROM "Prod-Sample space".ds1 [drop]');
  });
  it('FROM "Prod-Sample space".|ds1 -> FROM "Prod-Sample space".ds1 [drop]', () => {
    editor.doc.getValue.returns('FROM "Prod-Sample space".ds1');
    expect(codeMirrorUtils.insertTextAtPos(editor, '[drop]', { ch: 23, line: 0}))
    .to.equal('FROM "Prod-Sample space".ds1 [drop]');
  });
  it('FROM "Prod-Sample space".d1 | -> FROM "Prod-Sample space".ds1 [drop]', () => {
    editor.doc.getValue.returns('FROM "Prod-Sample space".ds1 ');
    expect(codeMirrorUtils.insertTextAtPos(editor, '[drop]', { ch: 29, line: 0}))
    .to.equal('FROM "Prod-Sample space".ds1 [drop]');
  });
});
