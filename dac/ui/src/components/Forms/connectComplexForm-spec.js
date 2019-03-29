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
import { mergeFormSectionFunc, sectionsContainer } from './connectComplexForm';

const testFnPropName = 'test_fn';
const generateSections = (...funcList) => funcList.map(fn => ({ [testFnPropName]: fn }));

describe('mergeFormSectionFunc', () => {
  it('returns noop function if section list is empty', () => {
    expect(mergeFormSectionFunc([], 'testFn')()).to.be.undefined;
  });

  it('passes through call parameters', () => {
    const n = 5;
    const args = [1, 2, 3];
    const functions = Array(n).fill().map(() => sinon.stub());
    mergeFormSectionFunc(generateSections(...functions), testFnPropName)(...args);
    for (let i = 0; i < n; i++) {
      expect(functions[i]).to.be.calledWith(...args);
    }
  });

  it('returns a merged result for simple values', () => {
    const fn1 = sinon.stub().returns({ a: 1 });
    const fn2 = sinon.stub().returns({ b: 2 });
    const fn3 = sinon.stub().returns({ c: 3 });
    const result = mergeFormSectionFunc(generateSections(fn1, fn2, fn3), testFnPropName)();
    expect(result).to.be.eql({
      a: 1,
      b: 2,
      c: 3
    });
  });

  it('ignores sections with not defined target function', () => {
    const fn1 = sinon.stub().returns({ a: 1 });
    const fn2 = sinon.stub().returns({ b: 2 });
    const fn3 = sinon.stub().returns({ c: 3 });
    const result = mergeFormSectionFunc([{}, ...generateSections(fn1, fn2, fn3)], testFnPropName)();
    expect(result).to.be.eql({
      a: 1,
      b: 2,
      c: 3
    });
  });

  it('returns a merged result and last value wins in case of conflict', () => {
    const fn1 = sinon.stub().returns({ a: 1 });
    const fn2 = sinon.stub().returns({ b: 2 });
    const fn3 = sinon.stub().returns({ a: 3 });
    const result = mergeFormSectionFunc(generateSections(fn1, fn2, fn3), testFnPropName)();
    expect(result).to.be.eql({
      a: 3,
      b: 2
    });
  });

  it('deeply merges complex results', () => {
    const fn1 = sinon.stub().returns({
      a: {
        f1: 1,
        f2: 1
      },
      b: 1
    });
    const fn2 = sinon.stub().returns({
      a: {
        f2: 2,
        f3: 2
      },
      c: 2
    });
    const result = mergeFormSectionFunc(generateSections(fn1, fn2), testFnPropName)();
    expect(result).to.be.eql({
      a: {
        f1: 1,
        f2: 2,
        f3: 2
      },
      b: 1,
      c: 2
    });
  });
});

describe('sectionsContainer', () => {
  it('merges \'getFields\' methods correctly', () => {
    const comp = {
      getFields: () => ['1']
    };
    const section1 = { getFields: () => ['2'] };
    const section2 = { getFields: () => ['3'] };
    expect(sectionsContainer(section1, section2)(comp).getFields()).to.be.eql(['1', '2', '3']);
  });

  const testApiKey = methodName => it(`merges '${methodName}' methods correctly`, () => {
    const comp = {
      [methodName]: () => ({ a: 1 })
    };
    const section1 = { [methodName]: () => ({ b: 2 }) };
    const section2 = { [methodName]: () => ({ c: 3 }) };
    expect(sectionsContainer(section1, section2)(comp)[methodName]()).to.be.eql({
      a: 1,
      b: 2,
      c: 3
    });
  });

  [
    'getInitialValues',
    'validate',
    'formMapStateToProps',
    'mutateSubmitValues'
  ].map(testApiKey);
});
