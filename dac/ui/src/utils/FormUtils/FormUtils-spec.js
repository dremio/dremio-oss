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
import FormConfig from 'utils/FormUtils/FormConfig';
import FormUtils from 'utils/FormUtils/FormUtils';

describe('FormUtils', () => {

  describe('getMinDuration', () => {
    it('should get undefined to unknow interval', () => {
      expect(FormUtils.getMinDuration('a')).to.be.undefined;
    });

    it('should get zero for millisecond', () => {
      expect(FormUtils.getMinDuration('millisecond')).to.equal(0);
    });

    it('should get values for second through week', () => {
      const sec = 1000;
      expect(FormUtils.getMinDuration('second')).to.equal(sec);
      expect(FormUtils.getMinDuration('minute')).to.equal(60 * sec);
      expect(FormUtils.getMinDuration('hour')).to.equal(3600 * sec);
      expect(FormUtils.getMinDuration('day')).to.equal(24 * 3600 * sec);
      expect(FormUtils.getMinDuration('week')).to.equal(7 * 24 * 3600 * sec);
    });
  });

  describe('addTrailingBrackets', () => {
    it('should leave no name or name with brackets', () => {
      expect(FormUtils.addTrailingBrackets('')).to.equal('');
      expect(FormUtils.addTrailingBrackets('a[]')).to.equal('a[]');
    });
    it('should add missing trailing brackets', () => {
      expect(FormUtils.addTrailingBrackets('a')).to.equal('a[]');
      expect(FormUtils.addTrailingBrackets('a.b.@@@###.c')).to.equal('a.b.@@@###.c[]');
    });
  });

  describe('dropTrailingBrackets', () => {
    it('should return falsy argument', () => {
      expect(FormUtils.dropTrailingBrackets('')).to.equal('');
      expect(FormUtils.dropTrailingBrackets(null)).to.equal(null);
      expect(FormUtils.dropTrailingBrackets()).to.be.undefined;
    });
    it('should remove trailing square brackets', () => {
      expect(FormUtils.dropTrailingBrackets('a[]')).to.equal('a');
      expect(FormUtils.dropTrailingBrackets('[]')).to.equal('');
      expect(FormUtils.dropTrailingBrackets('a.b[]')).to.equal('a.b');
    });
    it('should not remove other brackets', () => {
      expect(FormUtils.dropTrailingBrackets('a[]b')).to.equal('a[]b');
      expect(FormUtils.dropTrailingBrackets('a[ ]')).to.equal('a[ ]');
      expect(FormUtils.dropTrailingBrackets('a()')).to.equal('a()');
      expect(FormUtils.dropTrailingBrackets('a{}')).to.equal('a{}');
      expect(FormUtils.dropTrailingBrackets('a(]')).to.equal('a(]');
      expect(FormUtils.dropTrailingBrackets('[]a')).to.equal('[]a');
      expect(FormUtils.dropTrailingBrackets('[a]')).to.equal('[a]');
    });
  });

  describe('getFieldByComplexPropName', () => {
    it('should not find field', () => {
      expect(FormUtils.getFieldByComplexPropName([], 'a')).to.be.undefined;
      expect(FormUtils.getFieldByComplexPropName({}, 'a')).to.be.undefined;
      expect(FormUtils.getFieldByComplexPropName({b: 'b'}, 'a')).to.be.undefined;
      expect(FormUtils.getFieldByComplexPropName({a: {b: 'c'}}, 'a.c')).to.be.undefined;
    });

    it('should find field by simple name', () => {
      expect(FormUtils.getFieldByComplexPropName({a: 'a'}, 'a')).to.equal('a');
      expect(FormUtils.getFieldByComplexPropName({a: {b: 'b'}}, 'a')).to.eql({b: 'b'});
    });

    it('should find field by compound name', () => {
      expect(FormUtils.getFieldByComplexPropName({a: {b: 'b'}}, 'a.b')).to.equal('b');
      expect(FormUtils.getFieldByComplexPropName({a: {b: {c: 'c'}}}, 'a.b')).to.eql({c: 'c'});
    });

    it('should find field by name with brackets', () => {
      expect(FormUtils.getFieldByComplexPropName({a: {b: {c: 'c'}}}, 'a.b[]')).to.eql({c: 'c'});
    });
  });

  describe('addValueByComplexPropName', () => {
    it('should ignore invalid input', () => {
      expect(FormUtils.addValueByComplexPropName(null)).to.be.null;
      const obj = {};
      expect(FormUtils.addValueByComplexPropName(obj, '', 2)).to.eql(obj);
      expect(FormUtils.addValueByComplexPropName(obj, false, 2)).to.eql(obj);
    });
    it('should add simple property', () => {
      const obj = FormUtils.addValueByComplexPropName({}, 'test', 2);
      expect(obj.test).to.equal(2);
    });
    it('should add complex property', () => {
      const obj = FormUtils.addValueByComplexPropName({}, 'config.test', 2);
      expect(obj.config.test).to.equal(2);
    });
    it('should update simple property', () => {
      let obj = {test: 1};
      obj = FormUtils.addValueByComplexPropName(obj, 'test', 2);
      expect(obj.test).to.equal(2);
    });
    it('should update complex property', () => {
      let obj = {comfig: {test: 1}};
      obj = FormUtils.addValueByComplexPropName(obj, 'config.test', 2);
      expect(obj.config.test).to.equal(2);
    });
    it('should append complex property', () => {
      let obj = {config: {test: 1}};
      obj = FormUtils.addValueByComplexPropName(obj, 'config.test2', 2);
      expect(obj.config.test).to.equal(1);
      expect(obj.config.test2).to.equal(2);
    });
  });

  describe('tabFieldsIncludeErrorFields', () => {
    it('should not find no entry in empty arrays', () => {
      expect(FormUtils.tabFieldsIncludeErrorFields([], [])).to.be.false;
    });

    it('should not find no entry in array', () => {
      expect(FormUtils.tabFieldsIncludeErrorFields([], ['a'])).to.be.false;
    });

    it('should not find entry in empty array', () => {
      expect(FormUtils.tabFieldsIncludeErrorFields(['a', 'b'], [])).to.be.false;
    });

    it('should not find entry in non matching array', () => {
      expect(FormUtils.tabFieldsIncludeErrorFields(['a', 'b'], ['c', 'd'])).to.be.false;
    });

    it('should find entry in matching array', () => {
      expect(FormUtils.tabFieldsIncludeErrorFields(['a', 'b'], ['a', 'd'])).to.be.true;
    });

    it('should find entry in array with dot delimetered name', () => {
      expect(FormUtils.tabFieldsIncludeErrorFields(['a.e', 'b'], ['a.e', 'd'])).to.be.true;
    });
  });

  describe('findFieldsWithError', () => {
    it('should return empty array if no err', () => {
      expect(FormUtils.findFieldsWithError()).to.eql([]);
    });
    it('should return array of simple paths', () => {
      expect(FormUtils.findFieldsWithError({a: '1'})).to.eql(['a']);
      expect(FormUtils.findFieldsWithError({a: '1', b: '2'})).to.eql(['a', 'b']);
      expect(FormUtils.findFieldsWithError({a: '1', bbb: '2'})).to.eql(['a', 'bbb']);
    });
    it('shoud return array of paths in nested props', () => {
      expect(FormUtils.findFieldsWithError({a: {'1': 'a1', '2': 'a2'}, b: '2'})).to.eql(['a.1', 'a.2', 'b']);
      expect(FormUtils.findFieldsWithError({a: {'1': 'a1', '2': {'3': 'a23', '4': 'a24'}, b: '2'}})).to.eql(['a.1', 'a.2.3', 'a.2.4', 'a.b']);
    });
    it('should apply prefix', () => {
      expect(FormUtils.findFieldsWithError({a: '1', b: '2'}, 'p')).to.eql(['p.a', 'p.b']);
      expect(FormUtils.findFieldsWithError({a: {'1': 'a1', '2': 'a2'}, b: '2'}, 'p')).to.eql(['p.a.1', 'p.a.2', 'p.b']);
    });
  });

  describe('findTabWithError', () => {
    let formconfig = {};
    beforeEach(() => {
      formconfig = new FormConfig({tabs: [
        {name: 'tabA', sections: []},
        {name: 'tabB', sections: []}
      ]});
    });

    it('should return undefined if selected name is not found', () => {
      expect(FormUtils.findTabWithError(formconfig, [], 'tabQ')).to.be.undefined;
    });

    it('should return first tab if selected name is not specified', () => {
      expect(FormUtils.findTabWithError(formconfig, [], '').getName()).to.equal('tabA');
    });

    it('should return selected tab if no errors', () => {
      expect(FormUtils.findTabWithError(formconfig, [], 'tabB').getName()).to.equal('tabB');
    });

    it('should return selected tab if it has errors', () => {
      const getTabFieldsFromConfigStub = sinon.stub(FormUtils, 'getTabFieldsFromConfig');
      getTabFieldsFromConfigStub.returns([]);
      const tabHasErrorStub = sinon.stub(FormUtils, 'tabFieldsIncludeErrorFields');
      tabHasErrorStub.returns(true);

      expect(FormUtils.findTabWithError(formconfig, ['a', 'b'], 'tabB').getName()).to.equal('tabB');

      getTabFieldsFromConfigStub.restore();
      tabHasErrorStub.restore();
    });

    it('should return first tab with errors if selected has no errors', () => {
      formconfig = new FormConfig({tabs: [
          {name: 'tabA', sections: [], elements: [{propName: 'a'}]},
          {name: 'tabB', sections: []}
      ]});
      expect(FormUtils.findTabWithError(formconfig, ['a', 'b'], 'tabB').getName()).to.equal('tabA');
    });

    it('should return selected tab, if error fields provided are not found in tabs', () => {
      formconfig = new FormConfig({tabs: [
          {name: 'tabA', sections: [], elements: [{propName: 'c'}]},
          {name: 'tabB', sections: []}
      ]});
      expect(FormUtils.findTabWithError(formconfig, ['a', 'b'], 'tabB').getName()).to.equal('tabB');
    });
  });

  describe('getFieldsFromConfig', () => {
    let formConfig;
    beforeEach(() => {
    });
    it('should add default fields', () => {
      formConfig = new FormConfig({tabs: [
          {name: 'tabA', sections: []},
          {name: 'tabB', sections: []}
      ]});
      const fields = FormUtils.getFieldsFromConfig({form: formConfig}, []);
      expect(fields.includes('id')).to.equal(true);
      expect(fields.includes('version')).to.equal(true);
    });

    it('should call form getFields', () => {
      formConfig = new FormConfig({tabs: [
          {name: 'tabA', sections: []},
          {name: 'tabB', sections: []}
      ]});
      const spy = sinon.spy(formConfig, 'getFields');
      FormUtils.getFieldsFromConfig({form: formConfig});
      expect(spy.calledOnce).to.equal(true);
      spy.restore();
    });
  });

  describe('mergeInitValuesWithConfig', () => {
    const configStub = sinon.stub(FormUtils, 'getInitialValuesFromConfig');
    let initValues;
    beforeEach(() => {
      configStub.returns({c: 'c', d: 'd'});
      initValues = {a: 'a', b: 'b'};
    });
    afterEach(() => {
      configStub.resetBehavior();
    });
    it('should return config values if no init', () => {
      const result = FormUtils.mergeInitValuesWithConfig(null, {}, {});
      expect(result.a).to.be.undefined;
      expect(result.c).to.equal('c');
    });
    it('should return init values if no config', () => {
      configStub.returns(null);
      const result = FormUtils.mergeInitValuesWithConfig(initValues, {}, {});
      expect(result.a).to.equal('a');
      expect(result.c).to.be.undefined;
    });
    it('should merge no overlapping attributes', () => {
      const result = FormUtils.mergeInitValuesWithConfig(initValues, {}, {});
      expect(result.a).to.equal('a');
      expect(result.c).to.equal('c');
    });
    it('should use initValues for overlapping attributes ', () => {
      initValues.c = 'CC';
      const result = FormUtils.mergeInitValuesWithConfig(initValues, {}, {});
      expect(result.a).to.equal('a');
      expect(result.c).to.equal('CC');
    });
  });

  describe('scaleValue', () => {
    it('should return passed value if it is not a number or no valid scale', () => {
      expect(FormUtils.scaleValue('a', '100')).to.equal('a');
      expect(FormUtils.scaleValue('123', 'a')).to.equal('123');
      expect(FormUtils.scaleValue('123')).to.equal('123');
    });
    it('should multiply by simple scale', () => {
      expect(FormUtils.scaleValue('12', '10')).to.equal(120);
      expect(FormUtils.scaleValue(1230, 0.1)).to.equal(123);
      expect(FormUtils.scaleValue('600000', 0.001)).to.equal(600);
      expect(FormUtils.scaleValue('12', 10)).to.equal(120);
      expect(FormUtils.scaleValue(12, '10')).to.equal(120);
    });
    it('should apply ratio scale', () => {
      expect(FormUtils.scaleValue(1230, '1:10')).to.equal(123);
      expect(FormUtils.scaleValue('1230', '1:10')).to.equal(123);
      expect(FormUtils.scaleValue(30, '2:10')).to.equal(6);
    });
    it('should apply revert scale', () => {
      expect(FormUtils.revertScaleValue('a', '100')).to.equal('a');
      expect(FormUtils.revertScaleValue('1230', '10')).to.equal(123);
      expect(FormUtils.revertScaleValue(40, '2:10')).to.equal(200);
    });
  });

  describe('addInitValue', () => {
    it('should ignore empty path', () => {
      expect(FormUtils.addInitValue({}, '', 'a')).to.eql({});
      expect(FormUtils.addInitValue({}, null, 'a')).to.eql({});
      expect(FormUtils.addInitValue({}, 0, 'a')).to.eql({});
    });

    it('should add simple path value', () => {
      expect(FormUtils.addInitValue({}, 'a', 'a')).to.eql({a: 'a'});
      expect(FormUtils.addInitValue({}, 'abc', 'a')).to.eql({abc: 'a'});
    });

    it('should add nested value', () => {
      expect(FormUtils.addInitValue({}, 'a.b', 'a')).to.eql({a: {b: 'a'}});
      expect(FormUtils.addInitValue({}, 'a.b.c', 'a')).to.eql({a: {b: {c: 'a'}}});
    });

    it('should add nested value to existing accumulator', () => {
      expect(FormUtils.addInitValue({d: 'd'}, 'a.b', 'a')).to.eql({d: 'd', a: {b: 'a'}});
      expect(FormUtils.addInitValue({a: {d: 'd'}}, 'a.b.c', 'a')).to.eql({a: {d: 'd', b: {c: 'a'}}});
    });

  });

  describe('getInitialValuesFromConfig', () => {

    it('should add simple property value', () => {
      const initValues = FormUtils.addInitValue({}, 'config', 'a');
      expect(initValues.config).to.equal('a');
    });

    it('should add nested property value', () => {
      const initValues = FormUtils.addInitValue({}, 'config.port', '2000');
      expect(initValues.config.port).to.equal('2000');
    });

    it('should add values from object', () => {
      const initValues = FormUtils.addInitValueObj({a: 1}, {b: 2, c: 3});
      expect(initValues.a).to.equal(1);
      expect(initValues.b).to.equal(2);
      expect(initValues.c).to.equal(3);
    });

    it('should add init values for editing', () => {
      // const initValues = {config: {}};
      // no point to stub createdSource, but tricky to mock state with immutable return from getIn
      // const state = {resources: {sourceList: {get: () => { return {size:2, getIn: ()=>'www'}}}}};
      // FormUtils.addInitValueForEditing(initValues, 'host', state);
      // expect(initValues.config.host).to.equal('www');
    });

    //TODO test with multiplier
  });

  describe('getValidationsFromConfig', () => {
    let formConfig;
    beforeEach(() => {
      formConfig = new FormConfig({tabs: [
          {name: 'tabA', sections: []},
          {name: 'tabB', sections: []}
      ]});
    });
    it('should call form addValidators', () => {
      const spy = sinon.spy(formConfig, 'addValidators');
      FormUtils.getValidationsFromConfig({form: formConfig});
      expect(spy.calledOnce).to.equal(true);
      spy.restore();
    });
    it('should return a function', () => {
      expect(typeof FormUtils.getValidationsFromConfig({form: formConfig})).to.equal('function');
    });
  });

  describe('addValidator', () => {
    it('should not blow up w/empty config', () => {
      const accumulator = {functions: [], validators: []};
      const elementConfig = {};
      const result = FormUtils.addValidators(accumulator, elementConfig);
      expect(result.functions.length).to.equal(0);
      expect(result.validators.length).to.equal(0);
    });
    it('should not blow up w/empty validate', () => {
      const accumulator = {functions: [], validators: []};
      const elementConfig = {validate: {}};
      const result = FormUtils.addValidators(accumulator, elementConfig);
      expect(result.validators.length).to.equal(0);
    });
    it('should add validators', () => {
      const accumulator = {functions: [], validators: []};
      const elementConfig = {propName: 'a', label: 'L', validate: {isRequired: true, isNumber: true, isWholeNumber: true}};
      const result = FormUtils.addValidators(accumulator, elementConfig);
      expect(result.validators.length).to.equal(3);
      expect(typeof result.validators[0]).to.equal('function');
      expect(typeof result.validators[1]).to.equal('function');
      expect(typeof result.validators[2]).to.equal('function');
    });
  });

});
