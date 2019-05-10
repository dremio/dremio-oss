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
import { merge, get, set } from 'lodash/object';
import { applyValidators, isRequired, isNumber, isWholeNumber, isRequiredIfAnotherPropertyEqual } from 'utils/validation';
import { getCreatedSource } from 'selectors/resources';

export default class FormUtils {

  static DURATIONS= {
    // interval durations in milliseconds
    millisecond: 0,
    second: 1000,
    minute: 60 * 1000,
    hour: 60 * 60 * 1000,
    day: 24 * 60 * 60 * 1000,
    week: 7 * 24 * 60 * 60 * 1000
  };

  static MEMORY_UNITS = {
    // memory units in bytes
    KB: 1024,
    MB: 1024 ** 2,
    GB: 1024 ** 3,
    TB: 1024 ** 4
  };

  static noop = () => {};

  static getMinDuration(intervalCode) {
    return FormUtils.DURATIONS[intervalCode];
  }

  static getMinByte(unitCode, scaleToByteUnitCode) {
    return FormUtils.MEMORY_UNITS[unitCode];
  }

  static deepCopyConfig(config) {
    return JSON.parse(JSON.stringify(config));
  }

  static addTrailingBrackets(name) {
    if (name && name.length && !name.endsWith('[]')) {
      return name + '[]';
    }
    return name;
  }

  static dropTrailingBrackets(name) {
    if (name && name.endsWith('[]')) return name.substring(0, name.length - 2);
    return name;
  }

  static getFieldByComplexPropName(fields, complexPropName = '') {
    const dropBrackets = this.dropTrailingBrackets(complexPropName);
    return get(fields, dropBrackets);
  }

  static addValueByComplexPropName(obj, complexPropName = '', value, index) {
    if (!complexPropName || !obj) return obj;
    let dropBrackets = this.dropTrailingBrackets(complexPropName);
    dropBrackets = (index === undefined) ? dropBrackets : `${dropBrackets}[${index}]`;
    set(obj, dropBrackets, value);
    return obj;
  }


  /**
   * get array of full field path for each element listed in error structure e.g
   * err: {
   *   config: {
   *     connectionString: "JDBC Connection String is required.",
   *     password: "Password is required unless you choose no authentication.",
   *     username: "Username is required unless you choose no authentication."
   *   },
   *   metadataPolicy: {
   *     name: "Name is required."
   *   }
   * }
   * @param err
   * @param prefix
   * @return array of strings
   */
  static findFieldsWithError(err, prefix = '') {
    if (!err) return [];

    return Object.entries(err).reduce((accumulator, entry) => {
      const [key, value] = entry;
      const path = (prefix) ? `${prefix}.${key}` : key;
      if (typeof value === 'object') {
        accumulator = accumulator.concat(this.findFieldsWithError(value, path));
      } else if (typeof value === 'string') {
        accumulator.push(path);
      }
      return accumulator;
    }, []);
  }

  static tabFieldsIncludeErrorFields(tabFields, errorFields) {
    if (!tabFields || !tabFields.length || !errorFields || !errorFields.length) return false;
    // true if some tabField is found in errorFields
    return tabFields.some(tabField => errorFields.includes(tabField));
  }

  static tabHasError(tabConfig, errorFields) {
    const tabFields = tabConfig.getFields();
    return this.tabFieldsIncludeErrorFields(tabFields, errorFields);
  }

  static findTabWithError(formConfig, fieldsWithError, selectedTabName) {
    const selectedTabConfig = (selectedTabName) ?
      formConfig.findTabByName(selectedTabName) : formConfig.getTabs()[0];

    // if no errors return selectedTab
    if (!fieldsWithError || !fieldsWithError.length) {
      return selectedTabConfig;
    }

    // if selected tab has error, return selectedTab
    if (this.tabFieldsIncludeErrorFields(selectedTabConfig.getFields(), fieldsWithError)) {
      return selectedTabConfig;
    }

    // else iterate tabs and return first with error; if none found, return selected tab
    const foundTab = formConfig.getTabs().find(tab => {
      const tabFields = tab.getFields();
      return tab.getName() !== selectedTabName &&
        this.tabFieldsIncludeErrorFields(tabFields, fieldsWithError);
    });
    return foundTab || selectedTabConfig;
  }


  /**
   * collect all the form field names in an array
   * @param sourceFormConfig
   * @returns {Array}
   */
  static getFieldsFromConfig(sourceFormConfig) {
    const defaultFields = ['id', 'version', 'tag'];
    return defaultFields.concat(sourceFormConfig.form.getFields());
  }


  /**
   * collect all form field initial values in a map/object
   * @param state - used in edit mode to populate existing source
   * @param props - used to detect edit mode
   * @returns {*} - resulting accumulator object
   */
  static getInitialValuesFromConfig(state, props) {
    const {sourceFormConfig} = props;
    return sourceFormConfig.form.addInitValues({}, state, props);
  }

  static mergeInitValuesWithConfig(initValues, state, props) {
    return merge(this.getInitialValuesFromConfig(state, props), initValues);
  }

  /**
   * scale or revertScale; console.error if scale is not valid
   * @param value
   * @param scale - scale down may be given as "1:1000" or just "0.001"; scale up as "10:2" or "5"
   * @param operation "*" - multiply to scale; "/" - divide to revert scale
   */
  static processScale(value, scale, operation = '*') {
    if (isNaN(value)) return value;
    if (!FormUtils.isScaleValid(scale)) {
      if (scale !== undefined) {
        const opName = (operation === '*') ? 'scaleValue' : 'revertScaleValue';
        console.error(`Invalid scale passed to FormUtils.${opName} : ${scale}`);
      }
      return value;
    }

    if (isNaN(scale)) {
      const scaleFactors = scale.split(':');
      return (operation === '*') ?
        value * scaleFactors[0] / scaleFactors[1] :
        value * scaleFactors[1] / scaleFactors[0];
    }
    return (operation === '*') ? value * scale : value / scale;
  }

  static isScaleValid(scale) {
    return scale && (!isNaN(scale) || scale.includes(':'));
  }

  static scaleValue(value, scale) {
    return FormUtils.processScale(value, scale);
  }

  static revertScaleValue(value, scale) {
    return FormUtils.processScale(value, scale, '/');
  }

  /**
   * add initial value to a proper level of initValues based on dot-delimetered path
   * e.g. path = 'config.port', value = 10 will make initValues[config][port] = 10
   * @param initValues
   * @param path
   * @param value
   * @returns {*} - mutated initValues or new object if initValues was not defined.
   */
  static addInitValue(initValues, path, value, multiplier) {
    if (!path) return initValues;

    const adjustedValue = (multiplier && value instanceof Number) ? value * multiplier : value;

    set(initValues, path, adjustedValue);
    return initValues;
  }

  static addInitValueObj(initValues, valueObj) {
    if (initValues && valueObj) {
      Object.keys(valueObj).forEach(key => {
        initValues[key] = valueObj[key];
      });
    }
    return initValues;
  }

  static addInitValueForEditing(initValues, configPropName, state) {
    const createdSource = getCreatedSource(state);
    if (createdSource && createdSource.size > 1) {
      initValues.config = initValues.config || {};
      initValues.config[configPropName] = createdSource.getIn(['config', configPropName])
        && createdSource.getIn(['config', configPropName]).toJS() || [];
    }
  }


  /**
   * generate validation methods for all form fields
   * @param sourceFormConfig
   * @returns {*}
   */
  static getValidationsFromConfig(sourceFormConfig) {
    let accumulator = {functions: [], validators: []};
    accumulator = sourceFormConfig.form.addValidators(accumulator);

    return function(values) {
      const combinedValidateResult = accumulator.functions.reduce((obj, fn) => {
        return merge(obj, fn(values));
      }, {});
      const appliedValidatorsResult = applyValidators(values, accumulator.validators);
      return merge(combinedValidateResult, appliedValidatorsResult);
    };
  }


  static addValidators(accumulator, elementConfigJson) {
    if (!elementConfigJson || !elementConfigJson.validate) return accumulator;

    if (elementConfigJson.validate.isRequired) {
      accumulator.validators.push(isRequired(elementConfigJson.propName, elementConfigJson.label));
    }
    if (elementConfigJson.validate.isNumber) {
      accumulator.validators.push(isNumber(elementConfigJson.propName, elementConfigJson.label));
    }
    if (elementConfigJson.validate.isWholeNumber) {
      accumulator.validators.push(isWholeNumber(elementConfigJson.propName, elementConfigJson.label));
    }
    if (elementConfigJson.validate.isRequiredIf) {
      accumulator.validators.push(isRequiredIfAnotherPropertyEqual(elementConfigJson.propName,
        elementConfigJson.validate.isRequiredIf.otherPropName,
        elementConfigJson.validate.isRequiredIf.otherPropValue,
        elementConfigJson.validate.label));
    }
    return accumulator;
  }

}
