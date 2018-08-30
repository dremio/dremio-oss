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
import { Component } from 'react';
import PropTypes from 'prop-types';
import uuid from 'uuid';

import FieldList, { AddButton } from 'components/Fields/FieldList';
import Property, { PropertyValue } from 'components/Forms/Property';
import { description, sectionTitle } from 'uiTheme/radium/forms';
import FormUtils from 'utils/FormUtils/FormUtils';

PropertyItem.propTypes = {
  style: PropTypes.object,
  item: PropTypes.object,
  onRemove: PropTypes.func,
  elementConfig: PropTypes.object,
  singleValue: PropTypes.bool
};

// todo: chris is wondering where `style` is actually populated
// todo: chris also curious why the `PropertyItem` wrapper is needed (couldn't `Property` just own all of this)
function PropertyItem({style, item, onRemove, singleValue, elementConfig}) {
  return (
    <div className='property-item' style={{...styles.item, style}}>
      {!singleValue && <Property fields={item} onRemove={onRemove}/>}
      {singleValue && <PropertyValue field={item} elementConfig={elementConfig} onRemove={onRemove}/>}
    </div>
  );
}

function validateValueList(values, elementConfig) {
  const enteredValues = FormUtils.getFieldByComplexPropName(values, elementConfig.propName);
  const emptyMsg = `${elementConfig.label} can not be empty`;
  const repeatMsg = 'Values can not repeat';

  if (!enteredValues) {
    return {};
  }

  return enteredValues.reduce((errors, value, index) => {
    if (!value) {
      if (elementConfig.validate === undefined || elementConfig.validate.isRequired) {
        errors = FormUtils.addValueByComplexPropName(errors, elementConfig.propName, emptyMsg, index);
      }
    } else {
      const foundIndex = enteredValues.findIndex(val => val === value);
      if (foundIndex < index && foundIndex >= 0) {
        errors = FormUtils.addValueByComplexPropName(errors, elementConfig.propName, repeatMsg, index);
      }
    }
    return errors;
  }, {});
}

export default class SourceProperties extends Component {
  static getFields(elementConfig) {
    const propName = (elementConfig) ? elementConfig.propName : 'config.propertyList';
    return Property.getFields().map(field => `${propName}[].${field}`);
  }

  static propTypes = {
    fields: PropTypes.object,
    title: PropTypes.string,
    emptyLabel: PropTypes.string,
    addLabel: PropTypes.string,
    description: PropTypes.string,
    elementConfig: PropTypes.object,
    singleValue: PropTypes.bool
  };

  static defaultProps = { // todo: `la` failing to build here
    title: ('Properties'),
    emptyLabel: ('No properties added'),
    addLabel: ('Add property'),
    singleValue: false
  };

  static validate(values) {
    const propertyName = (this.props.elementConfig) ? this.props.elementConfig.propertyName : 'propertyList';
    const result = {config: {}};
    result[propertyName] = values.config[propertyName].map(property => Property.validate(property));
    return result;
  }

  static getValidators(elementConfig) {
    return function(values) {
      return validateValueList(values, elementConfig);
    };
  }

  //
  // Handlers
  //

  addItem = (e) => {
    const {fields: {config}, elementConfig, singleValue} = this.props;
    const propertyName = (elementConfig) ? elementConfig.propertyName : 'propertyList';
    const propertyListFields = config[propertyName];
    const properties = (singleValue) ? FormUtils.getFieldByComplexPropName(this.props.fields, elementConfig.propName) : propertyListFields;
    e.preventDefault();
    properties.addField({id: uuid.v4()});
  };

  render() {
    const {fields: {config}, title, emptyLabel, addLabel, elementConfig, singleValue} = this.props;
    const propertyName = (elementConfig) ? elementConfig.propertyName : 'propertyList';
    const propertyListFields = config[propertyName];
    const des = this.props.description ? <div className='largerFontSize' style={description}>{this.props.description}</div> : null;
    const properties = (singleValue) ? FormUtils.getFieldByComplexPropName(this.props.fields, elementConfig.propName) : propertyListFields;
    return (
      <div className='properties'>
        {!elementConfig && <h2 style={sectionTitle}>{title}</h2>}
        {des}
        <FieldList
          className='normalWeight'
          singleValue={singleValue}
          items={properties}
          itemHeight={50}
          getKey={item => item.id.value}
          emptyLabel={emptyLabel}
          propName={elementConfig.propName}>
          <PropertyItem singleValue={singleValue} elementConfig={elementConfig}/>
        </FieldList>

        <AddButton addItem={this.addItem} style={styles.addButton}>{addLabel}</AddButton>
      </div>
    );
  }
}

const styles = {
  item: {
    display: 'block',
    alignItems: 'center',
    paddingRight: 14,
    marginRight: -14,
    marginBottom: 3
  },
  addButton: {
    marginLeft: -3,
    marginTop: -8
  }
};
