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
import { Component } from 'react';
import PropTypes from 'prop-types';
import { FieldWithError, TextField } from 'components/Fields';
import { get } from 'lodash';
import { FormValidationMessage } from 'dremio-ui-lib';

import { label } from 'uiTheme/radium/forms';
import FieldList, { RemoveButton, AddButton } from 'components/Fields/FieldList';

PropertyItem.propTypes = {
  style: PropTypes.object,
  item: PropTypes.object,
  onRemove: PropTypes.func,
  fieldKey: PropTypes.string,
  disabled: PropTypes.bool,
  classess: PropTypes.object
};

function PropertyItem({
  style,
  item,
  onRemove,
  fieldKey,
  disabled,
  classess
}) {
  const field = fieldKey ? get(item, fieldKey) : item;
  const inputContainer = classess && classess.inputContainer;
  const inputBox = classess && classess.inputBox;
  const removeIcon = classess && classess.removeIcon;
  return (
    <div className='property-item' style={{display: 'flex', marginBottom: 10}}>
      <FieldWithError
        errorPlacement='top'
        {...field}
        className={inputContainer}
      >
        <TextField {...field} disabled={disabled} className={inputBox}/>
      </FieldWithError>
      {!disabled && onRemove && <RemoveButton onClick={onRemove} className={removeIcon} /> }
    </div>
  );
}

export default class TextFieldList extends Component {
  static propTypes = {
    label: PropTypes.string,
    arrayField: PropTypes.array,
    newItemDefaultValue: PropTypes.any,
    minItems: PropTypes.number,
    addButtonText: PropTypes.string,
    fieldKey: PropTypes.string,
    initWithInitialValue: PropTypes.bool,
    errorMessage: PropTypes.string,
    disabled : PropTypes.bool,
    classess: PropTypes.object
  };

  static defaultProps = {
    newItemDefaultValue: '',
    minItems: 0,
    fieldKey: '',
    initWithInitialValue: false
  };

  componentDidMount() {
    const { initWithInitialValue } = this.props;
    if (initWithInitialValue ) {
      this.addItem();
    }
  }

  addItem = () => {
    const { arrayField, newItemDefaultValue } = this.props;
    arrayField.addField(newItemDefaultValue);
  };

  render() {
    const {
      fieldKey,
      arrayField,
      minItems,
      addButtonText,
      errorMessage,
      disabled,
      classess
    } = this.props;
    return (
      <div>
        { this.props.label && <div style={label}>{this.props.label}</div> }
        <FieldList
          items={arrayField}
          minItems={minItems}>
          <PropertyItem fieldKey={fieldKey} disabled={disabled} classess={classess}/>
        </FieldList>
        { errorMessage && (
          <FormValidationMessage className='margin-top--half margin-bottom' dataQa='error'>
            {errorMessage}
          </FormValidationMessage>
        )}
        { !disabled && <AddButton addItem={this.addItem} style={styles.addButton}>{addButtonText}</AddButton>}
      </div>
    );
  }
}

const styles = {
  addButton: {
    padding: 0
  }
};
