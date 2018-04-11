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
import { FieldWithError, TextField } from 'components/Fields';
import { get } from 'lodash';

import { label } from 'uiTheme/radium/forms';
import FieldList, { RemoveButton, AddButton } from 'components/Fields/FieldList';

PropertyItem.propTypes = {
  style: PropTypes.object,
  item: PropTypes.object,
  onRemove: PropTypes.func,
  fieldKey: PropTypes.string
};

function PropertyItem({style, item, onRemove, fieldKey}) {
  const field = fieldKey ? get(item, fieldKey) : item;
  return (
    <div className='property-item' style={{display: 'flex', marginBottom: 10, ...style}}>
      <FieldWithError
        errorPlacement='top'
        {...field}>
        <TextField {...field} />
      </FieldWithError>
      {onRemove && <RemoveButton onClick={onRemove} /> }
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
    fieldKey: PropTypes.string
  }

  static defaultProps = {
    newItemDefaultValue: '',
    minItems: 0,
    fieldKey: ''
  }

  addItem = (e) => {
    const { arrayField, newItemDefaultValue } = this.props;
    arrayField.addField(newItemDefaultValue);
  }

  render() {
    return (
      <div>
        <div style={label}>{this.props.label}</div>
        <FieldList
          items={this.props.arrayField}
          minItems={this.props.minItems}>
          <PropertyItem fieldKey={this.props.fieldKey} />
        </FieldList>

        <AddButton addItem={this.addItem} style={styles.addButton}>{this.props.addButtonText}</AddButton>
      </div>
    );
  }
}

const styles = {
  addButton: {
    padding: 0
  }
};
