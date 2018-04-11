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

import { section, sectionTitle, formRow } from 'uiTheme/radium/forms';

import { FieldWithError, TextField, TextArea } from 'components/Fields';

import { applyValidators, isRequired } from 'utils/validation';

import GeneralMixin from 'dyn-load/components/Forms/GeneralMixin';
import AccelerationSection from './AccelerationSection';

@GeneralMixin
export default class General extends Component {
  static propTypes = {
    fields: PropTypes.object,
    editing: PropTypes.bool,
    sectionDescription: PropTypes.string,
    children: PropTypes.node,
    showAccelerationSection: PropTypes.bool
  };

  static defaultProps = {
    showAccelerationSection: true
  };

  static validate(values) {
    return {
      ...AccelerationSection.validate(values),
      ...applyValidators(values, [isRequired('name')])
    };
  }

  render() {
    const { fields: { name, description }, editing } = this.props;

    // TextFields have a set width, so we override them using flexes here to use all the available space
    const fieldWithErrorStyle = {display: 'flex', flex: 1, flexDirection: 'column'};
    const fieldWithErrorDivStyle = {display: 'flex', flex: 1};

    return (
      <div>
        <div className='general' style={section}>
          <h2 style={sectionTitle}>{la('General')}</h2>
          <div style={{...formRow, display: 'flex'}}>
            <FieldWithError errorPlacement='top' label={la('Name')} {...name}
              style={fieldWithErrorStyle}>
              <div style={fieldWithErrorDivStyle}>
                <TextField initialFocus {...name} disabled={editing} style={{flex: 1}}/>
              </div>
            </FieldWithError>
          </div>
          <FieldWithError label={la('Description')} {...description} style={{marginTop: '15px'}}>
            <TextArea {...description}/>
          </FieldWithError>
        </div>
        {this.props.children}
        {this.props.showAccelerationSection && <AccelerationSection fields={this.props.fields} />}
        {this.renderFooter()}
      </div>
    );
  }
}
