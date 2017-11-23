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
import { Component } from 'react';

import PropTypes from 'prop-types';

import General from 'components/Forms/General';
import MetadataRefresh from 'components/Forms/MetadataRefresh';
import SourceProperties from 'components/Forms/SourceProperties';
import { FieldWithError, TextField, Checkbox } from 'components/Fields';
import HoverHelp from 'components/HoverHelp';
import { ModalForm, FormBody, modalFormProps } from 'components/Forms';
import { connectComplexForm } from 'components/Forms/connectComplexForm';
import { section, formRow, sectionTitle } from 'uiTheme/radium/forms';
import { getCreatedSource } from 'selectors/resources';
import { applyValidators, isRequired } from 'utils/validation';
import { HBASE } from 'dyn-load/constants/sourceTypes';

const SECTIONS = [General, SourceProperties, MetadataRefresh];
const DEFAULT_PORT = 2181;

function validate(values) {
  return applyValidators(values, [
    isRequired('config.zkQuorum', 'Zookeeper Quorum'),
    isRequired('config.port', 'Zookeeper Port')
  ]);
}

export class HBaseForm extends Component {

  static sourceType = HBASE;

  static propTypes = {
    onFormSubmit: PropTypes.func.isRequired,
    onCancel: PropTypes.func.isRequired,
    handleSubmit: PropTypes.func.isRequired,
    editing: PropTypes.bool,
    fields: PropTypes.object,
    formBodyStyle: PropTypes.object
  };

  render() {
    const {fields, editing, handleSubmit, onFormSubmit, formBodyStyle} = this.props;
    return (
      <ModalForm {...modalFormProps(this.props)} onSubmit={handleSubmit(onFormSubmit)}>
        <FormBody style={formBodyStyle}>
          <General fields={fields} editing={editing}>
            <div style={section}>
              <h2 style={sectionTitle}>{la('Zookeeper Quorum')}</h2>
              <div style={{ display: 'flex' }}>
                <FieldWithError {...fields.config.zkQuorum} errorPlacement='top'>
                  <TextField {...fields.config.zkQuorum}/>
                </FieldWithError>
                <HoverHelp content={la('Comma delimited list of hosts; e.g "123.0.0.1,123.0.0.2"')}/>
              </div>
            </div>
            <div style={section}>
              <h2 style={sectionTitle}>{la('Zookeeper Port')}</h2>
              <FieldWithError {...fields.config.port} errorPlacement='top'>
                <TextField {...fields.config.port} type='number'/>
              </FieldWithError>
            </div>
            <div style={section}>
              <FieldWithError {...fields.config.isSizeCalcEnabled} style={formRow}>
                <Checkbox {...fields.config.isSizeCalcEnabled} label={la('Region Size Calculation')}/>
              </FieldWithError>
            </div>
            <SourceProperties fields={fields} />
          </General>
        </FormBody>
      </ModalForm>
    );
  }
}

function mapStateToProps(state, props) {
  const createdSource = getCreatedSource(state);
  const initialValues = {
    ...props.initialValues,
    config: {
      isSizeCalcEnabled: false,
      port: DEFAULT_PORT,
      ...props.initialValues.config
    }
  };

  if (createdSource && createdSource.size > 1 && props.editing) {
    const propertyList = createdSource.getIn(['config', 'propertyList'])
      && createdSource.getIn(['config', 'propertyList']).toJS() || [];
    initialValues.config.propertyList = propertyList;
  }
  return { initialValues };
}

export default connectComplexForm({
  form: 'source',
  fields: ['config.zkQuorum', 'config.port', 'config.isSizeCalcEnabled'],
  validate
}, SECTIONS, mapStateToProps, null)(HBaseForm);
