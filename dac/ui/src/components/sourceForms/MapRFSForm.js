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
import { Component, PropTypes } from 'react';

import General from 'components/Forms/General';
import { getCreatedSource } from 'selectors/resources';
import SourceProperties from 'components/Forms/SourceProperties';
import MetadataRefresh from 'components/Forms/MetadataRefresh';

import { FieldWithError, TextField, Checkbox } from 'components/Fields';

import { ModalForm, FormBody, modalFormProps } from 'components/Forms';
import { connectComplexForm } from 'components/Forms/connectComplexForm';
import { section, sectionTitle } from 'uiTheme/radium/forms';
import { formRow } from 'uiTheme/radium/forms';
import AdvancedOptionsExpandable from 'components/Forms/AdvancedOptionsExpandable';

import { MAPRFS } from 'dyn-load/constants/sourceTypes';

const SECTIONS = [General, SourceProperties, MetadataRefresh];

export class MapRFSForm extends Component {

  static sourceType = MAPRFS;

  static propTypes = {
    onFormSubmit: PropTypes.func.isRequired,
    onCancel: PropTypes.func.isRequired,
    handleSubmit: PropTypes.func.isRequired,
    editing: PropTypes.bool,
    fields: PropTypes.object,
    formBodyStyle: PropTypes.object
  };

  render() {
    const {fields, handleSubmit, onFormSubmit, formBodyStyle, editing} = this.props;
    return (
      <ModalForm {...modalFormProps(this.props)} onSubmit={handleSubmit(onFormSubmit)}>
        <FormBody style={formBodyStyle}>
          <General fields={fields} editing={editing}>
            <div style={section}>
              <h3 style={sectionTitle}>{la('Cluster Name')}</h3>
              <FieldWithError {...fields.config.clusterName}>
                <TextField {...fields.config.clusterName}/>
              </FieldWithError>
            </div>
            <div style={section}>
              <Checkbox {...fields.config.enableImpersonation}
                label={la('Impersonation')} style={{margin: '-20px 0 20px 0'}}/>
            </div>
            <div style={section}>
              <h3 style={sectionTitle}>{la('Options')}</h3>
              <FieldWithError {...fields.config.secure} style={formRow}>
                <Checkbox {...fields.config.secure} label={la('Encrypt Connection')}/>
              </FieldWithError>
            </div>
            <SourceProperties fields={fields} />
            <div style={section}>
              <h3 style={sectionTitle}>{la('Advanced Options')}</h3>
              <AdvancedOptionsExpandable>
                <MetadataRefresh fields={fields} hideObjectNames showAuthorization/>
              </AdvancedOptionsExpandable>
            </div>
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
      secure: false,
      enableImpersonation: false,
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
  fields: ['config.clusterName', 'config.secure', 'config.enableImpersonation']
}, SECTIONS, mapStateToProps, null)(MapRFSForm);
