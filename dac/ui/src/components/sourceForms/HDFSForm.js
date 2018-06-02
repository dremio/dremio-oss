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

import General from 'components/Forms/General';
import SingleHost from 'components/Forms/SingleHost';
import SourceProperties from 'components/Forms/SourceProperties';
import Checkbox from 'components/Fields/Checkbox';
import MetadataRefresh from 'components/Forms/MetadataRefresh';
import { FormBody, ModalForm, modalFormProps } from 'components/Forms';
import { connectComplexForm } from 'components/Forms/connectComplexForm';
import { getCreatedSource } from 'selectors/resources';
import { section, sectionTitle } from 'uiTheme/radium/forms';
import AdvancedOptionsExpandable from 'components/Forms/AdvancedOptionsExpandable';
import { FieldWithError, Select, TextField } from 'components/Fields';
import HoverHelp from 'components/HoverHelp';

import { HDFS } from 'dyn-load/constants/sourceTypes';

const SECTIONS = [General, SingleHost, SourceProperties, MetadataRefresh];

const shortCircuitFlagOptions = [
  {option: 'SYSTEM', label: 'Use HDFS Default'},
  {option: 'ENABLED', label: 'Enabled'},
  {option: 'DISABLED', label: 'Disabled'}
];

export class HDFSForm extends Component {

  static sourceType = HDFS;

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

    // disable the socket path unless shortCircuitFlag is enabled
    const disableSocketPath = fields.config.shortCircuitFlag.value !== 'ENABLED';

    return (
      <ModalForm {...modalFormProps(this.props)} onSubmit={handleSubmit(onFormSubmit)}>
        <FormBody style={formBodyStyle}>
          <General fields={fields} editing={editing}>
            <SingleHost style={{marginBottom: 5}} fields={fields} title='NameNode'/>
            <div style={section}>
              <Checkbox {...fields.config.enableImpersonation} label={la('Impersonation')}/>
            </div>
            <SourceProperties fields={fields}/>
            <div style={section}>
              <h2 style={sectionTitle}>{la('Advanced Options')}</h2>
              <AdvancedOptionsExpandable>
                <div style={styles.formSubRow}>
                  <FieldWithError errorPlacement='top' label={la('Root Path')} {...fields.config.rootPath}>
                    <TextField {...fields.config.rootPath}/>
                  </FieldWithError>

                  <div style={{paddingTop: 6, display: 'flex', alignItems: 'center'}}>
                    <h3>{la('Short Circuit Local Reads')}</h3>
                    <HoverHelp content={la('Bypass DataNode for reads where the client is co-located with the data.')} tooltipInnerStyle={styles.hoverTip} />
                  </div>

                  <FieldWithError errorPlacement='top' label={la('Mode')} {...fields.config.shortCircuitFlag}
                    style={{paddingTop: 6}}>
                    <Select
                      {...fields.config.shortCircuitFlag}
                      items={shortCircuitFlagOptions}/>
                  </FieldWithError>

                  <FieldWithError errorPlacement='top'
                    label={la('Socket Path')} {...fields.config.shortCircuitSocketPath}
                    style={{paddingTop: 6}}>
                    <TextField {...fields.config.shortCircuitSocketPath} disabled={disableSocketPath} placeholder={'/var/lib/hadoop-hdfs/dn_socket'}/>
                  </FieldWithError>
                </div>
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
      port: 8020,
      enableImpersonation: false,
      rootPath: '/',
      shortCircuitFlag: 'SYSTEM',
      ...props.initialValues.config
    }
  };
  if (createdSource && createdSource.size > 1 && props.editing) {
    const propertyList = createdSource.getIn(['config', 'propertyList'])
      && createdSource.getIn(['config', 'propertyList']).toJS() || [];
    initialValues.config.propertyList = propertyList;
  }
  return {
    initialValues
  };
}

export default connectComplexForm({
  form: 'source',
  fields: ['config.enableImpersonation', 'config.rootPath', 'config.shortCircuitFlag', 'config.shortCircuitSocketPath']
}, SECTIONS, mapStateToProps, null)(HDFSForm);

const styles = {
  formSubRow: {
    marginBottom: 6
  },

  hoverTip: {
    display: 'inline-block',
    textAlign: 'left',
    width: 300,
    whiteSpace: 'pre-line'
  }
};
