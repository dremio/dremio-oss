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
import S3Credentials from 'components/Forms/S3Credentials';
import SourceProperties from 'components/Forms/SourceProperties';
import MetadataRefresh from 'components/Forms/MetadataRefresh';

import { Checkbox } from 'components/Fields';
import ValueList from 'components/Fields/ValueList';

import { ModalForm, FormBody, modalFormProps } from 'components/Forms';
import { connectComplexForm } from 'components/Forms/connectComplexForm';

import { section, sectionTitle, formRow, description } from 'uiTheme/radium/forms';
import { FieldWithError } from 'components/Fields';
import { getCreatedSource } from 'selectors/resources';
import AdvancedOptionsExpandable from 'components/Forms/AdvancedOptionsExpandable';

import { S3 } from 'dyn-load/constants/sourceTypes';

const SECTIONS = [General, S3Credentials, SourceProperties, MetadataRefresh];

export class S3Form extends Component {

  static sourceType = S3;

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
            <S3Credentials fields={fields}/>
            <div className='external-buckets' style={section}>
              <h2 style={sectionTitle}>{la('External Buckets')}</h2>
              <div style={description}>
                {la('Note: All buckets associated with the AWS account will be added automatically.')}
              </div>
              <FieldWithError {...fields.config.externalBucketList}>
                <ValueList
                  fieldList={fields.config.externalBucketList}
                  emptyLabel={la('(No External Buckets Added)')} />
              </FieldWithError>
            </div>
            <div style={section}>
              <h2 style={sectionTitle}>{la('Options')}</h2>
              <div style={formRow}>
                <Checkbox {...fields.config.secure} label={la('Enable SSL Encryption')}/>
              </div>
            </div>
            <FieldWithError {...fields.config.propertyList}>
              <SourceProperties fields={fields} />
            </FieldWithError>
            <div style={section}>
              <h2 style={sectionTitle}>{la('Advanced Options')}</h2>
              <AdvancedOptionsExpandable>
                <MetadataRefresh fields={fields} />
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
      ...props.initialValues
    }
  };
  if (createdSource && createdSource.size > 1 && props.editing) {
    const externalBucketList = createdSource.getIn(['config', 'externalBucketList'])
      && createdSource.getIn(['config', 'externalBucketList']).toJS() || [];
    const propertyList = createdSource.getIn(['config', 'propertyList'])
      && createdSource.getIn(['config', 'propertyList']).toJS() || [];
    initialValues.config.externalBucketList = externalBucketList;
    initialValues.config.propertyList = propertyList;
  }
  return {
    initialValues
  };
}

export default connectComplexForm({
  form: 'source',
  fields: ['config.externalBucketList[]', 'config.secure']
}, SECTIONS, mapStateToProps, null)(S3Form);
