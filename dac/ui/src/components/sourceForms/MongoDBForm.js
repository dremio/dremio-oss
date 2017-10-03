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
import HostList from 'components/Forms/HostList';
import Credentials from 'components/Forms/Credentials';
import MongoDbOptions from 'components/Forms/MongoDbOptions';
import MetadataRefresh from 'components/Forms/MetadataRefresh';

import { ModalForm, FormBody, modalFormProps } from 'components/Forms';
import { connectComplexForm } from 'components/Forms/connectComplexForm';
import { getCreatedSource } from 'selectors/resources';

import { MONGODB } from 'dyn-load/constants/sourceTypes';

const SECTIONS = [General, HostList, Credentials, MongoDbOptions, MetadataRefresh];
const DEFAULT_PORT = 27017;

export class MongoDbForm extends Component {

  static sourceType = MONGODB;

  static propTypes = {
    onFormSubmit: PropTypes.func.isRequired,
    onCancel: PropTypes.func.isRequired,
    editing: PropTypes.bool,
    fields: PropTypes.object,
    handleSubmit: PropTypes.func.isRequired,
    hostList: PropTypes.array,
    propertyList: PropTypes.array,
    formBodyStyle: PropTypes.object
  };

  render() {
    const {fields, editing, handleSubmit, onFormSubmit, formBodyStyle} = this.props;
    const description = <span>
      If MongoDB is sharded, enter the <code>mongos</code> hosts.
      Otherwise, enter the <code>mongod</code> host.
    </span>;

    return (
      <ModalForm {...modalFormProps(this.props)} onSubmit={handleSubmit(onFormSubmit)}>
        <FormBody style={formBodyStyle}>
          <General fields={fields} editing={editing}>
            <HostList fields={fields} description={description} defaultPort={DEFAULT_PORT}/>
            <Credentials fields={fields}/>
            <MongoDbOptions fields={fields}/>
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
    config:{
      useSsl: false,
      authenticationTimeoutMillis: 2000,
      subpartitionSize: 0,
      authenticationType: 'MASTER',
      authDatabase: 'admin',
      hostList: [{port: DEFAULT_PORT}],
      secondaryReadsOnly: false,
      ...props.initialValues.config
    }
  };
  if (createdSource && createdSource.size > 1 && props.editing) {
    const hostList = createdSource.getIn(['config', 'hostList'])
      && createdSource.getIn(['config', 'hostList']).toJS();
    const propertyList = createdSource.getIn(['config', 'propertyList'])
      && createdSource.getIn(['config', 'propertyList']).toJS() || [];
    initialValues.config.hostList = hostList;
    initialValues.config.propertyList = propertyList;
  }
  return {
    initialValues
  };
}

export default connectComplexForm({
  form: 'source'
}, SECTIONS, mapStateToProps, null)(MongoDbForm);
