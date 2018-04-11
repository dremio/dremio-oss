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
import { ModalForm, FormBody, modalFormProps } from 'components/Forms';
import { connectComplexForm } from 'components/Forms/connectComplexForm';
import ResourceTreeController from 'components/Tree/ResourceTreeController';
import { TextField, FieldWithError } from 'components/Fields';

export const FIELDS = ['context'];

export class SelectContextForm extends Component {
  static propTypes = {
    handleSubmit: PropTypes.func.isRequired,
    onFormSubmit: PropTypes.func,
    onCancel: PropTypes.func,
    fields: PropTypes.object
  };

  constructor(props) {
    super(props);

    this.handleChangeSelectedNode = this.handleChangeSelectedNode.bind(this);
  }

  handleChangeSelectedNode(nodeId) {
    this.props.fields.context.onChange(nodeId);
  }

  render() {
    const { fields, handleSubmit } = this.props;
    const nodeId = (fields.context.value || fields.context.initialValue || '');

    return (
      <div className='select-context' style={{height: '100%'}}>
        <ModalForm
          {...modalFormProps(this.props)}
          onSubmit={handleSubmit(this.props.onFormSubmit)}
          confirmText={la('Select')}>
          <FormBody>
            <label>{la('Select New Context')}</label>
            <div style={style.resourceTree}>
              <ResourceTreeController
                hideDatasets
                onChange={this.handleChangeSelectedNode}
                preselectedNodeId={nodeId}/>
            </div>
            <FieldWithError errorPlacement='right' {...fields.context} style={style.resourceInput}>
              <TextField {...fields.context} initialFocus/>
            </FieldWithError>
          </FormBody>
        </ModalForm>
      </div>
    );
  }
}

const mapStateToProps = (state, props) => ({initialValues: props.initialValues});

export default connectComplexForm({
  form: 'selectContextForm',
  fields: FIELDS
}, [], mapStateToProps, null)(SelectContextForm);

const style = {
  resourceTree: {
    height: '250px'
  },
  resourceInput: {
    marginTop: '20px'
  }
};
