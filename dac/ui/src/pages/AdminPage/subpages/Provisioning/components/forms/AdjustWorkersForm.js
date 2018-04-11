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
import Immutable from 'immutable';

import { TextField } from 'components/Fields';
import { InnerComplexForm, connectComplexForm } from 'components/Forms/connectComplexForm.js';
import { getViewState } from 'selectors/resources';
import { changeWorkersSize } from 'actions/resources/provisioning';
import * as ButtonTypes from 'components/Buttons/ButtonTypes';
import Button from 'components/Buttons/Button';
import ViewStateWrapper from 'components/ViewStateWrapper';
import ResourceSummary from './../ResourceSummary';

const VIEW_ID = 'AdjustWorkersForm';

export class AdjustWorkersForm extends Component {
  static propTypes = {
    fields: PropTypes.object,
    viewState: PropTypes.instanceOf(Immutable.Map),
    parent: PropTypes.object,
    onCancel: PropTypes.func,
    handleSubmit: PropTypes.func,
    changeWorkersSize: PropTypes.func,
    entity: PropTypes.instanceOf(Immutable.Map)
  }

  submit = (values) => {
    this.props.changeWorkersSize(values, this.props.entity.get('id'), VIEW_ID).then(() => {
      this.props.onCancel();
    });
  }

  render() {
    const { fields, viewState, entity } = this.props;
    return (
      <div>
        <ViewStateWrapper viewState={viewState} />
        <InnerComplexForm
          {...this.props}
          style={styles.form}
          onSubmit={this.submit}>
          <div style={{...styles.formRow, marginBottom: 10}}>
            <TextField {...fields.containerCount} type='number' style={{ width: 80 }} step={1} min={0} />
            <ResourceSummary entity={entity} />
          </div>
          <div style={styles.footer}>
            <Button
              type={ButtonTypes.CANCEL}
              text={la('Cancel')}
              disableSubmit
              onClick={this.props.onCancel}
              />
            <Button
              style={{marginLeft: 5}}
              type={ButtonTypes.NEXT}
              text={la('Adjust')}
              />
          </div>
        </InnerComplexForm>
      </div>
    );
  }
}

const styles = {
  formRow: {
    display: 'flex',
    width: '100%',
    paddingLeft: 10
  },
  form: {
    paddingTop: 0
  },
  footer: {
    display: 'flex',
    background: '#f3f3f3',
    paddingTop: 10,
    paddingBottom: 5,
    paddingRight: 10,
    justifyContent: 'flex-end',
    width: '100%'
  }
};

function mapToFormState(state, ownProps) {
  const initialValues = {
    containerCount: ownProps.entity.getIn(['dynamicConfig', 'containerCount']) || 0
  };
  return {
    viewState: getViewState(state, VIEW_ID),
    initialValues
  };
}

export default connectComplexForm({
  form: 'automaticAcceleration',
  fields: ['containerCount']
}, [], mapToFormState, {
  changeWorkersSize
})(AdjustWorkersForm);
