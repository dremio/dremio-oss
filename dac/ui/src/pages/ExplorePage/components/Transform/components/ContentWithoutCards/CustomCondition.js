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
import Radium from 'radium';

import PropTypes from 'prop-types';

import AddFieldEditor from 'components/Wizards/DetailsWizard/components/AddFieldEditor';
import actionUtils from 'utils/actionUtils/actionUtils';

@Radium
export default class CustomCondition extends Component {
  static propTypes = {
    fields: PropTypes.object,
    submitForm: PropTypes.func
  };

  constructor(props) {
    super(props);
    this.onChange = this.onChange.bind(this);
  }

  onChange(data) {
    const { fields: { booleanExpression } } = this.props;
    actionUtils.runAutoPreview(this.props.submitForm);
    booleanExpression.onChange(data);
  }

  render() {
    const { fields: { booleanExpression } } = this.props;
    return (
      <div style={[styles.base]}>
        <AddFieldEditor
          {...booleanExpression}
          onChange={this.onChange}
          style={styles.addFieldEditor}
          tooltip={la('Write an expression that evaluates to TRUE or FALSE.')}
          //todo DX-12762
          blockHeight={140}
          sqlHeight={142}
          functionPaneHeight={142}
          activeMode
          dragType='help-func'/>
      </div>
    );
  }
}

const styles = {
  addFieldEditor: {
    height: 160
  },
  base: {
    width: '100%',
    position: 'relative',
    marginTop: -10
  },
  wrap: {
    margin: '0 10px 0 10px',
    height: 24
  },
  wrapFooter: {
    marginTop: 40,
    marginLeft: 10,
    position: 'relative'
  },
  input: {
    width: 230,
    height: 24,
    fontSize: 13,
    border: '1px solid #ccc',
    borderRadius: 3,
    outline: 'none',
    float: 'left',
    padding: 2
  },
  check: {
    float: 'left',
    marginLeft: 10
  },
  nameCheck: {
    marginLeft: 5
  }
};
