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
import pureRender from 'pure-render-decorator';
import PropTypes from 'prop-types';
import Radium from 'radium';
import Immutable from 'immutable';
import { connectComplexForm, InnerComplexForm } from 'components/Forms/connectComplexForm';
import DefaultWizardFooter from 'components/Wizards/components/DefaultWizardFooter';

import AggregateForm from 'components/Aggregate/AggregateForm';

const SECTIONS = [AggregateForm];

@pureRender
@Radium
export class GroupBy extends Component {
  static propTypes = {
    dataset: PropTypes.instanceOf(Immutable.Map),
    submit: PropTypes.func,
    onCancel: PropTypes.func,
    changeFormType: PropTypes.func,
    fields: PropTypes.object,
    columns: PropTypes.instanceOf(Immutable.List),
    location: PropTypes.object,
    error: PropTypes.object
  };

  render() {
    return (
      <div
        className='group-by'
        style={[styles.base]}
      >
        <InnerComplexForm
          {...this.props}
          onSubmit={this.props.submit}
        >
          <AggregateForm
            {...this.props}
            type='groupBy'
            footerStyle={styles.footer}
            contentStyle={styles.content}
            headerStyle={styles.header}
          />
          <DefaultWizardFooter
            style={{marginTop: 10}}
            {...this.props}
            onFormSubmit={this.props.submit}
          />
        </InnerComplexForm>
      </div>
    );
  }
}

const styles = {
  base: {},
  footer: {
    margin: '5px 15px'
  },
  content: {
    margin: '0 15px',
    maxHeight: 180
  },
  header: {
    margin: '10px 15px 0'
  }
};

const mapStateToProps = (state, ownProps) => {
  const { location } = ownProps;

  if (location.state.columnName) {
    const column = {
      column: location.state.columnName,
      type: location.state.columnType
    };
    return {
      initialValues: {
        columnsDimensions: [column]
      }
    };
  }
};

export default connectComplexForm({
  form: 'groupBy'
}, SECTIONS, mapStateToProps, null)(GroupBy);
