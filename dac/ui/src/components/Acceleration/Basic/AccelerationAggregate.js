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
import { overlay } from 'uiTheme/radium/overlay';
import Immutable from 'immutable';
import FontIcon from 'components/Icon/FontIcon';
import { Toggle } from 'components/Fields';
import AggregateForm from 'components/Aggregate/AggregateForm';
import Spinner from 'components/Spinner';
import Button from 'components/Buttons/Button';
import { commonStyles } from '../commonStyles';
import LayoutInfo from '../LayoutInfo';

@pureRender
@Radium
export default class AccelerationAggregate extends Component {
  static getFields() {
    return AggregateForm.getFields();
  }
  static validate(values) {
    return AggregateForm.validate(values);
  }
  static propTypes = {
    dataset: PropTypes.instanceOf(Immutable.Map).isRequired,
    reflection: PropTypes.instanceOf(Immutable.Map), // option (e.g. brand new)
    location: PropTypes.object,
    fields: PropTypes.object.isRequired,
    style: PropTypes.object,
    shouldHighlight: PropTypes.bool,
    errorMessage: PropTypes.node,
    loadingRecommendations: PropTypes.bool,
    skipRecommendations: PropTypes.func
  };

  static defaultProps = {
    fields: {}
  };

  mapSchemaToColumns() {
    return this.props.dataset.get('fields').map((item, index) => {
      return Immutable.fromJS({
        type: item.getIn(['type', 'name']),
        name: item.get('name'),
        index
      });
    });
  }

  renderForm() {
    const { location, fields, dataset, loadingRecommendations, skipRecommendations } = this.props;
    const columns = this.mapSchemaToColumns();

    if (loadingRecommendations) {
      return <div style={overlay} className='view-state-wrapper-overlay'>
        <div>
          <Spinner message={<span style={{display: 'flex', alignItems: 'center'}}>
            {la('Determining Automatic Aggregation Reflections…')}
            <Button style={{marginLeft: '1em'}} disableSubmit onClick={skipRecommendations} type='CUSTOM' text={la('Skip')} />
          </span>} />
        </div>
      </div>;
    } else {
      return <AggregateForm
        dataset={Immutable.fromJS({displayFullPath: dataset.get('path')})} // fake just enough of the legacy DS model
        style={styles.aggregateForm}
        fields={fields}
        columns={columns}
        location={location}
        canSelectMeasure={false}
        canUseFieldAsBothDimensionAndMeasure
      />;
    }
  }

  render() {
    const { fields, style, reflection, errorMessage } = this.props;
    const { enabled } = fields.aggregationReflections[0];

    const toggleLabel = (
      <h3 style={commonStyles.toggleLabel}>
        <FontIcon type='Aggregate' theme={commonStyles.iconTheme}/>
        {la('Aggregation Reflections')}
      </h3>
    );
    return (
      <div style={[style, styles.wrap]} data-qa='aggregation-basic'>
        <div style={{
          ...commonStyles.header,
          ...(this.props.shouldHighlight ? commonStyles.highlight : {}),
          borderWidth: 0
        }} data-qa='aggregation-queries-toggle'>
          <Toggle {...enabled} label={toggleLabel} style={commonStyles.toggle}/>
          <LayoutInfo
            layout={reflection}
            style={{float: 'right'}} />
        </div>
        <div style={{position: 'relative'}}>
          {errorMessage}
        </div>
        {this.renderForm()}
      </div>
    );
  }
}

const styles = {
  wrap: {
    display: 'flex',
    flexDirection: 'column',
    flexGrow: 1,
    overflow: 'hidden',
    position: 'relative'
  },
  aggregateForm: {
    flexFlow: 'column nowrap',
    flexGrow: 1,
    padding: '10px 10px 0'
  }
};
