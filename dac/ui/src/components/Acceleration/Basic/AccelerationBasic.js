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
import { connect } from 'react-redux';
import Radium from 'radium';
import PropTypes from 'prop-types';
import Immutable from 'immutable';
import FontIcon from 'components/Icon/FontIcon';
import { FLEX_COL_START } from 'uiTheme/radium/flexStyle';
import { pathLink } from 'uiTheme/radium/typography';
import { modalFormProps } from 'components/Forms';
import { Toggle } from 'components/Fields';
import Message from 'components/Message';

import { commonStyles } from '../commonStyles';
import LayoutInfo from '../LayoutInfo';
import AccelerationAggregate from './AccelerationAggregate';

@Radium
export class AccelerationBasic extends Component {
  static getFields() {
    return AccelerationAggregate.getFields();
  }
  static validate(values) {
    return AccelerationAggregate.validate(values);
  }
  static propTypes = {
    dataset: PropTypes.instanceOf(Immutable.Map).isRequired,
    reflections: PropTypes.instanceOf(Immutable.Map).isRequired,
    location: PropTypes.object.isRequired,
    fields: PropTypes.object,
    handleSubmit: PropTypes.func,
    submit: PropTypes.func,
    onCancel: PropTypes.func,
    loadingRecommendations: PropTypes.bool,
    skipRecommendations: PropTypes.func
  };

  static contextTypes = {
    reflectionSaveErrors: PropTypes.instanceOf(Immutable.Map).isRequired
  };

  getHighlightedSection() {
    const {layoutId} = (this.props.location.state || {});
    if (!layoutId) return null;

    return this.props.reflections.getIn([layoutId, 'type']);
  }

  render() {
    const { fields, location, reflections, dataset, loadingRecommendations, skipRecommendations } = this.props;

    if (!fields.rawReflections.length || !fields.aggregationReflections.length) return null; // Form still initializing

    const { enabled } = fields.rawReflections[0];
    const toggleLabel = (
      <h3 style={commonStyles.toggleLabel}>
        <FontIcon type='RawMode' theme={commonStyles.iconTheme}/>
        {la('Raw Reflections')}
      </h3>
    );

    const firstRawLayout = reflections.find(r => r.get('type') === 'RAW');
    const firstAggLayout = reflections.find(r => r.get('type') === 'AGGREGATION');
    const highlightedSection = this.getHighlightedSection();

    const rawError = this.context.reflectionSaveErrors.get(fields.rawReflections[0].id.value);
    const rawErrorMessage = rawError && <Message
      messageType='error'
      inFlow={false}
      message={rawError.get('message')}
      messageId={rawError.get('id')}/>;

    const aggError = this.context.reflectionSaveErrors.get(fields.aggregationReflections[0].id.value);
    const aggErrorMessage = aggError && <Message
      messageType='error'
      inFlow={false}
      message={aggError.get('message')}
      messageId={aggError.get('id')}/>;

    return (
      <div style={styles.wrap} data-qa='raw-basic'>
        <div style={{...styles.formSection, marginBottom: 15}}>
          <div style={{
            ...commonStyles.header,
            ...(highlightedSection === 'RAW' ? commonStyles.highlight : {},
            {borderWidth: 0})
          }} data-qa='raw-queries-toggle' >
            <Toggle {...enabled} label={toggleLabel} style={commonStyles.toggle}/>
            <LayoutInfo
              layout={firstRawLayout}
              style={{float: 'right'}} />
          </div>
          <div style={{position: 'relative'}}>
            {rawErrorMessage}
          </div>
        </div>
        <AccelerationAggregate
          {...modalFormProps(this.props)}
          dataset={dataset}
          reflection={firstAggLayout}
          fields={fields}
          style={styles.formSection}
          location={location}
          shouldHighlight={highlightedSection === 'AGGREGATION'}
          errorMessage={aggErrorMessage}
          loadingRecommendations={loadingRecommendations}
          skipRecommendations={skipRecommendations}
        />
      </div>
    );
  }
}

const mapStateToProps = state => {
  const location = state.routing.locationBeforeTransitions;
  return {
    location
  };
};

export default connect(mapStateToProps)(AccelerationBasic);


const styles = {
  wrap: {
    ...FLEX_COL_START,
    width: '100%',
    height: '100%',
    position: 'relative',
    overflow: 'hidden'
  },
  body: {
    ...FLEX_COL_START,
    height: '100%',
    paddingLeft: 10
  },
  link: {
    ...pathLink,
    margin: '2px 0 0 10px',
    ':hover': {cursor: 'pointer'}
  },
  header: {
    margin: '10px 0',
    display: 'flex',
    alignItems: 'center'
  },
  column: {
    height: 30
  },
  formSection: {
    backgroundColor: '#f3f3f3',
    border: '1px solid #e1e1e1'
  }
};
