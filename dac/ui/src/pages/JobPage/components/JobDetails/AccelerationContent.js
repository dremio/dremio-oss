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
import { PureComponent } from 'react';
import PropTypes from 'prop-types';
import Immutable from 'immutable';
import { injectIntl, FormattedMessage } from 'react-intl';
import Art from 'components/Art';
import jobsUtils from 'utils/jobsUtils';
import ReflectionList from './ReflectionList';

@injectIntl
class AccelerationContent extends PureComponent {
  static propTypes = {
    jobDetails: PropTypes.instanceOf(Immutable.Map),
    intl: PropTypes.object.isRequired
  };

  render() {
    const byRelationship = jobsUtils.getReflectionsByRelationship(this.props.jobDetails);
    const allNotChosen = [...(byRelationship.MATCHED || []), ...(byRelationship.CONSIDERED || [])];

    const accelerated = this.props.jobDetails.get('accelerated');
    const summaryRow = <div className='detail-row'>
      <h4><FormattedMessage id='Job.Summary'/></h4>
      <div style={{display: 'flex', alignItems: 'center'}}>
        <Art src={accelerated ? 'Flame.svg' : 'FlameDisabled.svg'} alt='' style={{height: 20, marginRight: 5}} />
        <div>
          <FormattedMessage id={accelerated ? 'Job.Accelerated' : 'Job.NotAccelerated'} />
          {' '}
          {this.props.jobDetails.get('acceleration') && !Object.keys(byRelationship).length
            && <FormattedMessage id='Job.NoReflections'/>}
        </div>
      </div>
    </div>;

    return <div>
      {summaryRow}
      {byRelationship.CHOSEN && <div className='detail-row'>
        <h4><FormattedMessage id='Job.AcceleratedBy'/></h4>
        <ReflectionList reflections={byRelationship.CHOSEN} jobDetails={this.props.jobDetails} />
      </div>}
      {!!allNotChosen.length && <div className='detail-row'>
        <h4><FormattedMessage id='Job.NotChosenReflections'/></h4>
        <ReflectionList reflections={allNotChosen} jobDetails={this.props.jobDetails} />
      </div>}
    </div>;
  }
}


export default AccelerationContent;
