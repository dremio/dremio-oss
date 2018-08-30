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
import { PureComponent } from 'react';
import PropTypes from 'prop-types';
import Immutable from 'immutable';
import { injectIntl } from 'react-intl';
import { Link } from 'react-router';
import EllipsedText from 'components/EllipsedText';
import ReflectionIcon from 'components/Acceleration/ReflectionIcon';
import jobsUtils from 'utils/jobsUtils';

import './ReflectionList.css';

@injectIntl
export default class ReflectionList extends PureComponent {
  static propTypes = {
    jobDetails: PropTypes.instanceOf(Immutable.Map),
    intl: PropTypes.object.isRequired,
    reflections: PropTypes.array.isRequired
  };

  static contextTypes = {
    location: PropTypes.object.isRequired,
    loggedInUser: PropTypes.object.isRequired
  };

  render() {
    const items = this.props.reflections.map(({relationship, dataset, reflection, materialization, datasetLink}) => {
      const name = reflection.name || this.props.intl.formatMessage({ id: 'Reflection.UnnamedReflection' });

      let desc = '';
      if (relationship === 'CHOSEN') {
        if (materialization.refreshChainStartTime) { // protect against pre-1.3 materializations
          const age = jobsUtils.msToHHMMSS(this.props.jobDetails.get('startTime') - materialization.refreshChainStartTime);
          desc = this.props.intl.formatMessage({ id: 'Reflection.Age' }, {age});
        }
      } else if (relationship === 'MATCHED') {
        desc = this.props.intl.formatMessage({ id: 'Reflection.TooExpensive' });
      } else if (relationship === 'CONSIDERED') {
        desc = this.props.intl.formatMessage({ id: 'Reflection.DidNotCoverQuery' });
      }

      const showLink = this.context.loggedInUser.admin && (reflection.type === 'RAW' || reflection.type === 'AGGREGATION');
      const showDatasetLink = !!datasetLink;

      return <li>
        <div>
          <ReflectionIcon reflection={reflection} style={{marginRight: 5}} />
          <div>
            <EllipsedText text={name}>
              {showLink && <Link to={{
                ...this.context.location,
                state: {
                  modal: 'AccelerationModal',
                  datasetId: dataset.id,
                  layoutId: reflection.id
                }
              }}>{name}</Link>}
            </EllipsedText>
            <EllipsedText text={dataset.path.join('.')}>
              {showDatasetLink && <Link to={datasetLink} title={datasetLink.title}>{dataset.path.join('.')}</Link>}
            </EllipsedText>
          </div>
        </div>
        <div>
          {desc}
        </div>
      </li>;
    });

    return <ul className='reflection-list' children={items} />;
  }
}
