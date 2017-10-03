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
import Immutable from 'immutable';
import { Link } from 'react-router';

import EllipsedText from 'components/EllipsedText';
import FontIcon from 'components/Icon/FontIcon';
import jobsUtils from 'utils/jobsUtils';
import {mapStateToText, mapStateToIcon, syntheticLayoutState} from 'utils/accelerationUtils';
import { formDescription } from 'uiTheme/radium/typography';

import Footprint from './Footprint';
import ValidityIndicator from './ValidityIndicator';


export default class LayoutInfo extends Component {
  static propTypes = {
    layout: PropTypes.instanceOf(Immutable.Map),
    showStateText: PropTypes.bool,
    showValidity: PropTypes.bool,
    style: PropTypes.object
  };

  render() {
    if (!this.props.layout) return null;
    const layoutData = this.props.layout.toJS();
    const marginRight = 10;

    const layoutState = syntheticLayoutState(layoutData);

    const jobsURL = jobsUtils.navigationURLForLayoutId(layoutData.id.id);

    // todo: ax
    return <div style={{...styles.main, ...this.props.style}}>
      {this.props.showValidity && <div style={{marginRight, height: 20}}>
        <ValidityIndicator isValid={layoutData && layoutData.hasValidMaterialization}/>
      </div>}
      <div title={mapStateToText(layoutState)} style={{display: 'flex', alignItems: 'center', marginRight}}>
        <Link to={jobsURL} style={{height: 24}}><FontIcon type={mapStateToIcon(layoutState)}/></Link>
        {this.props.showStateText && <div>{mapStateToText(layoutState)}</div>}
      </div>
      <EllipsedText style={{flex: '1 1', marginRight}}>{/* todo: figure out how to @text for this */}
        <b>{la('Footprint: ')}</b>
        <Footprint currentByteSize={layoutData.currentByteSize} totalByteSize={layoutData.totalByteSize} />
      </EllipsedText>
      <div>
        <Link to={jobsURL}>{la('jobs')} »</Link>
      </div>
    </div>;
  }
}

const styles = {
  main: {
    ...formDescription,
    padding: '0 5px',
    display: 'flex',
    alignItems: 'center'
  }
};
