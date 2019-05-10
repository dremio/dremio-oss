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
import Radium from 'radium';
import Immutable from 'immutable';
import { injectIntl } from 'react-intl';

import ExploreTableColumnFilter from 'pages/ExplorePage/components/ExploreTable/ExploreTableColumnFilter';

import * as ButtonTypes from 'components/Buttons/ButtonTypes';
import Button from 'components/Buttons/Button';

import { PALE_BLUE } from 'uiTheme/radium/colors.js';
import { sqlEditorButton } from 'uiTheme/radium/buttons';

import './TableControls.less';
import SqlToggle from './SqlEditor/SqlToggle';
import SampleDataMessage from './SampleDataMessage';

@injectIntl
@Radium
class TableControls extends PureComponent {

  static propTypes = {
    dataset: PropTypes.instanceOf(Immutable.Map).isRequired,
    exploreViewState: PropTypes.instanceOf(Immutable.Map).isRequired,

    groupBy: PropTypes.func.isRequired,
    addField: PropTypes.func,
    join: PropTypes.func.isRequired,
    sqlState: PropTypes.bool.isRequired,
    rightTreeVisible: PropTypes.bool,
    approximate: PropTypes.bool,
    intl: PropTypes.object.isRequired
  };

  constructor(props) {
    super(props);

    this.state = {
      tooltipState: false,
      anchorOrigin: {
        horizontal: 'right',
        vertical: 'bottom'
      },
      targetOrigin: {
        horizontal: 'right',
        vertical: 'top'
      }
    };
  }

  renderPreviewWarning() {
    const { approximate } = this.props;
    return approximate && <SampleDataMessage />;
  }

  renderButton({
    key,
    style,
    intlId, // international string id to apply. Will override text property if provided
    ...buttonProps
  }) {
    if (intlId) {
      buttonProps.text = this.props.intl.formatMessage({ id: intlId });
    }
    return <Button
      key={key}
      innerTextStyle={styles.innerTextStyle}
      type={ButtonTypes.SECONDARY}
      styles={{
        ...styles.activeButton,
        ...style
      }}
      iconStyle={{ Container: styles.iconContainer, Icon: styles.iconBox}}
      {...buttonProps}/>;
  }

  render() {
    const {
      rightTreeVisible,
      dataset,
      addField,
      groupBy,
      join,
      exploreViewState,
      sqlState
    } = this.props;

    const disable = exploreViewState.get('isInProgress');
    const width = rightTreeVisible ? { width: 'calc(100% - 251px)' } : {};

    return (
      <div className='table-controls' style={[styles.tableControls, disable && styles.disabledStyle, width]}>
        <div className='left-controls'>
          <div className='controls' style={styles.controlsInner}>
            {!sqlState && <SqlToggle dataset={dataset}/>}
            {
              this.renderButton({
                key: 'calc',
                icon: 'AddFields',
                intlId: 'Dataset.AddField',
                onClick: addField
              })
            }
            {
              this.renderButton({
                key: 'groupBy',
                icon: 'GroupBy',
                intlId: 'Dataset.GroupBy',
                onClick: groupBy
              })
            }
            {
              this.renderButton({
                key: 'Join',
                icon: 'Join',
                intlId: 'Dataset.Join',
                onClick: join
              })
            }
            <ExploreTableColumnFilter
              dataset={dataset}
            />
          </div>
        </div>
        <div className='right-controls'>
          {this.renderPreviewWarning()}
        </div>
      </div>
    );
  }
}

export const styles = {
  disabledStyle: {
    pointerEvents: 'none',
    opacity: 0.7
  },
  innerTextStyle: {
    top: '-7px',
    textAlign: 'left'
  },
  activeButton: {
    ...sqlEditorButton,

    color: 'rgb(0, 0, 0)',
    ':hover': {
      backgroundColor: 'rgb(229, 242, 247)'
    }
  },
  iconBox: {
    width: 24,
    height: 24
  },
  iconContainer: {
    marginRight: 1,
    lineHeight: '24px',
    width: 24,
    position: 'relative'
  },
  tableControls: {
    marginTop: 0,
    marginLeft: 0,
    paddingLeft: 5,
    height: 42,
    display: 'flex',
    alignItems: 'center',
    backgroundColor: PALE_BLUE
  },
  controlsInner: {
    height: 24
  }
};

export default TableControls;
