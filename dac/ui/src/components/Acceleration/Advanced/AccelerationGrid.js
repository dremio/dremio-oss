/*
 * Copyright (C) 2017-2019 Dremio Corporation
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
import { FormattedMessage, injectIntl } from 'react-intl';

import FontIcon from 'components/Icon/FontIcon';
import { Column, Table } from 'fixed-data-table-2';
import { AutoSizer } from 'react-virtualized';
import SearchField from 'components/Fields/SearchField';
import PrevalidatedTextField from 'components/Fields/PrevalidatedTextField';
import Select from 'components/Fields/Select';
import Toggle from 'components/Fields/Toggle';
import FieldWithError from 'components/Fields/FieldWithError';
import Modal from 'components/Modals/Modal';
import ModalForm from 'components/Forms/ModalForm';
import FormBody from 'components/Forms/FormBody';
import Message from 'components/Message';

import AccelerationGridMixin from '@inject/components/Acceleration/Advanced/AccelerationGridMixin.js';
import EllipsedText from '@app/components/EllipsedText';
import Checkbox from '@app/components/Fields/Checkbox';

import { typeToIconType } from '@app/constants/DataTypes';

import '@app/uiTheme/less/commonModifiers.less';
import { displayNone} from '@app/uiTheme/less/commonStyles.less';
import '@app/uiTheme/less/Acceleration/Acceleration.less';
import '@app/uiTheme/less/Acceleration/AccelerationGrid.less';
import LayoutInfo from '../LayoutInfo';

import 'fixed-data-table-2/dist/fixed-data-table.css';

const HEADER_HEIGHT = 90;
const COLUMN_WIDTH = 80;
const GRID_PADDING = 20;

@injectIntl
@Radium
@AccelerationGridMixin
export class AccelerationGrid extends Component {
  static propTypes = {
    columns: PropTypes.instanceOf(Immutable.List),
    shouldShowDistribution: PropTypes.bool,
    renderBodyCell: PropTypes.func,
    renderHeaderCellData: PropTypes.func,
    layoutFields: PropTypes.array,
    reflections: PropTypes.instanceOf(Immutable.Map).isRequired,
    onFilterChange: PropTypes.func,
    activeTab: PropTypes.string.isRequired,
    filter: PropTypes.string,
    location: PropTypes.object.isRequired,
    intl: PropTypes.object.isRequired,
    hasPermission: PropTypes.any
  };

  static defaultProps = {
    columns: Immutable.List()
  };

  static contextTypes = {
    reflectionSaveErrors: PropTypes.instanceOf(Immutable.Map).isRequired,
    lostFieldsByReflection: PropTypes.object.isRequired
  };

  state = {
    tableWidth: 900,
    visibleLayoutExtraSettingsIndex: -1
  };

  focusedColumn = undefined;

  componentDidMount() {
    this.updateResizeTable();
    if (window.addEventListener) {
      window.addEventListener('resize', this.updateResizeTable);
    }
  }

  componentWillUnmount() {
    if (window.removeEventListener) {
      window.removeEventListener('resize', this.updateResizeTable);
    }
  }

  componentWillReceiveProps(nextProps) {
    if (nextProps.activeTab !== this.props.activeTab) {
      this.focusedColumn = undefined;
    } else if (nextProps.layoutFields.length > this.props.layoutFields.length) {
      this.focusedColumn = this.props.layoutFields.length;
    }
  }

  updateResizeTable = () => {
    if (this.gridWrapper) {
      this.setState({ tableWidth: this.gridWrapper.getBoundingClientRect().width - GRID_PADDING});
    }
  };

  renderLeftHeaderCell = () => {
    const isAdminOrHasCanAlter = this.checkIfUserHasCanAlter();
    return (
      <div className={'AccelerationGrid__leftHeader'}>
        <div className={`${isAdminOrHasCanAlter ? ' AccelerationGrid__enabledSearchField' : 'AccelerationGrid__disabledSearchField'}`}>
          <SearchField
            inputClassName={'AccelerationGrid__input'} // SHAWN: change according to Shashi's comment
            showCloseIcon
            value={this.props.filter}
            placeholder={la('Search fields…')}
            onChange={this.props.onFilterChange}
            style={{paddingBottom: 0}}
          />
          <div className={'AccelerationGrid__fields'}>
            <h4>{la('Fields')}</h4>
          </div>
        </div>
      </div>
    );
  };

  renderStatus(fields) {
    const id = fields.id.value;
    const shouldDelete = fields.shouldDelete.value;
    const enabled = fields.enabled.value;
    const layoutData = this.props.reflections.get(id);

    let overlayMessage;
    const error = this.context.reflectionSaveErrors.get(id);
    const lostFields = this.context.lostFieldsByReflection[id];
    if (error) {
      overlayMessage = <Message
        className={'AccelerationGrid__message'}
        messageType='error'
        inFlow={false}
        useModalShowMore
        message={error.get('message')}
        messageId={error.get('id')}/>;
    } else if (lostFields && !shouldDelete) {
      const details = [];
      for (const fieldListName of 'displayFields dimensionFields measureFields sortFields partitionFields distributionFields'.split(' ')) {
        if (lostFields[fieldListName]) {
          details.push(<div>
            <FormattedMessage id='Reflection.LostFieldsPreamble' values={{fieldListName}} />
            <ul style={{listStyle: 'disc', margin: '.5em 0 1em 2em'}}>{lostFields[fieldListName].map(field => {
              return <li>{field.name} {field.granularity && `(${field.granularity})`}</li>;
            })}</ul>
          </div>);
        }
      }

      overlayMessage = <Message
        className={'AccelerationGrid__message'}
        messageType='warning'
        inFlow={false}
        useModalShowMore
        message={new Immutable.Map({
          code: 'REFLECTION_LOST_FIELDS',
          moreInfo: <div children={details} />
        })}
        isDismissable={false}/>;
    }

    let textMessage;
    if (shouldDelete) {
      textMessage = la('will remove');
    } else if (!enabled) {
      textMessage = la('disabled');
    } else if (!layoutData) {
      textMessage = la('new');
    }

    // todo: loc, ax
    return <div className={'AccelerationGrid__status'}>
      {overlayMessage}
      <LayoutInfo layout={layoutData} className={'AccelerationGrid__layoutMessage'} overrideTextMessage={textMessage}/>
    </div>;
  }

  renderHeaderCell = (rowIndex, columnIndex, shouldJumpTo = false) => { //todo: loc
    const fields = this.props.layoutFields[columnIndex];
    const shouldDelete = fields.shouldDelete.value;

    // todo: loc
    const placeholderName = this.props.intl.formatMessage({id:'Reflection.UnnamedReflection'});
    const name = this.props.layoutFields[columnIndex].name.value || placeholderName;
    const isAdminOrHasCanAlter = this.checkIfUserHasCanAlter();

    return (
      <div className={`AccelerationGrid__header ${isAdminOrHasCanAlter ? '--bgColor-advEnDrag' : '--bgColor-advDisDrag'}`}
        data-qa={`reflection_${columnIndex}`}>
        <div className={`AccelerationGrid__layoutDescriptionLine ${shouldJumpTo ? '--bgColor-highlight' : null}`}>
          <div className={'AccelerationGrid__togglesContainer h4'}>
            <Toggle {...fields.enabled} className={'AccelerationGrid__toggle'} size='small'/>
            {/*
              use PrevalidatedTextField as a buffer against expensive rerender as you type
            */}
            <PrevalidatedTextField {...this.props.layoutFields[columnIndex].name}
              placeholder={placeholderName}
              className={'AccelerationGrid__prevalidatedField'}
              style={{textDecoration: shouldDelete ? 'line-through' : null}}
              onKeyPress={(e) => {
                if (e.key === 'Enter') e.preventDefault();
              }}
            />
            { <FontIcon type={shouldDelete ? 'Add' : 'Minus'}
              iconClass={isAdminOrHasCanAlter ? 'AccelerationGrid__layoutHeaderDeleteIcon' : displayNone }
              onClick={() => fields.shouldDelete.onChange(!shouldDelete)} />
            }
            <FontIcon
              type='SettingsMediumFilled'
              iconClass={isAdminOrHasCanAlter ? 'AccelerationGrid__layoutHeaderSettingsIcon' : displayNone }
              onClick={() => this.setState({visibleLayoutExtraSettingsIndex: columnIndex})} />
          </div>
        </div>
        {this.renderStatus(fields)}
        {this.renderSubCellHeaders()}
        {this.renderExtraLayoutSettingsModal(columnIndex, name)}
      </div>
    );
  };

  renderExtraLayoutSettingsModal(columnIndex, name) {
    const fields = this.props.layoutFields[columnIndex];

    const hide = () => {
      this.setState({visibleLayoutExtraSettingsIndex: -1});
    };
    return <Modal
      size='smallest'
      style={{width: 500, height: 250}}
      title={la('Settings: ') + name} //todo: text sub loc
      isOpen={this.state.visibleLayoutExtraSettingsIndex === columnIndex}
      hide={hide}
    >
      <ModalForm onSubmit={hide} confirmText={la('Close')} isNestedForm>
        <FormBody>
          <FieldWithError label={la('Reflection execution strategy')}>
            <Select
              {...fields.partitionDistributionStrategy}
              style={{width: 250}}
              items={[
                {label: la('Minimize Number of Files Produced'), option: 'CONSOLIDATED'},
                {label: la('Minimize Refresh Time'), option: 'STRIPED'}
              ]}
            />
          </FieldWithError>
          <div className={'AccelerationGrid__checkRow'}>
            <Checkbox
              {...fields.arrowCachingEnabled}
              isOnOffSwitch
              label={la('Arrow caching')}
              toolTip={la('Increase query performance by locally caching in a performance optimized format.')}
              toolTipPosition='top-start'
            />
          </div>
        </FormBody>
      </ModalForm>
    </Modal>;
  }

  renderSubCellHeaders() {
    const isRaw = this.props.activeTab === 'raw';
    return <div className={'AccelerationGrid__subCellHeader'}>
      {isRaw && <div className={'AccelerationGrid__cell'}>{la('Display')}</div>}
      {!isRaw && <div className={'AccelerationGrid__cell'}>{la('Dimension')}</div>}
      {!isRaw && <div className={'AccelerationGrid__cell'}>{la('Measure')}</div>}
      <div className={'AccelerationGrid__cell'}>{la('Sort')}</div>
      <div className={this.props.shouldShowDistribution ? 'AccelerationGrid__cell' : 'AccelerationGrid__lastCell'}>{la('Partition')}</div>
      { this.props.shouldShowDistribution && <div className={'AccelerationGrid__lastCell'}>{la('Distribution')}</div> }
    </div>;
  }

  renderLeftSideCell = (rowIndex) => {
    const { columns, hasPermission, intl: { formatMessage } } = this.props;
    const backgroundColor = rowIndex % 2 ? '--bgColor-advEnDark' : '--bgColor-advEnLight';
    const disabledBackgroundColor = rowIndex % 2 ? '--bgColor-advDisDark' : '--bgColor-advDisLight';
    return (
      <div className={`AccelerationGrid__leftCell --bColor-bottom 
      ${hasPermission ? backgroundColor : disabledBackgroundColor}
      `}>
        <div className={'AccelerationGrid__column'}>
          <FontIcon type={typeToIconType[columns.getIn([rowIndex, 'type', 'name'])]} theme={theme.columnTypeIcon}/>
          <EllipsedText
            title={hasPermission ? columns.getIn([rowIndex, 'name']) : formatMessage({id: 'Read.Only'})}
            style={{ marginLeft: 5 }} text={columns.getIn([rowIndex, 'name'])}
          />
        </div>
        <div>{columns.getIn([rowIndex, 'queries'])}</div>
      </div>
    );
  };

  render() {
    const { columns, layoutFields, activeTab, hasPermission } = this.props;
    const width = activeTab === 'raw' ? COLUMN_WIDTH * 4 : COLUMN_WIDTH * 5;

    const {layoutId} = (this.props.location.state || {});

    let jumpToIndex = 0;
    const columnNodes = layoutFields.map((layout, index) => {
      const shouldJumpTo = layout.id.value === layoutId;

      const columnOutput = <Column
        key={index}
        header={(props) => this.renderHeaderCell(props.rowIndex, index, shouldJumpTo)}
        headerHeight={HEADER_HEIGHT}
        width={width}
        allowCellsRecycling
        cell={(props) => this.props.renderBodyCell(props.rowIndex, index)}
      />;

      if (shouldJumpTo) jumpToIndex = index;
      return columnOutput;
    });

    return (
      <div
        className='AccelerationGrid grid-acceleration'
        style={{ maxHeight: 'calc(100vh - 376px)'}}
        ref={(wrap) => this.gridWrapper = wrap}
      >
        <AutoSizer>
          { ({height}) => (
            <Table
              rowHeight={30}
              rowsCount={columns.size}
              isColumnResizing={false}
              headerHeight={HEADER_HEIGHT}
              width={this.state.tableWidth}
              height={height}
              scrollToColumn={(typeof this.focusedColumn === 'number' ? this.focusedColumn : jumpToIndex) + 1}>
              <Column
                header={this.renderLeftHeaderCell()}
                width={COLUMN_WIDTH * 4 /* both raw/aggregrate show 4-wide field list */}
                fixed
                allowCellsRecycling
                cell={(props) => this.renderLeftSideCell(props.rowIndex, props.columnIndex)}
                allowOverflow={!hasPermission}
              />
              { columnNodes }
            </Table>)
          }
        </AutoSizer>
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

export default connect(mapStateToProps)(AccelerationGrid);

// DX-34369: refactor FontIcon to take in these as classnames instead.
const theme = {
  columnTypeIcon: {
    Icon: {
      display: 'flex',
      justifyContent: 'center',
      alignItems: 'center',
      height: 21,
      width: 24
    }
  }
};
