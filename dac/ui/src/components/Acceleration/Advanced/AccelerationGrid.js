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

import { formDescription, formLabel } from 'uiTheme/radium/typography';
import { typeToIconType } from 'constants/DataTypes';

import LayoutInfo from '../LayoutInfo';
import { commonStyles } from '../commonStyles';

import 'fixed-data-table-2/dist/fixed-data-table.css';
import './AccelerationGrid.less';

const HEADER_HEIGHT = 90;
const COLUMN_WIDTH = 70;
const GRID_PADDING = 20;

@injectIntl
@Radium
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
    intl: PropTypes.object.isRequired
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
  }

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
  }

  renderLeftHeaderCell = () => (
    <div style={styles.flexEnd}>
      <div style={styles.leftHeaderCell}>
        <SearchField
          showCloseIcon
          value={this.props.filter}
          placeholder={la('Search fields…')}
          onChange={this.props.onFilterChange}
          style={{paddingBottom: 0}}
        />
        <div style={styles.leftHeaderCellLabel}>
          <h4>{la('Fields')}</h4>
        </div>
      </div>
    </div>
  )

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
        messageType='error'
        inFlow={false}
        useModalShowMore
        messageTextStyle={{maxHeight: styles.layoutDescriptionLine.height * 2 - 2}} // -2 for border
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
        messageType='warning'
        inFlow={false}
        useModalShowMore
        messageTextStyle={{maxHeight: styles.layoutDescriptionLine.height * 2 - 2}} // -2 for border
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
    return <div style={{...styles.status, fontWeight: 'normal'}}>
      {overlayMessage}
      <LayoutInfo layout={layoutData} style={{...formDescription, width: '100%', flexGrow: 1, padding: '0 5px'}} overrideTextMessage={textMessage}/>
    </div>;
  }

  renderHeaderCell = (rowIndex, columnIndex, shouldJumpTo = false) => { //todo: loc
    const fields = this.props.layoutFields[columnIndex];
    const shouldDelete = fields.shouldDelete.value;

    // todo: loc
    const placeholderName = this.props.intl.formatMessage({id:'Reflection.UnnamedReflection'});
    const name = this.props.layoutFields[columnIndex].name.value || placeholderName;

    return (
      <div data-qa={`reflection_${columnIndex}`} style={{
        ...styles.flexEnd,
        borderRight: '1px solid #a8e0f1',
        borderLeft: '1px solid #a8e0f1',
        borderTop: '1px solid #a8e0f1',
        marginLeft: 10,
        backgroundColor: '#EFF6F9'
      }}>
        <div style={{...styles.layoutDescriptionLine, ...(shouldJumpTo ? commonStyles.highlight : {})}}>
          <div className='h4' style={{display: 'flex', alignItems: 'center', paddingLeft: 5}}>
            <Toggle {...fields.enabled} style={{width: 'auto'}} />
            {/*
              use PrevalidatedTextField as a buffer against expensive rerender as you type
            */}
            <PrevalidatedTextField {...this.props.layoutFields[columnIndex].name}
              placeholder={placeholderName}
              style={{...styles.prevalidatedField, textDecoration: shouldDelete ? 'line-through' : null}}
              onKeyPress={(e) => {
                if (e.key === 'Enter') e.preventDefault();
              }}
            />
            { <FontIcon type={shouldDelete ? 'Add' : 'Minus'}
              style={styles.layoutHeaderIcon}
              onClick={() => fields.shouldDelete.onChange(!shouldDelete)} />
            }
            <FontIcon
              type='SettingsMediumFilled'
              style={styles.layoutHeaderIcon}
              onClick={() => this.setState({visibleLayoutExtraSettingsIndex: columnIndex})} />
          </div>
        </div>
        {this.renderStatus(fields)}
        {this.renderSubCellHeaders()}
        {this.renderExtraLayoutSettingsModal(columnIndex, name)}
      </div>
    );
  }

  renderExtraLayoutSettingsModal(columnIndex, name) {
    const fields = this.props.layoutFields[columnIndex];

    const hide = () => {
      this.setState({visibleLayoutExtraSettingsIndex: -1});
    };
    return <Modal
      size='smallest'
      title={la('Settings: ') + name} //todo: text sub loc
      isOpen={this.state.visibleLayoutExtraSettingsIndex === columnIndex}
      hide={hide}
    >
      <ModalForm onSubmit={hide} confirmText={la('Close')} isNestedForm>
        <FormBody>
          <FieldWithError label={la('Reflection execution strategy:')}>
            <Select
              {...fields.partitionDistributionStrategy}
              style={{width: 250}}
              items={[
                {label: la('Minimize Number of Files Produced'), option: 'CONSOLIDATED'},
                {label: la('Minimize Refresh Time'), option: 'STRIPED'}
              ]}
            />
          </FieldWithError>
        </FormBody>
      </ModalForm>
    </Modal>;
  }

  renderSubCellHeaders() {
    const isRaw = this.props.activeTab === 'raw';
    return <div style={{ display: 'flex', justifyContent: 'space-between', ...formLabel }}>
      {isRaw && <div style={styles.cell}>{la('Display')}</div>}
      {!isRaw && <div style={styles.cell}>{la('Dimension')}</div>}
      {!isRaw && <div style={styles.cell}>{la('Measure')}</div>}
      <div style={styles.cell}>{la('Sort')}</div>
      <div style={this.props.shouldShowDistribution ? styles.cell : styles.lastCell}>{la('Partition')}</div>
      { this.props.shouldShowDistribution && <div style={styles.lastCell}>{la('Distribution')}</div> }
    </div>;
  }

  renderLeftSideCell = (rowIndex) => {
    const { columns } = this.props;
    const backgroundColor = rowIndex % 2 ? '#e5f3f0' : '#ebf9f6';
    const borderBottom = rowIndex === this.props.columns.size - 1 ? '1px solid #9de4d4' : '';
    return (
      <div style={{ ...styles.leftCell, backgroundColor, borderBottom }}>
        <div style={styles.column}>
          <FontIcon type={typeToIconType[columns.getIn([rowIndex, 'type', 'name'])]} theme={styles.columnTypeIcon}/>
          <span style={{ marginLeft: 5 }}>{columns.getIn([rowIndex, 'name'])}</span>
        </div>
        <div>{columns.getIn([rowIndex, 'queries'])}</div>
      </div>
    );
  }

  render() {
    const { columns, layoutFields, activeTab } = this.props;
    const width = activeTab === 'raw' ? COLUMN_WIDTH * 4 : COLUMN_WIDTH * 5;

    const {layoutId} = (this.props.location.state || {});

    let jumpToIndex;
    const columnNodes = layoutFields.map((layout, index) => {
      const shouldJumpTo = layout.id.value === layoutId;

      const column = <Column
        key={index}
        header={(props) => this.renderHeaderCell(props.rowIndex, index, shouldJumpTo)}
        headerHeight={HEADER_HEIGHT}
        width={width}
        allowCellsRecycling
        cell={(props) => this.props.renderBodyCell(props.rowIndex, index)}
      />;

      if (shouldJumpTo) jumpToIndex = index;
      return column;
    });

    return (
      <div
        className='grid-acceleration'
        style={{ width: '100%', overflow: 'hidden' }}
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


const styles = {
  base: {
    position: 'relative',
    display: 'flex',
    flexDirection: 'row'
  },
  gridWrapper: {
    position: 'absolute',
    left: 0,
    top: 0,
    backgroundColor: '#f3f3f3',
    flex: '0 0 75px'
  },
  flexEnd: {
    height: '100%',
    display: 'flex',
    flexDirection: 'column',
    justifyContent: 'flex-end'
  },
  leftHeaderCell: {
    borderRight: '1px solid #9de4d4',
    borderTop: '1px solid #9de4d4',
    borderLeft: '1px solid #9de4d4',
    borderBottom: '1px solid #e1e1e1',
    backgroundColor: '#e5f3f0'
  },
  leftHeaderCellLabel: {
    height: 30,
    display: 'flex',
    justifyContent: 'flex-start',
    paddingLeft: 10,
    alignItems: 'center'
    // backgroundColor: '#e5f3f0',
  },
  leftCell: {
    display: 'flex',
    justifyContent: 'space-between',
    alignItems: 'center',
    height: 30,
    padding: '0 5px',
    borderRight: '1px solid #9de4d4',
    borderLeft: '1px solid #9de4d4',
    ...formLabel
  },
  autoSizerWrap: {
    marginLeft: 150,
    display: 'flex',
    flexDirection: 'column',
    flex: '1 1 auto'
  },
  leftSideCell: {
    flex: '0 0 75px',
    zIndex: 10
  },
  layoutDescriptionLine: {
    height: 30,
    borderBottom: '1px solid #e1e1e1'
  },
  cell: {
    display: 'flex',
    justifyContent: 'center',
    alignItems: 'center',
    height: 30,
    width: '100%',
    borderBottom: '1px solid #e1e1e1',
    borderRight: '1px solid #e1e1e1'
  },
  column: {
    display: 'flex',
    alignItems: 'center',
    marginLeft: 4
  },
  columnTypeIcon: {
    Container: {
      paddingBottom: 5
    },
    Icon: {
      display: 'flex',
      justifyContent: 'center',
      alignItems: 'center',
      height: 21,
      width: 24
    }
  },
  prevalidatedField: {
    flex: 1,
    border: '1px solid transparent', // keep 1px to prevent wiggle on focus
    background: 'none',
    padding: 0,
    margin: 0,
    fontSize: 'inherit',
    fontWeight: 'inherit',
    width: 0
  },
  layoutHeaderIcon: { // todo: ax, hover
    flexGrow: 0,
    flexShrink: 0,
    cursor: 'pointer',
    height: 24
  }
};
styles.status = {
  ...styles.layoutDescriptionLine,
  display: 'flex',
  alignItems: 'center',
  position: 'relative' // for absolute Message
};

styles.lastCell = {
  ...styles.cell,
  borderRight: 0
};
