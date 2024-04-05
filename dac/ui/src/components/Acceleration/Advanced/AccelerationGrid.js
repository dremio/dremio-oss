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
import { Component } from "react";
import { connect } from "react-redux";
import PropTypes from "prop-types";
import Immutable from "immutable";
import { injectIntl } from "react-intl";

import { IconButton as OldIconButton } from "dremio-ui-lib";
import { IconButton, Spinner } from "dremio-ui-lib/components";
import FontIcon from "components/Icon/FontIcon";
import { Column, Table } from "fixed-data-table-2";
import { AutoSizer } from "react-virtualized";
import SearchField from "components/Fields/SearchField";
import PrevalidatedTextField from "components/Fields/PrevalidatedTextField";
import Select from "components/Fields/Select";
import Toggle from "components/Fields/Toggle";
import FieldWithError from "components/Fields/FieldWithError";
import Modal from "components/Modals/Modal";
import ModalForm from "components/Forms/ModalForm";
import FormBody from "components/Forms/FormBody";
import Message from "components/Message";

import {
  getAggregationRecommendation,
  getRawRecommendation,
} from "@app/selectors/reflectionRecommendations";
import AccelerationGridMixin from "@inject/components/Acceleration/Advanced/AccelerationGridMixin.js";
import EllipsedText from "@app/components/EllipsedText";

import { typeToIconType } from "@app/constants/DataTypes";
import { PartitionTransformations } from "dremio-ui-common/sonar/reflections/ReflectionDataTypes.js";
import { ColumnTypeIcons } from "@app/exports/components/PartitionTransformation/PartitionTransformationUtils";
import { getSupportFlags } from "@app/selectors/supportFlags";
import {
  ALLOW_REFLECTION_PARTITION_TRANFORMS,
  ALLOW_REFLECTION_REFRESH,
} from "@app/exports/endpoints/SupportFlags/supportFlagConstants";
import { refreshReflection } from "@app/exports/endpoints/ReflectionSummary/refreshReflection";

import "@app/uiTheme/less/commonModifiers.less";
import "@app/uiTheme/less/Acceleration/Acceleration.less";
import "@app/uiTheme/less/Acceleration/AccelerationGrid.less";
import LayoutInfo from "../LayoutInfo";

import "fixed-data-table-2/dist/fixed-data-table.css";
import { getFeatureFlag } from "@app/selectors/featureFlagsSelector";
import { REFLECTION_REFRESH_ENABLED } from "@inject/featureFlags/flags/REFLECTION_REFRESH_ENABLED";
import { isNotSoftware } from "@app/utils/versionUtils";

const HEADER_HEIGHT = 90;
const COLUMN_WIDTH = 80;
const GRID_PADDING = 20;

@AccelerationGridMixin
export class AccelerationGrid extends Component {
  static propTypes = {
    columns: PropTypes.instanceOf(Immutable.List),
    allColumns: PropTypes.instanceOf(Immutable.List),
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
    hasPermission: PropTypes.any,
    applyPartitionRecommendations: PropTypes.func,
    allowPartitionTransform: PropTypes.bool,
    recommendation: PropTypes.object,
    loadingRecommendations: PropTypes.bool,
    allowReflectionRefresh: PropTypes.bool,
  };

  static defaultProps = {
    columns: Immutable.List(),
  };

  static contextTypes = {
    reflectionSaveErrors: PropTypes.instanceOf(Immutable.Map).isRequired,
    lostFieldsByReflection: PropTypes.object.isRequired,
  };

  state = {
    tableWidth: 900,
    visibleLayoutExtraSettingsIndex: -1,
    disabledRefresh: {},
  };

  focusedColumn = undefined;

  componentDidMount() {
    this.updateResizeTable();
    if (window.addEventListener) {
      window.addEventListener("resize", this.updateResizeTable);
    }
  }

  componentDidUpdate(nextProps) {
    const curDisabledKeys = Object.keys(this.state.disabledRefresh);
    if (curDisabledKeys.length > 0) {
      const reflections = nextProps.reflections?.toJS();
      let removed = false;
      const newDisabledRefresh = { ...this.state.disabledRefresh };
      curDisabledKeys.forEach((key) => {
        if (
          reflections &&
          (reflections[key].status.refresh === "PENDING" ||
            reflections[key].status.refresh === "RUNNING")
        ) {
          removed = true;
          delete newDisabledRefresh[key];
        }
      });
      if (removed) this.setState({ disabledRefresh: newDisabledRefresh });
    }
  }

  componentWillUnmount() {
    if (window.removeEventListener) {
      window.removeEventListener("resize", this.updateResizeTable);
    }
  }

  UNSAFE_componentWillReceiveProps(nextProps) {
    if (nextProps.activeTab !== this.props.activeTab) {
      this.focusedColumn = undefined;
    } else if (nextProps.layoutFields.length > this.props.layoutFields.length) {
      this.focusedColumn = this.props.layoutFields.length;
    }
  }

  updateResizeTable = () => {
    if (this.gridWrapper) {
      this.setState({
        tableWidth:
          this.gridWrapper.getBoundingClientRect().width - GRID_PADDING,
      });
    }
  };

  renderLeftHeaderCell = () => {
    const isAdminOrHasCanAlter = this.checkIfUserHasCanAlter();
    return (
      <div className={"AccelerationGrid__leftHeader"}>
        <div
          className={`${
            isAdminOrHasCanAlter
              ? " AccelerationGrid__enabledSearchField"
              : "AccelerationGrid__disabledSearchField"
          }`}
        >
          <SearchField
            inputClassName={"AccelerationGrid__input"} // SHAWN: change according to Shashi's comment
            showCloseIcon
            value={this.props.filter}
            placeholder={laDeprecated("Search columns…")}
            onChange={this.props.onFilterChange}
            style={{ paddingBottom: 0 }}
          />
          <div className={"AccelerationGrid__fields"}>
            <h4>{laDeprecated("Columns")}</h4>
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
      overlayMessage = (
        <Message
          className={"AccelerationGrid__message"}
          messageType="error"
          inFlow={false}
          useModalShowMore
          message={error.get("message")}
          messageId={error.get("id")}
        />
      );
    } else if (lostFields && !shouldDelete) {
      const details = [];

      /* Older version for ref : "The following {fieldListName, select, partitionFields {Partition} distributionFields {Distribution} displayFields {Display}
      dimensionFields {Dimension} measureFields {Measure}} fields are no longer a part of the dataset and will be removed from the Reflection:", */
      const fieldDisplayMap = {
        displayFields: "display",
        dimensionFields: "dimension",
        measureFields: "measure",
        sortFields: "sort",
        partitionFields: "partition",
        distributionFields: "distribution",
      };

      for (const [fieldListName, value] of Object.entries(fieldDisplayMap)) {
        if (lostFields[fieldListName]) {
          details.push(
            <div>
              {this.props.intl.formatMessage(
                {
                  id: "Reflection.LostColumnsPreamble",
                },
                { columnNames: value },
              )}
              <ul style={{ listStyle: "disc", margin: ".5em 0 1em 2em" }}>
                {lostFields[fieldListName].map((field) => {
                  const { name, granularity } = field;
                  return (
                    <li key={name}>
                      {name} {granularity && `(${granularity})`}
                    </li>
                  );
                })}
              </ul>
            </div>,
          );
        }
      }

      overlayMessage = (
        <Message
          className={"AccelerationGrid__message"}
          messageType="warning"
          inFlow={false}
          useModalShowMore
          message={
            new Immutable.Map({
              code: "REFLECTION_LOST_FIELDS",
              moreInfo: <div>{details}</div>,
            })
          }
          isDismissable={false}
        />
      );
    }

    let textMessage;
    if (shouldDelete) {
      textMessage = laDeprecated("will remove");
    } else if (!enabled) {
      textMessage = laDeprecated("disabled");
    } else if (!layoutData) {
      textMessage = laDeprecated("new");
    }

    // todo: loc, ax
    return (
      <div className={"AccelerationGrid__status"}>
        {overlayMessage}
        <LayoutInfo
          layout={layoutData}
          className={"AccelerationGrid__layoutMessage"}
          overrideTextMessage={textMessage}
        />
      </div>
    );
  }

  // update this if BUCKET is ever added to the recommendations
  formatRecommendation = (partitionTransformation) => {
    const transformationType = partitionTransformation.type;

    switch (transformationType) {
      case PartitionTransformations.TRUNCATE:
        return `by truncation length ${partitionTransformation.truncateTransform.truncateLength}`;
      default:
        return `by ${partitionTransformation.type.toLowerCase()}`;
    }
  };

  generateTooltipMessage = () => {
    const {
      allColumns,
      intl: { formatMessage },
      recommendation: { partitionFields },
    } = this.props;

    const partitionFieldsWithColumnTypes = partitionFields.map(
      (partitionField) => ({
        ...partitionField,
        columnType: allColumns
          .find((column) => column.get("name") === partitionField.name)
          .getIn(["type", "name"]),
      }),
    );

    return (
      <>
        <p>
          {formatMessage({ id: "Acceleration.PartitionRecommendations.Title" })}
        </p>
        <br />
        <p>
          {formatMessage({
            id: "Acceleration.PartitionRecommendations.SubTitle",
          })}
        </p>
        <br />
        <p>
          {partitionFieldsWithColumnTypes?.map((partitionField) => (
            <span
              className="flex --alignCenter"
              style={{ gap: "var(--dremio--spacing--05)" }}
              key={partitionField.name}
            >
              <dremio-icon
                name={`column-types/${
                  ColumnTypeIcons[partitionField.columnType]
                }`}
              />
              {`${partitionField.name} (${this.formatRecommendation(
                partitionField.transform,
              )})`}
            </span>
          ))}
        </p>
      </>
    );
  };

  // also update this if BUCKET is ever added to the recommendations
  doCurrentPartitionsMatchRecommendations = (columnIndex) => {
    const {
      layoutFields,
      recommendation: { partitionFields },
    } = this.props;

    const curPartitionFields = layoutFields[columnIndex].partitionFields;

    if (partitionFields.length !== curPartitionFields.length) {
      return false;
    }

    return curPartitionFields.every((curPartitionField) => {
      const curPartitionValue = curPartitionField.name.value;

      // if a partition is recommended it will always include a transformation
      if (typeof curPartitionValue === "string") {
        return false;
      } else {
        // return false in cases where:
        // 1) no recommendation for the column exists
        // 2) the column's current transformation does not match the recommendation
        // 3) the recommended transformation is TRUNCATE and the lengths do not match

        const recommendedPartition = partitionFields.find(
          (partitionField) => partitionField.name === curPartitionValue.name,
        );

        const recommendedTransformation = recommendedPartition?.transform;
        const currentTransformation = curPartitionValue.transform;

        if (!recommendedPartition) {
          return false;
        }

        if (recommendedTransformation.type !== currentTransformation.type) {
          return false;
        }

        if (
          recommendedTransformation.type === PartitionTransformations.TRUNCATE
        ) {
          return (
            recommendedTransformation.truncateTransform.truncateLength ===
            currentTransformation.truncateTransform.truncateLength
          );
        }

        return true;
      }
    });
  };

  handleRefreshReflection = (reflectionId) => {
    try {
      refreshReflection({ reflectionId: reflectionId });
      this.setState((state) => ({
        disabledRefresh: { ...state.disabledRefresh, [reflectionId]: true },
      }));
    } catch (e) {
      //
    }
  };

  renderHeaderCell = (rowIndex, columnIndex, shouldJumpTo = false) => {
    const {
      allowPartitionTransform,
      applyPartitionRecommendations,
      recommendation: { partitionFields } = {},
      loadingRecommendations,
      allowReflectionRefresh,
    } = this.props;

    //todo: loc
    const fields = this.props.layoutFields[columnIndex];
    const shouldDelete = fields.shouldDelete.value;

    // todo: loc
    const placeholderName = this.props.intl.formatMessage({
      id: "Reflection.UnnamedReflection",
    });
    const name =
      this.props.layoutFields[columnIndex].name.value || placeholderName;
    const isAdminOrHasCanAlter = this.checkIfUserHasCanAlter();

    const shouldShowRecommendationsLoader =
      isAdminOrHasCanAlter && allowPartitionTransform && loadingRecommendations;

    const shouldShowRecommendations =
      isAdminOrHasCanAlter &&
      allowPartitionTransform &&
      !loadingRecommendations &&
      !!partitionFields?.length &&
      !this.doCurrentPartitionsMatchRecommendations(columnIndex);

    const reflectionId = fields.id.value;
    const reflection = this.props.reflections.get(reflectionId);
    const status = reflection?.getIn(["status", "refresh"]);
    const disableRefreshIcon =
      status === "PENDING" ||
      status === "RUNNING" ||
      !!this.state.disabledRefresh?.[reflectionId];

    return (
      <div
        className={`AccelerationGrid__header ${
          isAdminOrHasCanAlter ? "--bgColor-advEnDrag" : "--bgColor-advDisDrag"
        }`}
        data-qa={`reflection_${columnIndex}`}
      >
        <div
          className={`AccelerationGrid__layoutDescriptionLine ${
            shouldJumpTo ? "--bgColor-highlight" : null
          }`}
        >
          <div className={"AccelerationGrid__togglesContainer h4"}>
            <Toggle
              {...fields.enabled}
              className={"AccelerationGrid__toggle"}
              size="small"
            />
            {/*
              use PrevalidatedTextField as a buffer against expensive rerender as you type
            */}
            <PrevalidatedTextField
              {...this.props.layoutFields[columnIndex].name}
              placeholder={placeholderName}
              className={"AccelerationGrid__prevalidatedField"}
              style={{ textDecoration: shouldDelete ? "line-through" : null }}
              onKeyPress={(e) => {
                if (e.key === "Enter") e.preventDefault();
              }}
            />
            {shouldShowRecommendationsLoader && (
              <Spinner className="AccelerationGrid__layoutHeaderRecommendationSpinner" />
            )}
            {/* FIXME: the tooltip in the new IconButton component causes an infinite render loop
            when switching between Raw and Aggregate so we have to use the older version here */}
            {shouldShowRecommendations && (
              <OldIconButton
                tooltip={this.generateTooltipMessage()}
                tooltipPlacement="top"
                onClick={() =>
                  applyPartitionRecommendations(columnIndex, partitionFields)
                }
                className="AccelerationGrid__layoutHeaderRecommendationButton"
              >
                <dremio-icon
                  name="interface/warning"
                  class="AccelerationGrid__layoutHeaderRecommendationButton__icon"
                />
              </OldIconButton>
            )}
            {isAdminOrHasCanAlter && (
              <>
                {allowReflectionRefresh && reflection?.get("enabled") && (
                  <OldIconButton
                    tooltip={
                      !disableRefreshIcon
                        ? "Refresh now"
                        : `Reflection refresh ${
                            status === "RUNNING" ? "running" : "pending"
                          }`
                    }
                    tooltipPlacement="top"
                    onClick={() => this.handleRefreshReflection(reflectionId)}
                    className="AccelerationGrid__layoutHeaderRefreshButton"
                    disabled={disableRefreshIcon}
                  >
                    <dremio-icon
                      name="interface/refresh-clockwise"
                      class={`AccelerationGrid__layoutHeaderDeleteButton__icon ${disableRefreshIcon ? "--disabled" : ""}`}
                    />
                  </OldIconButton>
                )}
                <IconButton
                  aria-label={shouldDelete ? "Add" : "Delete"}
                  onClick={() => fields.shouldDelete.onChange(!shouldDelete)}
                  className="AccelerationGrid__layoutHeaderDeleteButton"
                >
                  <dremio-icon
                    name={shouldDelete ? "interface/add" : "interface/delete"}
                    class="AccelerationGrid__layoutHeaderDeleteButton__icon"
                  />
                </IconButton>
                <IconButton
                  aria-label="Settings"
                  onClick={() =>
                    this.setState({
                      visibleLayoutExtraSettingsIndex: columnIndex,
                    })
                  }
                  className="AccelerationGrid__layoutHeaderSettingsButton"
                >
                  <dremio-icon
                    name="interface/settings"
                    class="AccelerationGrid__layoutHeaderSettingsButton__icon"
                  />
                </IconButton>
              </>
            )}
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
      this.setState({ visibleLayoutExtraSettingsIndex: -1 });
    };
    return (
      <Modal
        size="smallest"
        style={{ width: 500, height: 250 }}
        title={laDeprecated("Settings: ") + name} //todo: text sub loc
        isOpen={this.state.visibleLayoutExtraSettingsIndex === columnIndex}
        hide={hide}
      >
        <ModalForm
          onSubmit={hide}
          confirmText={laDeprecated("Close")}
          isNestedForm
        >
          <FormBody>
            <FieldWithError
              label={laDeprecated("Reflection execution strategy")}
            >
              <Select
                {...fields.partitionDistributionStrategy}
                style={{ width: 275 }}
                items={[
                  {
                    label: laDeprecated("Minimize Number of Files Produced"),
                    option: "CONSOLIDATED",
                  },
                  {
                    label: laDeprecated("Minimize Refresh Time"),
                    option: "STRIPED",
                  },
                ]}
              />
            </FieldWithError>
          </FormBody>
        </ModalForm>
      </Modal>
    );
  }

  renderSubCellHeaders() {
    const isRaw = this.props.activeTab === "raw";
    return (
      <div className={"AccelerationGrid__subCellHeader"}>
        {isRaw && (
          <div className={"AccelerationGrid__cell"}>
            {laDeprecated("Display")}
          </div>
        )}
        {!isRaw && (
          <div className={"AccelerationGrid__cell"}>
            {laDeprecated("Dimension")}
          </div>
        )}
        {!isRaw && (
          <div className={"AccelerationGrid__cell"}>
            {laDeprecated("Measure")}
          </div>
        )}
        <div className={"AccelerationGrid__cell"}>{laDeprecated("Sort")}</div>
        <div
          className={
            this.props.shouldShowDistribution
              ? "AccelerationGrid__cell"
              : "AccelerationGrid__lastCell"
          }
        >
          {laDeprecated("Partition")}
        </div>
        {this.props.shouldShowDistribution && (
          <div className={"AccelerationGrid__lastCell"}>
            {laDeprecated("Distribution")}
          </div>
        )}
      </div>
    );
  }

  renderLeftSideCell = (rowIndex) => {
    const {
      columns,
      hasPermission,
      intl: { formatMessage },
    } = this.props;
    const backgroundColor =
      rowIndex % 2 ? "--bgColor-advEnDark" : "--bgColor-advEnLight";
    const disabledBackgroundColor =
      rowIndex % 2 ? "--bgColor-advDisDark" : "--bgColor-advDisLight";
    return (
      <div
        className={`AccelerationGrid__leftCell --bColor-bottom
      ${hasPermission ? backgroundColor : disabledBackgroundColor}
      `}
      >
        <div className={"AccelerationGrid__column"}>
          <FontIcon
            type={typeToIconType[columns.getIn([rowIndex, "type", "name"])]}
            theme={theme.columnTypeIcon}
          />
          <EllipsedText
            title={
              hasPermission
                ? columns.getIn([rowIndex, "name"])
                : formatMessage({ id: "Read.Only" })
            }
            style={{ marginLeft: 5 }}
            text={columns.getIn([rowIndex, "name"])}
          />
        </div>
        <div>{columns.getIn([rowIndex, "queries"])}</div>
      </div>
    );
  };

  render() {
    const { columns, layoutFields, activeTab, hasPermission } = this.props;
    const width = activeTab === "raw" ? COLUMN_WIDTH * 4 : COLUMN_WIDTH * 5;

    const { layoutId } = this.props.location.state || {};

    let jumpToIndex = 0;
    const columnNodes = layoutFields.map((layout, index) => {
      const shouldJumpTo = layout.id.value === layoutId;

      const columnOutput = (
        <Column
          key={index}
          header={(props) =>
            this.renderHeaderCell(props.rowIndex, index, shouldJumpTo)
          }
          headerHeight={HEADER_HEIGHT}
          width={width}
          allowCellsRecycling
          cell={(props) => this.props.renderBodyCell(props.rowIndex, index)}
        />
      );

      if (shouldJumpTo) jumpToIndex = index;
      return columnOutput;
    });

    return (
      <div
        className="AccelerationGrid grid-acceleration"
        style={{ maxHeight: "calc(100vh - 376px)" }}
        ref={(wrap) => (this.gridWrapper = wrap)}
      >
        <AutoSizer>
          {({ height }) => (
            <Table
              rowHeight={30}
              rowsCount={columns.size}
              isColumnResizing={false}
              headerHeight={HEADER_HEIGHT}
              width={this.state.tableWidth}
              height={height}
              scrollToColumn={
                (typeof this.focusedColumn === "number"
                  ? this.focusedColumn
                  : jumpToIndex) + 1
              }
            >
              <Column
                header={this.renderLeftHeaderCell()}
                width={
                  COLUMN_WIDTH *
                  4 /* both raw/aggregrate show 4-wide field list */
                }
                fixed
                allowCellsRecycling
                cell={(props) =>
                  this.renderLeftSideCell(props.rowIndex, props.columnIndex)
                }
                allowOverflow={!hasPermission}
              />
              {columnNodes}
            </Table>
          )}
        </AutoSizer>
      </div>
    );
  }
}

AccelerationGrid = injectIntl(AccelerationGrid);

const mapStateToProps = (state, ownProps) => {
  const { activeTab } = ownProps;

  const location = state.routing.locationBeforeTransitions;
  const isReflectionSupportEnabled =
    getSupportFlags(state)[ALLOW_REFLECTION_REFRESH];

  return {
    allowPartitionTransform:
      getSupportFlags(state)[ALLOW_REFLECTION_PARTITION_TRANFORMS],
    allowReflectionRefresh: isNotSoftware()
      ? getFeatureFlag(REFLECTION_REFRESH_ENABLED) === "ENABLED" &&
        isReflectionSupportEnabled
      : isReflectionSupportEnabled,
    location,
    recommendation:
      activeTab === "raw"
        ? getRawRecommendation(state)
        : getAggregationRecommendation(state),
  };
};

export default connect(mapStateToProps)(AccelerationGrid);

// DX-34369: refactor FontIcon to take in these as classnames instead.
const theme = {
  columnTypeIcon: {
    Icon: {
      display: "flex",
      justifyContent: "center",
      alignItems: "center",
      height: 21,
      width: 24,
    },
  },
};
