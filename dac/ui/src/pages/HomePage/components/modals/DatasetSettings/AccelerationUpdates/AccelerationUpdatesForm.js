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
import Immutable from "immutable";
import PropTypes from "prop-types";
import { connectComplexForm } from "components/Forms/connectComplexForm";
import { FormBody, ModalForm, modalFormProps } from "components/Forms";
import { label, section } from "uiTheme/radium/forms";
import { FieldSelect, Radio } from "components/Fields";
import DataFreshnessSection from "components/Forms/DataFreshnessSection";
import { intl } from "@app/utils/intl";
import { HoverHelp } from "dremio-ui-lib";

const SECTIONS = [DataFreshnessSection];

const INCREMENTAL = "INCREMENTAL";
const FULL = "FULL";
const FIELDS = ["method", "refreshField"].concat(
  DataFreshnessSection.getFields()
);

export class AccelerationUpdatesForm extends Component {
  static propTypes = {
    entity: PropTypes.instanceOf(Immutable.Map),
    submit: PropTypes.func,
    onCancel: PropTypes.func,
    handleSubmit: PropTypes.func,
    updateFormDirtyState: PropTypes.func,
    location: PropTypes.object,
    fields: PropTypes.object,
    values: PropTypes.object,
    errors: PropTypes.object,
    accelerationSettings: PropTypes.instanceOf(Immutable.Map),
    datasetFields: PropTypes.instanceOf(Immutable.List),
    entityType: PropTypes.string,
    fileFormatType: PropTypes.string,
    entityId: PropTypes.string,
  };

  whyCannotUseIncremental() {
    if (
      this.props.entityType === "physicalDataset" &&
      !this.props.datasetFields.size
    ) {
      return "no columns";
    }
    if (this.props.entityType === "file") {
      return "file-based";
    }
    if (this.props.fileFormatType === "Iceberg") {
      return "Iceberg";
    }
    if (this.props.fileFormatType === "Delta") {
      return "DeltaLake";
    }
    return undefined;
  }

  canUseIncremental() {
    return !this.whyCannotUseIncremental();
  }

  requiresIncrementalFieldSelection(values) {
    return (
      values.method === INCREMENTAL &&
      this.props.entityType === "physicalDataset"
    );
  }

  mapFormValues(values) {
    const { refreshField } = values;
    const requiredValues = {
      method: values.method,
      accelerationRefreshPeriod: values.accelerationRefreshPeriod,
      accelerationGracePeriod: values.accelerationGracePeriod,
      accelerationNeverExpire: values.accelerationNeverExpire,
      accelerationNeverRefresh: values.accelerationNeverRefresh,
    };

    if (this.requiresIncrementalFieldSelection(values)) {
      return {
        ...requiredValues,
        fieldList: [refreshField],
        refreshField,
      };
    }
    return requiredValues;
  }

  submitForm = (values) => {
    return this.props.submit(this.mapFormValues(values));
  };

  showRefreshMethodOptions() {
    const { accelerationSettings, entityType, fields } = this.props;

    const { formatMessage } = intl;
    const refreshMethod = accelerationSettings?.get("method");
    const incrementalLabel =
      entityType === "folder"
        ? formatMessage({ id: "Incremental.Update.NewFiles" })
        : formatMessage({ id: "Incremental.Update" });

    if (refreshMethod === "AUTO") {
      return <p>{intl.formatMessage({ id: "Refresh.Method.Auto" })}</p>;
    } else if (!this.canUseIncremental()) {
      const reasonForDisablingIncremental = this.whyCannotUseIncremental();
      const isPhysicalTableWithoutColumns =
        reasonForDisablingIncremental === "no columns";

      return (
        <p>
          {intl.formatMessage(
            {
              id: isPhysicalTableWithoutColumns
                ? "Refresh.Method.Full.NoColumns"
                : "Refresh.Method.Full",
            },
            {
              datasetType: reasonForDisablingIncremental,
              b: (chunk) => <b>{chunk}</b>,
            }
          )}
        </p>
      );
    } else {
      return (
        <>
          <Radio
            {...fields.method}
            radioValue={FULL}
            style={styles.margin}
            label={formatMessage({ id: "Full.Update" })}
          />
          <Radio
            {...fields.method}
            radioValue={INCREMENTAL}
            style={styles.margin}
            label={incrementalLabel}
          />
        </>
      );
    }
  }

  renderContent() {
    const { fields, values, entity } = this.props;
    const { formatMessage } = intl;
    const helpContent = formatMessage({
      id: "Refresh.Method.ForReflectionsUsingDataFromThisMethod",
    });
    return (
      <div>
        <div style={{ ...section, marginBottom: 26 }}>
          <span style={styles.label}>
            {formatMessage({ id: "Refresh.Method" })}
            <HoverHelp content={helpContent} />
          </span>
          <div style={styles.items}>
            {this.showRefreshMethodOptions()}
            {this.requiresIncrementalFieldSelection(values) ? (
              <label style={styles.fieldSelectWrap}>
                <span style={label}>
                  {formatMessage({
                    id: "Identify.NewRows.UsingTheColumn",
                  })}
                </span>
                <FieldSelect
                  formField={fields.refreshField}
                  style={styles.fieldSelect}
                  items={this.props.datasetFields.toJS()}
                />
              </label>
            ) : null}
          </div>
        </div>
        <DataFreshnessSection
          fields={fields}
          entityType="dataset"
          entity={entity}
          datasetId={this.props.entityId}
        />
      </div>
    );
  }

  render() {
    const { handleSubmit, onCancel } = this.props;
    return (
      <ModalForm
        {...modalFormProps(this.props)}
        onSubmit={handleSubmit(this.submitForm)}
        onCancel={onCancel}
      >
        <FormBody>{this.renderContent()}</FormBody>
      </ModalForm>
    );
  }
}

const styles = {
  margin: {
    marginBottom: 5,
  },
  fieldSelectWrap: {
    display: "flex",
    marginLeft: 23,
    alignItems: "center",
  },
  fieldSelect: {
    marginLeft: 5,
  },
  items: {
    display: "flex",
    flexDirection: "column",
  },
  label: {
    ...label,
    display: "flex",
    alignItems: "center",
    fontSize: 18,
    fontWeight: 600,
    marginBottom: 16,
    color: "var(--color--neutral--900)",
  },
};

const mapStateToProps = (state, ownProps) => {
  const settings = ownProps.accelerationSettings || Immutable.Map({});

  const accelerationRefreshPeriod = settings.has("accelerationRefreshPeriod")
    ? settings.get("accelerationRefreshPeriod")
    : DataFreshnessSection.defaultFormValueRefreshInterval();
  const accelerationGracePeriod = settings.has("accelerationGracePeriod")
    ? settings.get("accelerationGracePeriod")
    : DataFreshnessSection.defaultFormValueGracePeriod();
  const accelerationNeverExpire = settings.has("accelerationNeverExpire")
    ? settings.get("accelerationNeverExpire")
    : false;
  const accelerationNeverRefresh = settings.has("accelerationNeverRefresh")
    ? settings.get("accelerationNeverRefresh")
    : false;

  return {
    initialValues: {
      method: settings.get("method") || "FULL",
      refreshField:
        settings.get("refreshField") ||
        ownProps.datasetFields.getIn([0, "name"]) ||
        "",
      accelerationRefreshPeriod,
      accelerationGracePeriod,
      accelerationNeverExpire,
      accelerationNeverRefresh,
    },
  };
};

export default connectComplexForm(
  {
    form: "accelerationUpdatesForm",
    fields: FIELDS,
  },
  SECTIONS,
  mapStateToProps,
  null
)(AccelerationUpdatesForm);
