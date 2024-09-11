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
import ReactDOM from "react-dom";
import Immutable from "immutable";
import PropTypes from "prop-types";
import { v4 as uuidv4 } from "uuid";
import deepEqual from "deep-equal";

import {
  connectComplexForm,
  FormBody,
  ModalForm,
  modalFormProps,
} from "@app/components/Forms";
import reflectionActions from "@app/actions/resources/reflection";
import { setReflectionRecommendations } from "@app/actions/resources/reflectionRecommendations";
import { getRawRecommendation } from "@app/selectors/reflectionRecommendations";
import { getListErrorsFromNestedReduxFormErrorEntity } from "@app/utils/validation";
import {
  areReflectionFormValuesBasic,
  areReflectionFormValuesUnconfigured,
  createReflectionFormValues,
  fixupReflection,
  forceChangesForDatasetChange,
} from "@app/utils/accelerationUtils";
import { fetchSupportFlags } from "@app/actions/supportFlags";
import { getSupportFlags } from "@app/selectors/supportFlags";
import {
  ALLOW_REFLECTION_PARTITION_TRANFORMS,
  ALLOW_REFLECTION_REFRESH,
  MANUALLY_GENERATE_RECOMMENDATION,
} from "@app/exports/endpoints/SupportFlags/supportFlagConstants";
import ApiUtils from "@app/utils/apiUtils/apiUtils";

import { DEFAULT_ERR_MSG } from "@inject/constants/errors";

import "@app/uiTheme/less/commonModifiers.less";
import "@app/uiTheme/less/Acceleration/Acceleration.less";
import { AccelerationFormWithMixin } from "@inject/components/Acceleration/AccelerationFormMixin";
import {
  formatPartitionFields,
  preparePartitionFieldsAsFormValues,
} from "@app/exports/components/PartitionTransformation/PartitionTransformationUtils";
import Message from "../Message";
import AccelerationBasic from "./Basic/AccelerationBasic";
import AccelerationAdvanced from "./Advanced/AccelerationAdvanced";
import { isNotSoftware } from "@app/utils/versionUtils";
import { REFLECTION_REFRESH_ENABLED } from "@inject/featureFlags/flags/REFLECTION_REFRESH_ENABLED";
import { postDatasetReflectionRecommendations } from "@app/endpoints/Reflections/postDatasetReflectionRecommendations";

const SECTIONS = [AccelerationBasic, AccelerationAdvanced];

export class AccelerationForm extends Component {
  static propTypes = {
    dataset: PropTypes.instanceOf(Immutable.Map).isRequired,
    reflections: PropTypes.instanceOf(Immutable.Map).isRequired,
    onCancel: PropTypes.func,
    handleSubmit: PropTypes.func.isRequired,
    location: PropTypes.object.isRequired,
    fields: PropTypes.object,
    error: PropTypes.object, // top-level
    errors: PropTypes.object, // specific fields (currently unused)
    dirty: PropTypes.bool,
    updateFormDirtyState: PropTypes.func,
    submitFailed: PropTypes.bool,
    values: PropTypes.object,
    destroyForm: PropTypes.func,
    isModal: PropTypes.bool,
    canAlter: PropTypes.any,
    canSubmit: PropTypes.bool,
    allowPartitionTransform: PropTypes.bool,
    rawRecommendation: PropTypes.object,
    isTriggerRecommendation: PropTypes.bool,

    putReflection: PropTypes.func.isRequired,
    postReflection: PropTypes.func.isRequired,
    deleteReflection: PropTypes.func.isRequired,
    reloadReflections: PropTypes.func,
    fetchSupportFlags: PropTypes.func,
    setReflectionRecommendations: PropTypes.func.isRequired,
  };

  static defaultProps = {
    canSubmit: true,
    errors: {},
    fields: {},
    acceleration: Immutable.Map(),
  };

  static childContextTypes = {
    reflectionSaveErrors: PropTypes.instanceOf(Immutable.Map).isRequired,
    lostFieldsByReflection: PropTypes.object.isRequired,
  };

  initialValues = null;
  lostFieldsByReflection = {};
  suggestions = {};

  getChildContext() {
    const { error } = this.props;
    let reflectionSaveErrors = new Immutable.Map();
    if (
      error &&
      Immutable.Map.isMap(error.message) &&
      error.message.get("code") === "COMBINED_REFLECTION_SAVE_ERROR"
    ) {
      reflectionSaveErrors = error.message
        .get("details")
        .get("reflectionSaveErrors");
    }
    return {
      reflectionSaveErrors,
      lostFieldsByReflection: this.lostFieldsByReflection,
    };
  }

  constructor(props) {
    super(props);

    const mode = this.getMustBeInAdvancedMode() ? "ADVANCED" : "BASIC";

    this.state = {
      mode,
      waitingForRecommendations: false,
      waitingForAggRecommendations: false,
      waitingForRawRecommendations: false,
      saving: false,
      formIsDirty: false,
    };
  }

  unmounted = false;
  componentWillUnmount() {
    this.props.setReflectionRecommendations([]);
    this.unmounted = true;
  }

  async UNSAFE_componentWillMount() {
    const { fetchSupportFlags } = this.props;
    await fetchSupportFlags(ALLOW_REFLECTION_PARTITION_TRANFORMS);
    await fetchSupportFlags(ALLOW_REFLECTION_REFRESH);
    await fetchSupportFlags(MANUALLY_GENERATE_RECOMMENDATION);
    if (isNotSoftware()) {
      await this.props.fetchFeatureFlag(REFLECTION_REFRESH_ENABLED);
    }
    this.initializeForm();
  }

  initializeForm() {
    let { dataset } = this.props;
    const { allowPartitionTransform, reflections, isTriggerRecommendation } =
      this.props;
    dataset = dataset.toJS();

    const lostFieldsByReflection = {};
    const aggregationReflectionValues = [];
    const rawReflectionValues = [];
    for (const reflection of reflections.toList().toJS()) {
      const { lostFields, reflection: fixedReflection } =
        forceChangesForDatasetChange(reflection, dataset);
      if (lostFields) lostFieldsByReflection[fixedReflection.id] = lostFields;

      if (fixedReflection.type === "RAW") {
        rawReflectionValues.push(createReflectionFormValues(fixedReflection));
      } else {
        aggregationReflectionValues.push(
          createReflectionFormValues(fixedReflection),
        );
      }
    }
    this.lostFieldsByReflection = lostFieldsByReflection;

    const { rawReflections, aggregationReflections } = this.props.fields;

    if (
      ((this.state.mode === "BASIC" && !aggregationReflectionValues.length) ||
        allowPartitionTransform) &&
      !isTriggerRecommendation
    ) {
      this.fetchRecommendationsOld();
    }

    const defaultToEnabled = false;

    // can't do this in mapStateToProps it puts redux-form in a render loop
    if (!aggregationReflectionValues.length) {
      const firstAgg = createReflectionFormValues({
        type: "AGGREGATION",
        enabled: defaultToEnabled,
      });
      aggregationReflectionValues.push(firstAgg);
    }
    if (!rawReflectionValues.length) {
      const firstRaw = createReflectionFormValues({
        type: "RAW",
        enabled: defaultToEnabled,
      });
      if (this.state.mode === "BASIC") {
        firstRaw.displayFields = dataset.fields.map(({ name }) => ({ name }));
      }
      this.suggestions[firstRaw.id] = firstRaw;
      rawReflectionValues.push(firstRaw);
    }

    const updatedAggregationReflectionValues = aggregationReflectionValues.map(
      (v) => {
        return {
          ...v,
          partitionFields: preparePartitionFieldsAsFormValues(v),
        };
      },
    );

    const updatedRawReflectionValues = rawReflectionValues.map((v) => {
      return {
        ...v,
        partitionFields: preparePartitionFieldsAsFormValues(v),
      };
    });

    updatedAggregationReflectionValues.forEach((v) =>
      aggregationReflections.addField(v),
    );

    updatedRawReflectionValues.forEach((v) => rawReflections.addField(v));

    this.syncAdvancedToBasic(updatedAggregationReflectionValues[0]);

    this.updateInitialValues();
  }

  fetchRecommendationsOld() {
    this.setState({ waitingForRecommendations: true });
    const endpoint = `dataset/${encodeURIComponent(
      this.props.dataset.get("id"),
    )}/reflection/recommendation`;

    return ApiUtils.fetchJson(
      endpoint,
      ({ data: reflections }) => {
        //handle json with data
        ReactDOM.unstable_batchedUpdates(() => {
          if (!this.state.waitingForRecommendations || this.unmounted) return;

          const isAdvancedMode = this.state.mode === "ADVANCED";

          if (
            (isAdvancedMode && !this.props.allowPartitionTransform) ||
            !reflections.length ||
            !reflections.some((r) => r.type === "AGGREGATION")
          ) {
            // protect - ensure we get at least one agg or if user switched to advanced mode
            this.setState({ waitingForRecommendations: false });
            return;
          }

          this.props.setReflectionRecommendations(reflections);

          const { aggregationReflections, rawReflections } = this.props.fields;
          const { reflections: curReflections } = this.props;

          const curReflectionsAsArray = Object.values(
            curReflections?.toJS() ?? {},
          );

          const hasExistingRawReflection = curReflectionsAsArray.some(
            (curReflection) => curReflection.type === "RAW",
          );

          const hasExistingAggregationReflection = curReflectionsAsArray.some(
            (curReflection) => curReflection.type === "AGGREGATION",
          );

          if (!hasExistingAggregationReflection && !isAdvancedMode) {
            aggregationReflections.forEach(() =>
              aggregationReflections.removeField(),
            );
          }

          if (!hasExistingRawReflection & !isAdvancedMode) {
            rawReflections.forEach(() => rawReflections.removeField());
          }

          for (const reflection of reflections) {
            // do not update the grid if the user goes to advanced mode before recommendations finish loading
            // this keeps the user from losing any changes they might've made
            if (isAdvancedMode) {
              break;
            }

            if (
              (reflection.type === "RAW" && hasExistingRawReflection) ||
              (reflection.type === "AGGREGATION" &&
                hasExistingAggregationReflection)
            ) {
              continue;
            }

            // we want to disable recommendations
            reflection.enabled = false;
            const values = createReflectionFormValues(reflection);
            this.suggestions[values.id] = values;

            reflection.type === "RAW"
              ? rawReflections.addField(values)
              : aggregationReflections.addField(values);
          }

          this.setState({ waitingForRecommendations: false });
          this.updateInitialValues([
            "aggregationReflections",
            "columnsDimensions",
            "columnsMeasures",
            "rawReflections",
          ]);
        });
      },
      (error) => {
        if (this.unmounted) return;
        // quietly treat recommendation failures as "no recommendations"
        console.error(error);
        this.setState({ waitingForRecommendations: false });
      },
      { method: "POST" },
    );
  }

  fetchRecommendations = async (type) => {
    this.setState(
      type === "agg"
        ? { waitingForAggRecommendations: true }
        : { waitingForRawRecommendations: true },
    );
    await postDatasetReflectionRecommendations({
      datasetId: this.props.dataset.get("id"),
      type: type,
    })
      .then(({ data: reflections }) => {
        ReactDOM.unstable_batchedUpdates(() => {
          if (
            (type === "agg" && !this.state.waitingForAggRecommendations) ||
            (type === "raw" && !this.state.waitingForRawRecommendations) ||
            this.unmounted
          )
            return;

          const isAdvancedMode = this.state.mode === "ADVANCED";

          if (
            (isAdvancedMode && !this.props.allowPartitionTransform) ||
            !reflections.length
          ) {
            // protect - ensure we get at least one agg or if user switched to advanced mode
            this.setState(
              type === "agg"
                ? { waitingForAggRecommendations: false }
                : { waitingForRawRecommendations: false },
            );
            return;
          }
          this.props.setReflectionRecommendations(reflections);

          const { aggregationReflections, rawReflections } = this.props.fields;
          const { reflections: curReflections } = this.props;

          const curReflectionsAsArray = Object.values(
            curReflections?.toJS() ?? {},
          );

          const hasExistingRawReflection = curReflectionsAsArray.some(
            (curReflection) => curReflection.type === "RAW",
          );

          const hasExistingAggregationReflection = curReflectionsAsArray.some(
            (curReflection) => curReflection.type === "AGGREGATION",
          );
          let curReflection = "";
          if (hasExistingAggregationReflection) {
            curReflection = curReflectionsAsArray.find(
              (curReflection) => curReflection.type === "AGGREGATION",
            );
          }

          if (!isAdvancedMode && type === "agg") {
            aggregationReflections.forEach(() =>
              aggregationReflections.removeField(),
            );
          }

          if (!hasExistingRawReflection && !isAdvancedMode && type === "raw") {
            rawReflections.forEach(() => rawReflections.removeField());
          }

          for (const reflection of reflections) {
            // do not update the grid if the user goes to advanced mode before recommendations finish loading
            // this keeps the user from losing any changes they might've made
            if (isAdvancedMode) {
              break;
            }

            if (reflection.type === "RAW" && hasExistingRawReflection) {
              continue;
            }

            reflection.enabled = type === "raw";
            const isUpdatingAgg =
              curReflection && reflection.type === "AGGREGATION";
            const values = createReflectionFormValues({
              ...reflection,
              ...(isUpdatingAgg && {
                tag: curReflection.tag,
                id: curReflection.id,
                name: curReflection.name,
                enabled: curReflection.enabled,
              }),
            });
            this.suggestions[values.id] = values;

            reflection.type === "RAW"
              ? rawReflections.addField(values)
              : aggregationReflections.addField(values);
          }

          this.setState(
            type === "agg"
              ? { waitingForAggRecommendations: false }
              : { waitingForRawRecommendations: false },
          );
        });

        return;
      })
      .catch((error) => {
        if (this.unmounted) return;
        // quietly treat recommendation failures as "no recommendations"
        console.error(error);
        this.setState(
          type === "agg"
            ? { waitingForAggRecommendations: false }
            : { waitingForRawRecommendations: false },
        );
      });
  };

  skipRecommendations = () => {
    this.setState(
      this.props.isTriggerRecommendation
        ? { waitingForAggRecommendations: false }
        : { waitingForRecommendations: false },
    );
  };

  updateInitialValues(fieldsToUpdate) {
    // let the redux update run so that this.props.values gets updated
    setTimeout(() => {
      //this.props.initializeForm(this.initialValues, true);
      let isFormDirty = false;
      if (fieldsToUpdate && fieldsToUpdate.length) {
        fieldsToUpdate.forEach((field) => {
          this.initialValues[field] = this.props.values[field];
        });
        isFormDirty = !deepEqual(this.props.values, this.initialValues);
      } else {
        this.initialValues = { ...this.props.values };
      }
      this.props.updateFormDirtyState(isFormDirty);
      this.setState({ formIsDirty: isFormDirty });
    }, 0);
  }

  componentDidUpdate(prevProps) {
    const { updateFormDirtyState, values } = this.props;

    const { formIsDirty, mode, saving } = this.state;

    const { values: prevValues } = prevProps;

    if (saving) {
      return;
    }

    const firstAggReflection = values.aggregationReflections[0];
    const prevFirstAggReflection = prevValues.aggregationReflections[0];

    // sync any changes made to the first aggregation reflection in ADVANCED to BASIC
    if (!deepEqual(firstAggReflection, prevFirstAggReflection)) {
      this.syncAdvancedToBasic(firstAggReflection);
    }

    const { columnsDimensions, columnsMeasures } = values;
    const {
      columnsDimensions: prevColumnsDimensions,
      columnsMeasures: prevColumnsMeasures,
    } = prevValues;

    // sync any changes made to the aggregation'sdimensions / measures in BASIC to ADVANCED
    if (
      !deepEqual(columnsDimensions, prevColumnsDimensions) ||
      !deepEqual(columnsMeasures, prevColumnsMeasures)
    ) {
      this.syncBasicToAdvanced();
    }

    // will force the form to ADVANCED mode if the recommendations contain non-basic values,
    // if a reflection with non-basic values exists, or multiple reflections exist
    if (mode === "BASIC" && this.getMustBeInAdvancedMode(this.props)) {
      this.setState({ mode: "ADVANCED" });
    }

    const isDirty = !deepEqual(values, this.initialValues);

    if (formIsDirty !== isDirty) {
      this.setState({ formIsDirty: isDirty });
      updateFormDirtyState(isDirty);
    }
  }

  syncAdvancedToBasic(firstAggValues) {
    const { columnsDimensions, columnsMeasures } = this.props.fields;

    columnsDimensions.forEach(() => columnsDimensions.removeField());
    columnsMeasures.forEach(() => columnsMeasures.removeField());

    if (!firstAggValues) return;

    firstAggValues.dimensionFields.forEach(({ name }) =>
      columnsDimensions.addField({ column: name }),
    );
    firstAggValues.measureFields.forEach(({ name }) =>
      columnsMeasures.addField({ column: name }),
    );
  }

  syncBasicToAdvanced() {
    const columnsDimensionsValues = this.props.values.columnsDimensions;
    const columnsMeasuresValues = this.props.values.columnsMeasures;
    const firstAgg = this.props.fields.aggregationReflections[0];

    // Note: we are careful to preserve granularity when syncing this direction

    const dimensionSet = new Set(columnsDimensionsValues.map((v) => v.column));
    const measureSet = new Set(columnsMeasuresValues.map((v) => v.column));

    firstAgg.dimensionFields.forEach(({ name }, i) => {
      if (!dimensionSet.delete(name.value))
        firstAgg.dimensionFields.removeField(i);
    });
    firstAgg.measureFields.forEach(({ name }, i) => {
      if (!measureSet.delete(name.value)) firstAgg.measureFields.removeField(i);
    });

    for (const name of dimensionSet)
      firstAgg.dimensionFields.addField({ name });
    for (const name of measureSet) firstAgg.measureFields.addField({ name });
  }

  toggleMode = (e) => {
    e.stopPropagation();
    e.preventDefault();
    const { mode } = this.state;
    this.setState({
      mode: mode === "BASIC" ? "ADVANCED" : "BASIC",
    });
  };

  clearReflections = (e) => {
    e.stopPropagation();
    e.preventDefault();
    this.props.fields.rawReflections
      .concat(this.props.fields.aggregationReflections)
      .forEach((reflection) => reflection.shouldDelete.onChange(true));
  };

  prepare(values) {
    const { mode } = this.state;
    const { aggregationReflections, rawReflections } = values;

    if (aggregationReflections[0]) {
      aggregationReflections[0].measureFields =
        aggregationReflections[0].measureFields.filter((field) => field.name);
    }

    let reflections = [...aggregationReflections, ...rawReflections];
    for (const reflection of reflections) {
      if (
        mode === "BASIC" &&
        reflection.type === "RAW" &&
        !reflection.enabled
      ) {
        reflection.shouldDelete = true;
      }

      if (
        !reflection.enabled &&
        areReflectionFormValuesUnconfigured(reflection)
      ) {
        reflection.shouldDelete = true;
      }

      fixupReflection(reflection, this.props.dataset);
    }

    reflections = reflections.filter((reflection) => {
      // can simply ignore new reflections which were then deleted
      return reflection.tag || !reflection.shouldDelete;
    });

    // todo: reveal/scroll to errored reflection
    const errors = {};
    for (const reflection of reflections) {
      if (reflection.shouldDelete || !reflection.enabled) continue;

      if (reflection.type === "RAW") {
        if (!reflection.displayFields.length) {
          errors[reflection.id] = laDeprecated(
            "At least one display column per raw Reflection is required.",
          );
        }
      } else {
        // AGGREGATION
        if (
          !reflection.dimensionFields.length &&
          !reflection.measureFields.length
        ) {
          // eslint-disable-line no-lonely-if
          errors[reflection.id] = laDeprecated(
            "At least one dimension or measure column per aggregation Reflection is required.",
          );
        }
      }
    }

    return { reflections, errors };
  }

  updateReflection(id, data) {
    const field = this.props.fields.rawReflections
      .concat(this.props.fields.aggregationReflections)
      .find((reflection) => reflection.id.value === id);
    if (field) {
      for (const [key, value] of Object.entries(data)) {
        field[key].onChange(value);
      }
    }
  }

  submitForm = (values) => {
    const { allowPartitionTransform, reloadReflections } = this.props;
    const { reflections, errors } = this.prepare(values);

    this.setState({ saving: true });

    const promises = reflections.map((reflection) => {
      const reflectionId = reflection.id;
      if (errors[reflectionId]) return; // don't save

      const shouldDelete = reflection.shouldDelete;
      delete reflection.shouldDelete;

      // add the datasetid before we send the reflections out
      reflection.datasetId = this.props.dataset.get("id");

      if (allowPartitionTransform) {
        reflection.partitionFields = formatPartitionFields(reflection);
      }

      let promise;
      let cleanup;
      if (!reflection.tag) {
        // new, unsaved, reflections have fake ids for tracking, but no tag
        delete reflection.id;
        delete reflection.tag;

        promise = this.props.postReflection(reflection);
        cleanup = ({ id, tag }) => {
          this.updateReflection(reflectionId, { tag, id }); // no longer new
        };
      } else if (shouldDelete) {
        promise = this.props.deleteReflection(reflection);
        cleanup = () => {
          this.updateReflection(reflectionId, {
            // it's now new
            tag: "",
            id: uuidv4(),
          });
        };
      } else {
        promise = this.props.putReflection(reflection);
        cleanup = ({ tag }) => {
          this.updateReflection(reflectionId, { tag }); // tag may have updated
        };
      }

      // not using ApiUtils.attachFormSubmitHandlers because not yet ready to map validation errors to specific fields
      // Also: need to collect all the errors (not default Promise.all behavior), so catching everything without re-reject (until later)
      return promise
        .then((action) => {
          if (!action || !action.error) {
            const newData =
              action.payload &&
              action.payload.get("entities").get("reflection").first().toJS();
            cleanup(newData); // make it so that if some reflection saving succeeds and some fail the user can correct issues and resubmit safely
            return action;
          }
          const error = action.payload;

          // todo: Charles abort not respected (modal closes) - why????
          // todo: if a delete succeeds and another call fails then we can end up with no reflections of a type

          // start with fallback
          errors[reflectionId] = error.message || DEFAULT_ERR_MSG;

          const { response } = error;
          if (response) {
            if (response.errorMessage || response.message) {
              errors[reflectionId] = response;
            }
          }

          return action;
        })
        .catch((error) => {
          errors[reflectionId] = "Request Error: " + error.statusText; // todo: loc
        });
    });

    return Promise.all(promises).then(() => {
      this.setState({ saving: false });
      this.updateInitialValues();
      reloadReflections();

      if (Object.keys(errors).length)
        return this.createSubmitErrorWrapper(
          errors,
          [...values.aggregationReflections, ...values.rawReflections].length,
        );
      return null;
    });
  };

  createSubmitErrorWrapper(reflectionSaveErrors, totalCount) {
    reflectionSaveErrors = Immutable.Map(reflectionSaveErrors).map((message) =>
      Immutable.fromJS({
        id: uuidv4(),
        message,
      }),
    );
    return Promise.reject({
      _error: {
        id: uuidv4(),
        message: Immutable.fromJS({
          // no #message needed, code used
          code: "COMBINED_REFLECTION_SAVE_ERROR",
          details: {
            reflectionSaveErrors,
            totalCount,
          },
        }),
      },
    });
  }

  getMustBeInAdvancedMode(props) {
    const { aggregationReflections, rawReflections } = props
      ? props.values
      : this.props.values;

    const { rawRecommendation } = this.props;

    if (aggregationReflections.length > 1 || rawReflections.length > 1) {
      return true;
    }

    // out of sync is not in form data, so we check independently
    if (
      this.props.reflections.some(
        (reflection) => reflection.get("status").get("config") === "INVALID",
      )
    ) {
      return true;
    }

    return [...aggregationReflections, ...rawReflections].some(
      (reflection) =>
        !areReflectionFormValuesBasic(
          reflection,
          this.props.dataset.toJS(),
          rawRecommendation,
        ),
    );
  }

  // used to update the dirty state properly for AccelerationAdvanced instead of updateFormDirtyState
  updateDirtyState = (isDirty) => {
    this.setState({
      formIsDirty: isDirty,
    });
  };

  renderAccelerationMode() {
    const {
      fields,
      location,
      values,
      updateFormDirtyState,
      dataset,
      reflections,
      canAlter,
      isTriggerRecommendation,
    } = this.props;
    const {
      mode,
      waitingForAggRecommendations,
      waitingForRawRecommendations,
      waitingForRecommendations,
    } = this.state;

    if (mode === "BASIC") {
      return (
        <AccelerationBasic
          canAlter={canAlter}
          dataset={dataset}
          reflections={reflections}
          location={location}
          fields={fields}
          skipRecommendations={this.skipRecommendations}
          fetchRecommendations={
            isTriggerRecommendation
              ? this.fetchRecommendations
              : this.fetchRecommendationsOld
          }
          formIsDirty={this.state.formIsDirty}
          isTriggerRecommendation={isTriggerRecommendation}
          {...(isTriggerRecommendation
            ? {
                loadingAggRecommendations: waitingForAggRecommendations,
                loadingRawRecommendations: waitingForRawRecommendations,
              }
            : { loadingRecommendations: waitingForRecommendations })}
        />
      );
    } else if (mode === "ADVANCED") {
      return (
        <AccelerationAdvanced
          canAlter={canAlter}
          dataset={dataset}
          reflections={reflections}
          fields={fields}
          values={values}
          loadingRecommendations={
            isTriggerRecommendation
              ? waitingForAggRecommendations
              : waitingForRecommendations
          }
          updateFormDirtyState={updateFormDirtyState}
          updateDirtyState={this.updateDirtyState}
          initialValues={this.initialValues}
        />
      );
    }
  }

  renderFormErrorMessage() {
    const { errors, submitFailed } = this.props;
    const listOfErrors = getListErrorsFromNestedReduxFormErrorEntity(errors);
    if (listOfErrors.length > 0 && submitFailed) {
      return (
        <div>
          {listOfErrors.map((errorMessage, i) => (
            <Message
              key={errorMessage}
              messageType="error"
              message={errorMessage}
              messageId={"" + i}
            />
          ))}
        </div>
      );
    }
  }

  renderExtraErrorMessages() {
    const messages = [];

    const { layoutId } = this.props.location.state || {};
    if (layoutId) {
      const found = this.props.reflections.get(layoutId);

      if (!found) {
        messages.push(
          <Message
            key="not-found"
            messageType="warning"
            message={Immutable.Map({ code: "REQUESTED_REFLECTION_MISSING" })}
            isDismissable={false}
            className={"AccelerationForm__extraError"}
          />,
        );
      }
    }

    if (
      this.props.reflections.some(
        (reflection) => reflection.get("status").get("config") === "INVALID",
      )
    ) {
      messages.push(
        <Message
          key="version-mismatch"
          messageType="warning"
          message={Immutable.Map({
            code: "COMBINED_REFLECTION_CONFIG_INVALID",
          })}
          isDismissable={false}
          className={"AccelerationForm__extraError"}
        />,
      );
    }

    return messages;
  }

  resetForm = () => {
    this.props.destroyForm();
    this.initializeForm();
  };

  render() {
    const { handleSubmit, onCancel, canSubmit, isModal = true } = this.props;
    const { formIsDirty } = this.state;
    const modalFormStyle = isModal ? {} : modalStyles.noModalForm;
    const confirmStyle = isModal ? {} : modalStyles.noModalConfirmCancel;
    const cancelText = isModal
      ? laDeprecated("Cancel")
      : laDeprecated("Revert");
    const onCancelClick = isModal ? onCancel : this.resetForm;
    const updatedCanSubmit = (canSubmit && isModal) || formIsDirty;
    const canCancel = isModal || formIsDirty;
    return (
      <div className={"AccelerationForm"}>
        <ModalForm
          isModal={isModal}
          {...modalFormProps(this.props)}
          style={modalFormStyle}
          confirmStyle={confirmStyle}
          cancelText={cancelText}
          onSubmit={handleSubmit(this.submitForm)}
          onCancel={onCancelClick}
          canSubmit={updatedCanSubmit}
          canCancel={canCancel}
        >
          {this.renderFormErrorMessage()}
          {this.renderExtraErrorMessages()}
          <FormBody
            className={"AccelerationForm__formBody"}
            dataQa="acceleration-form"
          >
            {this.renderHeader()}
            {this.renderAccelerationMode()}
          </FormBody>
        </ModalForm>
      </div>
    );
  }
}

const modalStyles = {
  noModalForm: {
    width: "100%",
    height: "100%",
    flexWrap: "nowrap",
  },
  noModalConfirmCancel: {
    margin: "10px 11px 30px 0",
  },
};

function mapStateToProps(state) {
  return {
    allowPartitionTransform:
      getSupportFlags(state)[ALLOW_REFLECTION_PARTITION_TRANFORMS],
    location: state.routing.locationBeforeTransitions,
    rawRecommendation: getRawRecommendation(state),
    isTriggerRecommendation:
      getSupportFlags(state)[MANUALLY_GENERATE_RECOMMENDATION],
  };
}

export default connectComplexForm(
  {
    form: "accelerationForm",
    skipDirtyFields: ["columnsDimensions", "columnsMeasures"],
  },
  SECTIONS,
  mapStateToProps,
  {
    putReflection: reflectionActions.put.dispatch,
    postReflection: reflectionActions.post.dispatch,
    deleteReflection: reflectionActions.delete.dispatch,
    fetchSupportFlags,
    setReflectionRecommendations,
  },
)(AccelerationFormWithMixin(AccelerationForm));
