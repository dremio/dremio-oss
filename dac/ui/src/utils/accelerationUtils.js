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
import deepEqual from "deep-equal";
import { v4 as uuidv4 } from "uuid";
import Immutable from "immutable";
import { formatMessage } from "utils/locale";
import { getUniqueName } from "utils/pathUtils";
import {
  allMeasureTypes,
  cellTypesWithNoSum,
} from "@app/constants/AccelerationConstants";
import { ANY } from "@app/constants/DataTypes";

export const createReflectionFormValues = (opts, siblingNames = []) => {
  const reflection = {
    id: uuidv4(), // need to supply a temp uuid so errors can be tracked
    tag: "",
    type: "",
    name: "",
    enabled: true,
    arrowCachingEnabled: false,
    distributionFields: [],
    partitionFields: [],
    sortFields: [],
    partitionDistributionStrategy: "CONSOLIDATED",
    shouldDelete: false,
  };

  if (opts.type === "RAW") {
    reflection.displayFields = [];
  } else {
    reflection.dimensionFields = [];
    reflection.measureFields = [];
  }

  if (!opts.name) {
    reflection.name =
      siblingNames &&
      getUniqueName(
        opts.type === "RAW"
          ? laDeprecated("Raw Reflection")
          : laDeprecated("Aggregation Reflection"),
        (proposedName) => {
          return !siblingNames.includes(proposedName);
        },
      );
  }

  // only copy values from opts if the key exists in our reduced reflection representation
  for (const key in reflection) {
    if (key in opts) {
      // partition fields with transformations have a different format than normal partitions
      if (key === "partitionFields") {
        reflection[key] = opts[key].map((partitionField) => {
          if (partitionField.transform) {
            return {
              name: {
                ...partitionField,
              },
            };
          } else {
            return partitionField;
          }
        });

        continue;
      }

      reflection[key] = opts[key];
    }
  }

  return reflection;
};

export const areReflectionFormValuesUnconfigured = (reflectionFormValues) => {
  const unconfiguredReflection = createReflectionFormValues({
    type: reflectionFormValues.type,

    // we consider the reflection id, tag, name, and arrowCachingEnabled inconsequential
    id: reflectionFormValues.id,
    tag: reflectionFormValues.tag,
    name: reflectionFormValues.name,
    enabled: reflectionFormValues.enabled,
  });

  return deepEqual(reflectionFormValues, unconfiguredReflection);
};

export const areReflectionFormValuesBasic = (
  reflectionFormValues,
  dataset,
  rawRecommendation,
) => {
  reflectionFormValues = { ...reflectionFormValues };

  const basicCopy = createReflectionFormValues({
    // these don't matter for basic v advanced mode:
    type: reflectionFormValues.type,
    id: reflectionFormValues.id,
    tag: reflectionFormValues.tag,
    name: reflectionFormValues.name,
    enabled: reflectionFormValues.enabled,
    arrowCachingEnabled: reflectionFormValues.arrowCachingEnabled,
  });

  if (reflectionFormValues.type === "RAW") {
    reflectionFormValues.displayFields.sort(fieldSorter);

    if (rawRecommendation) {
      basicCopy.displayFields =
        rawRecommendation.displayFields.sort(fieldSorter);
    } else {
      basicCopy.displayFields = dataset.fields
        .map(({ name }) => ({ name }))
        .sort(fieldSorter);
    }

    reflectionFormValues.partitionFields = basicCopy.partitionFields;
  } else {
    reflectionFormValues = {
      ...reflectionFormValues,
      // these don't matter for basic v advanced mode:
      dimensionFields: basicCopy.dimensionFields, // ignoring granularity right now as even advanced does nothing with it (sync system is careful to preserve)
      measureFields: basicCopy.measureFields,
      partitionFields: basicCopy.partitionFields,
    };
  }

  return deepEqual(reflectionFormValues, basicCopy);
};

export const fieldSorter = (a, b) => {
  if (a.name < b.name) {
    return -1;
  }
  if (a.name > b.name) {
    return 1;
  }
  return 0;
};

export const forceChangesForDatasetChange = (reflection, dataset) => {
  if (reflection.status?.config !== "INVALID") return { reflection };

  const lostFields = {};
  reflection = { ...reflection };
  const validFields = new Set(dataset.fields.map((f) => f.name));

  for (const feildList of "sortFields partitionFields distributionFields displayFields dimensionFields measureFields".split(
    " ",
  )) {
    if (!reflection[feildList]) continue;
    reflection[feildList] = reflection[feildList].filter((field) => {
      if (validFields.has(field.name)) return true;
      lostFields[feildList] = lostFields[feildList] || [];
      lostFields[feildList].push(field);
      return false;
    });
  }

  // future: handle change field to invalid type (as-is; left to validation system to force a fix)

  return { reflection, lostFields };
};

export const findAllMeasureTypes = (fieldType) => {
  if (!fieldType) return null;

  const allTypes = Object.values(allMeasureTypes); //['MIN', 'MAX', 'SUM', 'COUNT', 'APPROX_COUNT_DISTINCT'];

  if (cellTypesWithNoSum.includes(fieldType)) {
    //filter creates a copy vs. splice, which would mutate allTypes
    return allTypes.filter((v) => v !== allMeasureTypes.SUM);
  }

  return allTypes;
};

export const getDefaultMeasureTypes = (fieldType) => {
  const cellMeasureTypes = findAllMeasureTypes(fieldType);
  if (cellMeasureTypes.includes(allMeasureTypes.SUM)) {
    return [allMeasureTypes.SUM, allMeasureTypes.COUNT];
  }

  return [allMeasureTypes.COUNT];
};

// fixup any null or empty measureTypeLists
export const fixupReflection = (reflection, dataset) => {
  if (
    reflection.type === "AGGREGATION" &&
    reflection.measureFields &&
    reflection.measureFields.length > 0
  ) {
    for (const measureField of reflection.measureFields) {
      if (!measureField.measureTypeList) {
        measureField.measureTypeList = getDefaultMeasureTypes(
          getTypeForField(dataset, measureField.name),
        );
      }
    }
  }
};

export const getTypeForField = (dataset, fieldName) => {
  if (!dataset.has("fields")) {
    return ANY;
  }

  return dataset
    .get("fields")
    .find((elm) => {
      return elm.get("name") === fieldName;
    })
    .getIn(["type", "name"]);
};

export function getReflectionUiStatus(reflection) {
  if (!reflection) return;

  const status = reflection.get("status");

  let icon = "WarningSolid";
  let text = "";
  let iconId = "job-state/warning";
  let className = "";

  const statusMessage =
    status && status.get("availability") === "AVAILABLE"
      ? formatMessage("Reflection.StatusCanAccelerate")
      : formatMessage("Reflection.StatusCannotAccelerate");

  if (!reflection.get("enabled")) {
    icon = "Disabled";
    iconId = "job-state/cancel";
    text = formatMessage("Reflection.StatusDisabled");
  } else if (status.get("config") === "INVALID") {
    icon = "ErrorSolid";
    iconId = "job-state/failed";
    text = formatMessage("Reflection.StatusInvalidConfiguration", {
      status: statusMessage,
    });
  } else if (status.get("refresh") === "RUNNING") {
    icon = "Loader";
    iconId = "job-state/loading";
    className = "spinner";
    if (status.get("availability") === "AVAILABLE") {
      text = formatMessage("Reflection.StatusRefreshing", {
        status: statusMessage,
      });
    } else {
      text = formatMessage("Reflection.StatusBuilding", {
        status: statusMessage,
      });
    }
  } else if (status.get("failureCount") > 0) {
    if (status.get("refresh") === "GIVEN_UP") {
      // reflection exhausts retry policy
      icon = "ErrorSolid";
      iconId = "job-state/failed";
      text = formatMessage("Reflection.StatusFailedNoReattempt", {
        status: statusMessage,
        failCount: status.get("failureCount"),
      });
    } else {
      // reflection still reattempting according to retry policy
      icon = "WarningSolid";
      iconId = "job-state/warning";
      text = formatMessage("Reflection.StatusFailedNonFinal", {
        status: statusMessage,
        failCount: status.get("failureCount"),
      });
    }
  } else if (status.get("refresh") === "GIVEN_UP") {
    // Reflection manager is not running, which happens here:
    // oss/services/accelerator/src/main/java/com/dremio/service/reflection
    // /ReflectionStatusServiceImpl.java#L228
    icon = "ErrorSolid";
    iconId = "job-state/failed";
    text = formatMessage("Reflection.StatusReflectionManagerDown", {
      status: statusMessage,
    });
  } else if (status.get("availability") === "AVAILABLE") {
    icon = "OKSolid";
    iconId = "job-state/completed";
    if (status.get("refresh") === "MANUAL") {
      text = formatMessage("Reflection.StatusManual", {
        status: statusMessage,
      });
    } else {
      text = formatMessage("Reflection.StatusCanAccelerate");
    }
  } else if (status.get("availability") === "NONE") {
    if (
      status.get("refresh") === "SCHEDULED" ||
      status.get("refresh") === "PENDING" ||
      status.get("refresh") === "ON_DATA_CHANGES"
    ) {
      icon = "Ellipsis";
      iconId = "job-state/queued";
      text = formatMessage("Reflection.Scheduled", {
        status: statusMessage,
      });
    } else if (status.get("refresh") === "MANUAL") {
      icon = "ErrorSolid";
      iconId = "job-state/failed";
      text = formatMessage("Reflection.StatusManual", {
        status: statusMessage,
      });
    }
  }

  return Immutable.fromJS({
    icon,
    text,
    iconId,
    className,
  });
}
