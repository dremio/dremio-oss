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
import { useSelector } from "react-redux";
import { v4 as uuidv4 } from "uuid";
import { merge } from "lodash";
import { intl } from "#oss/utils/intl";
import {
  NESSIE,
  NAS,
  HDFS,
  HIVE,
  HIVE3,
  RESTCATALOG,
  GCS,
  AZURE_STORAGE,
  S3,
  AWSGLUE,
  SNOWFLAKEOPENCATALOG,
  UNITY,
} from "#oss/constants/sourceTypes";

export function sourceTypesIncludeS3(sourceTypes: { sourceType: string }[]) {
  return sourceTypes && !!sourceTypes.find((type) => type.sourceType === "S3");
}

export function sourceTypesIncludeSampleSource(
  sourceTypes: { sourceType: string }[],
) {
  return (
    sourceTypes &&
    !!sourceTypes.find((type) => type.sourceType === "SAMPLE_SOURCE")
  );
}

export const isLimitedVersionSource = (type: string) => {
  return false;
};

export function isVersionedSource(type: string) {
  switch (type) {
    case NESSIE:
      return true;
    default:
      return false;
  }
}

export function isNessieSource(type?: string) {
  return type === NESSIE;
}

export function isArcticSource(type?: string) {
  return false;
}

export const isIcebergSource = (sourceType: string) => {
  return (
    NAS === sourceType ||
    HDFS === sourceType ||
    NESSIE === sourceType ||
    HIVE === sourceType ||
    HIVE3 === sourceType ||
    RESTCATALOG === sourceType ||
    GCS === sourceType ||
    UNITY === sourceType ||
    SNOWFLAKEOPENCATALOG === sourceType ||
    AZURE_STORAGE === sourceType ||
    S3 === sourceType ||
    AWSGLUE === sourceType
  );
};

export const getSourceIcon = (sourceType: string) => {
  if (NESSIE === sourceType) {
    return "entities/nessie-source";
  } else {
    return "entities/datalake-source";
  }
};

export const showSourceIcon = (sourceType: string) => {
  return true;
};

export const getAddSourceModalTitle = (sourceType: string) => {
  return undefined;
};

export const getEditSourceModalTitle = (sourceType: string, name: string) => {
  return intl.formatMessage({ id: "Source.EditSource" });
};

export const isURISupportedSource = (sourceType: string) => false;

export const useSourceTypeFromState = (sourceName: string) => {
  const source = useSelector((state: Record<string, any>) => {
    return getSourceFromState(
      state.resources.entities.get("source"),
      sourceName,
    );
  });
  if (source && source.get("type")) {
    return source.get("type");
  }
  return null;
};

export const getSourceFromState = (
  sources: Record<string, any>,
  sourceName: string,
) =>
  sources.find(
    (source: Record<string, any>) => source.get("name") === sourceName,
  );

/**
 * Client-side presets: extra "Add Source" tiles that open the form of an existing
 * backend source type with pre-filled values. The saved source keeps the base type
 * (e.g. RESTCATALOG), there is no preset-specific backend type.
 */
export const POLARIS_PRESET_ID = "POLARIS";

type PresetProperty = { name: string; value: string };

export type SourcePreset = {
  id: string;
  label: string;
  sourceType: string;
  propertyList?: PresetProperty[];
  secretPropertyList?: PresetProperty[];
  config?: Record<string, unknown>;
};

export const SOURCE_PRESETS: SourcePreset[] = [
  {
    id: POLARIS_PRESET_ID,
    label: "Apache Polaris OSS",
    sourceType: RESTCATALOG,
    // warehouse and credential are left empty on purpose: they are required by
    // Polaris and the form blocks submit until they are filled in.
    propertyList: [
      { name: "warehouse", value: "" },
      { name: "scope", value: "PRINCIPAL_ROLE:ALL" },
    ],
    secretPropertyList: [{ name: "credential", value: "" }],
    config: { isUsingVendedCredentials: false },
  },
];

export const getSourcePreset = (presetId?: string | null) =>
  presetId
    ? SOURCE_PRESETS.find((preset) => preset.id === presetId)
    : undefined;

type SourceTypeTile = {
  sourceType: string;
  label?: string;
  disabled?: boolean;
  presetId?: string;
  [key: string]: unknown;
};

/**
 * Appends a tile for every preset whose base source type is available (and enabled)
 * and that is not in the list yet.
 */
export function addSourcePresetTiles<T extends SourceTypeTile>(
  sourceTypes: T[],
): T[] {
  if (!sourceTypes) return sourceTypes;
  const presetTiles = SOURCE_PRESETS.reduce<T[]>((tiles, preset) => {
    if (sourceTypes.some((type) => type.presetId === preset.id)) {
      return tiles;
    }
    const baseType = sourceTypes.find(
      (type) =>
        type.sourceType === preset.sourceType &&
        !type.presetId &&
        !type.disabled,
    );
    if (baseType) {
      tiles.push({ ...baseType, label: preset.label, presetId: preset.id });
    }
    return tiles;
  }, []);
  return presetTiles.length ? [...sourceTypes, ...presetTiles] : sourceTypes;
}

const toPropertyRows = (properties?: PresetProperty[]) =>
  (properties || []).map((property) => ({ id: uuidv4(), ...property }));

/**
 * Initial redux-form values for a preset. Call once per selection: every call
 * generates new row ids.
 */
export function getSourcePresetInitialValues(presetId?: string | null) {
  const preset = getSourcePreset(presetId);
  if (!preset) return undefined;
  const config: Record<string, unknown> = { ...preset.config };
  if (preset.propertyList) {
    config.propertyList = toPropertyRows(preset.propertyList);
  }
  if (preset.secretPropertyList) {
    config.secretPropertyList = toPropertyRows(preset.secretPropertyList);
  }
  return { config };
}

const trimmed = (value: unknown) =>
  value === null || value === undefined ? "" : String(value).trim();

/**
 * Keys that hold secrets and must not be stored in the unmasked propertyList.
 * Compared case-insensitively. Together with the patterns in
 * {@link isSensitivePropertyKey} this mirrors
 * RestIcebergCatalogPlugin.isSensitivePropertyKey on the backend; keep both in sync.
 */
export const SENSITIVE_PROPERTY_KEYS = [
  "credential",
  "token",
  "header.authorization",
  "fs.s3a.access.key",
  "fs.s3a.secret.key",
  "fs.s3a.session.token",
  "s3.access-key-id",
  "s3.secret-access-key",
  "s3.session-token",
];

const SENSITIVE_KEY_SUBSTRINGS = [
  "secret",
  "password",
  "account.key",
  "private.key",
  "private-key",
  "sas-token",
];

const SENSITIVE_KEY_SUFFIXES = [".token", "-token", "_token"];

const normalizedKey = (name?: unknown) => trimmed(name).toLowerCase();

export const isSensitivePropertyKey = (name?: unknown) => {
  const key = normalizedKey(name);
  if (!key) return false;
  return (
    SENSITIVE_PROPERTY_KEYS.includes(key) ||
    SENSITIVE_KEY_SUBSTRINGS.some((part) => key.includes(part)) ||
    SENSITIVE_KEY_SUFFIXES.some((suffix) => key.endsWith(suffix))
  );
};

type PropertyRow = { name?: unknown; value?: unknown } | null;

const validatePropertyRows = (
  rows: PropertyRow[] | undefined,
  checkSensitive: boolean,
  allowedSensitiveKeys: Set<string> = new Set(),
) => {
  let hasErrors = false;
  const rowErrors = (rows || []).map((row) => {
    const errors: { name?: string; value?: string } = {};
    const name = trimmed(row?.name);
    if (!name) {
      errors.name = "Name is required.";
    } else if (
      checkSensitive &&
      isSensitivePropertyKey(name) &&
      !allowedSensitiveKeys.has(normalizedKey(name))
    ) {
      errors.name = `"${name}" is a secret. Move it to Catalog Credentials so that its value is masked.`;
    }
    if (!trimmed(row?.value)) {
      errors.value = "Value is required.";
    }
    if (errors.name || errors.value) {
      hasErrors = true;
    }
    return errors;
  });
  return hasErrors ? rowErrors : undefined;
};

export type SourcePropertyListValidationOptions = {
  /**
   * propertyList of the saved source (edit flow). Sensitive keys already stored there
   * are not blocked, so that sources created before this check (e.g. through the API)
   * can still be edited; only new or renamed rows are rejected. The backend logs a
   * warning for such keys.
   */
  initialPropertyList?: PropertyRow[];
};

const sensitiveKeysOf = (rows?: PropertyRow[]) =>
  new Set(
    (rows || [])
      .map((row) => normalizedKey(row?.name))
      .filter((key) => key && isSensitivePropertyKey(key)),
  );

/**
 * Extra form validation for Iceberg REST catalog sources: property rows need a name and
 * a value, and secret keys are rejected in the unmasked propertyList (except keys the
 * saved source already has, see {@link SourcePropertyListValidationOptions}).
 * Returns {} for other source types.
 */
export function validateSourcePropertyLists(
  sourceType: string | undefined,
  values: { config?: Record<string, any> } | undefined,
  options: SourcePropertyListValidationOptions = {},
) {
  if (sourceType !== RESTCATALOG || !values?.config) return {};
  const propertyList = validatePropertyRows(
    values.config.propertyList,
    true,
    sensitiveKeysOf(options.initialPropertyList),
  );
  const secretPropertyList = validatePropertyRows(
    values.config.secretPropertyList,
    false,
  );
  if (!propertyList && !secretPropertyList) return {};
  return {
    config: {
      ...(propertyList && { propertyList }),
      ...(secretPropertyList && { secretPropertyList }),
    },
  };
}

type FormValidator = (values: any) => Record<string, any>;

/**
 * Wraps a form validator (e.g. FormUtils.getValidationsFromConfig) with
 * {@link validateSourcePropertyLists}.
 */
export function withSourcePropertyListValidation(
  validate: FormValidator | undefined,
  sourceType: string | undefined,
  options: SourcePropertyListValidationOptions = {},
): FormValidator {
  return (values) =>
    merge(
      {},
      validate ? validate(values) : {},
      validateSourcePropertyLists(sourceType, values, options),
    );
}

/**
 * Extracts errorMessage from a failed `source/type/{type}` request. ApiUtils.fetchJson
 * also passes non-Response errors (e.g. a TypeError thrown while building the form) to
 * the error handler, so json() must not be assumed.
 */
export async function getSourceTypeLoadErrorMessage(
  error: unknown,
): Promise<string | undefined> {
  const response = error as { json?: () => Promise<any> } | null | undefined;
  if (typeof response?.json !== "function") {
    return undefined;
  }
  try {
    const body = await response.json();
    return body?.errorMessage || undefined;
  } catch {
    return undefined;
  }
}
