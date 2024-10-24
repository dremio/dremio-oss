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

import { FC, useCallback, useMemo, useState } from "react";
import { FilterTag } from "./FilterTag";
import { getIntlContext } from "../../contexts/IntlContext";

export const FILTER_ALL = "ALL";

export enum CatalogFilterType {
  TABLE = "TABLE",
  VIEW = "VIEW",
  FOLDER = "FOLDER",
  UDF = "UDF",
  JOB = "JOB",
  COMMIT = "COMMIT",
  SPACE = "SPACE",
  REFLECTION = "REFLECTION",
  SCRIPT = "SCRIPT",
  SOURCE = "SOURCE",
}

export type CatalogFilterTypeCounts = Map<CatalogFilterType, number>;

export const useCatalogTypeFilterState = () => {
  const [filterTypeState, setFilterTypeState] = useState<
    Set<CatalogFilterType>
  >(new Set());

  const resetFilters = useCallback(() => {
    setFilterTypeState(new Set());
  }, []);

  const addFilter = useCallback((type: CatalogFilterType) => {
    setFilterTypeState((prev) => {
      if (prev.has(type)) {
        return prev;
      }
      const next = new Set<CatalogFilterType>();
      next.add(type);
      return next;
    });
  }, []);

  const removeFilter = useCallback((type: CatalogFilterType) => {
    setFilterTypeState((prev) => {
      if (!prev.has(type)) {
        return prev;
      }
      const next = new Set<CatalogFilterType>();
      next.delete(type);
      return next;
    });
  }, []);

  const toggleFilter = useCallback((type: CatalogFilterType) => {
    setFilterTypeState((prev) => {
      const next = new Set<CatalogFilterType>();
      if (prev.has(type)) {
        next.delete(type);
      } else {
        next.add(type);
      }
      return next;
    });
  }, []);

  return {
    typeFilter: filterTypeState,
    addFilter,
    removeFilter,
    toggleFilter,
    resetFilters,
  } as const;
};

export const CatalogTypeFilters: FC<
  ReturnType<typeof useCatalogTypeFilterState> & {
    typeCounts: CatalogFilterTypeCounts | null;
  }
> = (props) => {
  const handleClick = (type: CatalogFilterType | typeof FILTER_ALL) => () => {
    if (type === "ALL") {
      props.resetFilters();
      return;
    }
    props.toggleFilter(type);
  };
  const allCount = useMemo(() => {
    if (!props.typeCounts) {
      return undefined;
    }
    return Array.from(props.typeCounts.values()).reduce(
      (total, curr) => total + curr,
      0,
    );
  }, [props.typeCounts]);
  const { t } = getIntlContext();
  return (
    <ul
      className="flex flex-row gap-1 pb-05 overflow-hidden no-select flex-wrap"
      aria-label="Object type filter"
      style={{ overflowY: "hidden" }}
    >
      {props.typeCounts ? (
        <li>
          <FilterTag
            aria-pressed={props.typeFilter.size === 0}
            count={allCount}
            onClick={handleClick(FILTER_ALL)}
          >
            All
          </FilterTag>
        </li>
      ) : null}

      <li>
        <FilterTag
          aria-pressed={props.typeFilter.has(CatalogFilterType.TABLE)}
          count={props.typeCounts?.get(CatalogFilterType.TABLE)}
          onClick={handleClick(CatalogFilterType.TABLE)}
        >
          <dremio-icon name="entities/dataset-table" alt=""></dremio-icon>{" "}
          {t("Catalog.Reference.Type.DATASET_DIRECT.Label")}
        </FilterTag>
      </li>
      <li>
        <FilterTag
          aria-pressed={props.typeFilter.has(CatalogFilterType.VIEW)}
          count={props.typeCounts?.get(CatalogFilterType.VIEW)}
          onClick={handleClick(CatalogFilterType.VIEW)}
        >
          <dremio-icon name="entities/dataset-view" alt=""></dremio-icon>{" "}
          {t("Catalog.Reference.Type.DATASET_VIRTUAL.Label")}
        </FilterTag>
      </li>
      <li>
        <FilterTag
          aria-pressed={props.typeFilter.has(CatalogFilterType.FOLDER)}
          count={props.typeCounts?.get(CatalogFilterType.FOLDER)}
          onClick={handleClick(CatalogFilterType.FOLDER)}
        >
          <dremio-icon name="entities/blue-folder" alt=""></dremio-icon>{" "}
          {t("Catalog.Reference.Type.FOLDER.Label")}
        </FilterTag>
      </li>
      <li>
        <FilterTag
          aria-pressed={props.typeFilter.has(CatalogFilterType.UDF)}
          count={props.typeCounts?.get(CatalogFilterType.UDF)}
          onClick={handleClick(CatalogFilterType.UDF)}
        >
          <dremio-icon name="sql-editor/function" alt=""></dremio-icon> UDF
        </FilterTag>
      </li>
      {/* <li>
        <FilterTag
          aria-pressed={props.typeFilter.has(CatalogFilterType.JOB)}
          count={props.typeCounts?.get(CatalogFilterType.JOB)}
          onClick={handleClick(CatalogFilterType.JOB)}
        >
          <dremio-icon name="interface/job-overview" alt=""></dremio-icon> Job
        </FilterTag>
      </li> */}
      {/* <li>
        <FilterTag
          aria-pressed={props.typeFilter.has(CatalogFilterType.COMMIT)}
          count={props.typeCounts?.get(CatalogFilterType.COMMIT)}
          onClick={handleClick(CatalogFilterType.COMMIT)}
        >
          <dremio-icon name="vcs/commit" alt=""></dremio-icon> Commit
        </FilterTag>
      </li> */}
      <li>
        <FilterTag
          aria-pressed={props.typeFilter.has(CatalogFilterType.SPACE)}
          count={props.typeCounts?.get(CatalogFilterType.SPACE)}
          onClick={handleClick(CatalogFilterType.SPACE)}
        >
          <dremio-icon name="entities/space" alt=""></dremio-icon>{" "}
          {t("Catalog.Reference.Type.SPACE.Label")}
        </FilterTag>
      </li>
      {/* <li>
        <FilterTag
          aria-pressed={props.typeFilter.has(CatalogFilterType.REFLECTION)}
          count={props.typeCounts?.get(CatalogFilterType.REFLECTION)}
          onClick={handleClick(CatalogFilterType.REFLECTION)}
        >
          <dremio-icon name="interface/reflections" alt=""></dremio-icon>{" "}
          Reflection
        </FilterTag>
      </li> */}
      {/* <li>
        <FilterTag
          aria-pressed={props.typeFilter.has(CatalogFilterType.SCRIPT)}
          count={props.typeCounts?.get(CatalogFilterType.SCRIPT)}
          onClick={handleClick(CatalogFilterType.SCRIPT)}
        >
          <dremio-icon name="entities/script" alt=""></dremio-icon> Script
        </FilterTag>
      </li> */}
      <li>
        <FilterTag
          aria-pressed={props.typeFilter.has(CatalogFilterType.SOURCE)}
          count={props.typeCounts?.get(CatalogFilterType.SOURCE)}
          onClick={handleClick(CatalogFilterType.SOURCE)}
        >
          <dremio-icon name="sources/SAMPLEDB" alt=""></dremio-icon>{" "}
          {t("Catalog.Reference.Type.SOURCE.Label")}
        </FilterTag>
      </li>
    </ul>
  );
};
