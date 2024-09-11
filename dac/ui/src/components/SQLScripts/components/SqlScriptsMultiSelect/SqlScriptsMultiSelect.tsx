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

import { useEffect, useMemo, useRef, useState } from "react";
import {
  Button,
  Tooltip,
  type TooltipPlacement,
} from "dremio-ui-lib/components";
import { getIntlContext } from "dremio-ui-common/contexts/IntlContext.js";

export const useSqlScriptsMultiSelect = (scripts: Record<string, any>[]) => {
  const [lastClickedId, setLastClickedId] = useState(null);
  const [selectedIds, setSelectedIds] = useState(() => new Set());
  const selectScript = (id: string) => {
    setSelectedIds((ids) => new Set(ids).add(id));
  };
  const deselectScript = (id: string) => {
    setSelectedIds((ids) => {
      const state = new Set(ids);
      state.delete(id);
      return state;
    });
  };

  const onCheckboxClick = (script: any) => (e: any) => {
    if (
      e.shiftKey &&
      !!lastClickedId &&
      !!scripts.find(({ id }) => id === lastClickedId)
    ) {
      const lastClickedIdx = scripts.findIndex(
        ({ id }) => id === lastClickedId,
      );
      const clickedIdx = scripts.findIndex(({ id }) => id === script.id);

      let start = lastClickedIdx;
      let end = clickedIdx + 1;
      if (lastClickedIdx > clickedIdx) {
        start = clickedIdx;
        end = lastClickedIdx + 1;
      }

      if (!selectedIds.has(script.id)) {
        // Select range
        setSelectedIds(
          (ids) =>
            new Set([...ids, ...scripts.slice(start, end).map(({ id }) => id)]),
        );
      } else {
        //Deselect range
        const deselectIds = scripts.slice(start, end).map(({ id }) => id);
        setSelectedIds(
          (ids) => new Set([...ids].filter((id) => !deselectIds.includes(id))),
        );
      }
    } else {
      if (!selectedIds.has(script.id)) {
        selectScript(script.id);
      } else {
        deselectScript(script.id);
      }
    }

    setLastClickedId(script.id);
  };

  const selectedScripts = scripts.filter((script) =>
    selectedIds.has(script.id),
  );

  // Select/Deselect all
  const onAllClick = (e: any) => {
    if (selectedScripts.length === scripts.length) {
      setSelectedIds(new Set());
    } else {
      setSelectedIds(new Set(scripts.map(({ id }) => id)));
    }
  };

  return {
    onCheckboxClick,
    onAllClick,
    selectedScripts,
  } as const;
};

export const ScriptCheckbox = ({
  onClick,
  checked,
  disabled,
  tooltipPlacement = "top-end",
}: {
  onClick: React.MouseEventHandler<HTMLInputElement>;
  checked: boolean;
  disabled?: boolean;
  tooltipPlacement?: TooltipPlacement;
}) => {
  const { t } = getIntlContext();
  const Input = (
    <input
      disabled={disabled}
      checked={checked}
      onClick={onClick}
      className="form-control"
      type="checkbox"
    />
  );

  return (
    // eslint-disable-next-line jsx-a11y/no-noninteractive-element-interactions
    <label
      className="cursor-pointer mr-1"
      onClick={(e) => {
        e.stopPropagation();
      }}
      onKeyDown={(e) => {
        e.stopPropagation();
      }}
    >
      {disabled ? (
        <Tooltip
          portal
          placement={tooltipPlacement}
          content={
            <div className="dremio-tooltip--sm">
              {t("Sonar.Scripts.BulkDelete.NoPrivilege")}
            </div>
          }
        >
          <div className="flex">{Input}</div>
        </Tooltip>
      ) : (
        Input
      )}
    </label>
  );
};

export const ScriptSelectAllCheckbox = ({
  scripts,
  selectedScripts,
  onCheckboxClick,
  onDeleteClick,
}: {
  scripts: Record<string, any>;
  selectedScripts: Record<string, any>;
  onCheckboxClick: (e: any) => void;
  onDeleteClick: (e: any) => void;
}) => {
  const { t } = getIntlContext();
  const { checked, indeterminate } = useMemo(() => {
    const leftover = scripts.reduce((acc, cur) => {
      acc[cur.id] = true;
      return acc;
    }, {});

    selectedScripts.forEach((cur) => {
      if (leftover[cur.id]) delete leftover[cur.id];
    });

    const leftoverKeys = Object.keys(leftover);

    return {
      checked: leftoverKeys.length === 0,
      indeterminate: leftoverKeys.length > 0,
    };
  }, [scripts, selectedScripts]);

  const checkboxRef = useRef<HTMLInputElement>(null);
  useEffect(() => {
    if (checkboxRef.current) {
      checkboxRef.current.indeterminate = indeterminate;
    }
  }, [indeterminate]);

  return (
    <div
      style={{
        bottom: 0,
        height: 64,
        zIndex: 1, // Display box shadow on top
        borderTop: "1px solid var(--border--neutral)",
        boxShadow: "var(--dremio--shadow--top)",
      }}
      className="flex shrink-0 items-center px-1"
    >
      <label className="cursor-pointer flex items-center flex-1 full-height">
        <input
          ref={checkboxRef}
          checked={checked}
          onClick={onCheckboxClick}
          className="form-control"
          type="checkbox"
          style={{
            marginInlineEnd: "var(--scale-1)",
          }}
        />
        <span>
          {t("Sonar.Scripts.NumSelected", {
            numSelected: selectedScripts.length,
          })}
        </span>
      </label>
      <Button variant="tertiary-danger" onClick={onDeleteClick}>
        {t("Common.Actions.Delete")}
      </Button>
    </div>
  );
};
