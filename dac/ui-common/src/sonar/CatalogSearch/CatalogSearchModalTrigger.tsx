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

import { FC } from "react";
import { getIntlContext } from "../../contexts/IntlContext";
import { getModifierKey } from "../../utilities/getModifierKey";

export const CatalogSearchModalTrigger: FC<{ onOpen: () => void }> = (
  props,
) => {
  const { t } = getIntlContext();
  const modifierKey = getModifierKey();
  return (
    <button
      className="form-control"
      onClick={props.onOpen}
      aria-label="Catalog search"
      type="button"
    >
      <div
        className="dremio-icon-label"
        style={{ color: "var(--text--disabled)" }}
      >
        <dremio-icon
          name="interface/search"
          alt=""
          class="icon-primary"
          style={{ width: "20px", height: "20ox" }}
        ></dremio-icon>
        {t("Catalog.Search.Modal.Trigger.Placeholder")}
      </div>

      <div
        className="ml-auto dremio-tag bg-secondary border"
        style={{
          border: "1px solid var(--border--neutral)",
        }}
      >
        {t(`Common.ModifierKey.${modifierKey}.Combo`, { key: "K" })}
      </div>
    </button>
  );
};
