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
//@ts-nocheck
import { type FC, useState } from "react";
import { getTheme, setTheme, Themes } from "../../appTheme/appTheme";
import clsx from "clsx";
import { SonarPreview } from "./SonarPreview";
import { getIntlContext } from "../../contexts/IntlContext";

const ThemePreviewCard: FC<{
  className?: string;
  selected?: boolean;
  label: string;
  onClick: () => void;
  theme: Themes;
}> = (props) => {
  const { label, selected, className, theme, ...rest } = props;
  return (
    <button
      className={clsx(
        className,
        "p-0 bg-primary border-thin flex flex-col overflow-hidden no-select",
      )}
      style={{
        appearance: "none",
        borderRadius: "4px",
        ...(selected && {
          "--border--color": "var(--color--brand--300)",
          borderColor: "var(--color--brand--300)",
        }),
      }}
      {...rest}
    >
      <div className="p-105 bg-disabled">
        <div className={theme}>
          <SonarPreview />
        </div>
      </div>
      <div className="p-105 flex flex-row justify-between items-center flex-1 shrink-0 w-full">
        {label}
        {!!selected && (
          <span
            className="text-sm px-05"
            style={{
              borderRadius: "4px",
              lineHeight: "1.5",
              background: "var(--fill--primary--selected)",
            }}
          >
            Active
          </span>
        )}
      </div>
    </button>
  );
};

const PreviewTag = () => (
  <span
    className="dremio-tag text-normal bg-secondary"
    style={{ fontSize: "0.75em", marginLeft: "2px" }}
  >
    Preview
  </span>
);

export const AppearancePicker = () => {
  const [theme, setThemeLocal] = useState(() => getTheme());
  const handleChange = (theme: Themes) => () => {
    setTheme(theme);
    setThemeLocal(theme);
  };
  const { t } = getIntlContext();
  return (
    <>
      <div className="preferences-settings-button-section">
        <div className="preferences-settings-button-name">
          {t("AccountSettings.AppearancePicker.Title")}
        </div>
      </div>
      <div className="preferences-settings-description">
        <span>{t("AccountSettings.AppearancePicker.Description")}</span>
      </div>

      <div className="mt-2">
        <div
          className="dremio-layout-grid"
          style={{ "--min-size": "200px", "--gap-size": "var(--scale-3)" }}
        >
          <ThemePreviewCard
            label={t("AccountSettings.AppearancePicker.ThemeTitle.LIGHT")}
            selected={theme === Themes.LIGHT}
            onClick={handleChange(Themes.LIGHT)}
            theme={Themes.LIGHT}
          />
          <ThemePreviewCard
            label={
              <div>
                {t("AccountSettings.AppearancePicker.ThemeTitle.DARK")}{" "}
                <PreviewTag />
              </div>
            }
            selected={theme === Themes.DARK}
            onClick={handleChange(Themes.DARK)}
            theme={Themes.DARK}
          />
          <ThemePreviewCard
            label={
              <div>
                {t(
                  "AccountSettings.AppearancePicker.ThemeTitle.HIGH_CONTRAST_LIGHT",
                )}{" "}
                <PreviewTag />
              </div>
            }
            selected={theme === Themes.HIGH_CONTRAST_LIGHT}
            onClick={handleChange(Themes.HIGH_CONTRAST_LIGHT)}
            theme={Themes.HIGH_CONTRAST_LIGHT}
          />
          <ThemePreviewCard
            label={
              <div>
                {t("AccountSettings.AppearancePicker.ThemeTitle.AUTO_SYSTEM")}{" "}
                <PreviewTag />
              </div>
            }
            selected={theme === Themes.SYSTEM_AUTO}
            onClick={handleChange(Themes.SYSTEM_AUTO)}
            theme={Themes.SYSTEM_AUTO}
          />
        </div>
      </div>
    </>
  );
};
