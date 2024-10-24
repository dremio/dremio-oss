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
import FormUtils from "utils/FormUtils/FormUtils";
import { inlineHelp, elementContainer } from "uiTheme/less/forms.less";
import { useContext } from "react";
import { FormContext } from "#oss/pages/HomePage/components/modals/formContext";
import { ElementConfig } from "#oss/types/Sources/SourceFormTypes";

type FormElementProps = {
  style: Record<string, any>;
  elementConfig: ElementConfig;
  fields: Record<string, any>[];
  disabled?: boolean;
};

export default function FormElement({
  style,
  elementConfig,
  fields,
  disabled,
}: FormElementProps) {
  const { editing, sourceType } = useContext(FormContext);

  const help = elementConfig.getConfig().help;
  const propName = elementConfig.getConfig().propName;

  function renderElement(elementConfig: ElementConfig, fields: any) {
    if (!elementConfig.getRenderer) return;

    const field = FormUtils.getFieldByComplexPropName(
      fields,
      elementConfig.getPropName(),
    );
    const Renderer = elementConfig.getRenderer({ sourceType, propName });

    return (
      <Renderer
        elementConfig={elementConfig}
        fields={fields}
        field={field}
        editing={editing}
        disabled={disabled}
      />
    );
  }

  return (
    <div className={elementContainer} style={style} data-qa={propName}>
      {help && help.position === "top" && (
        <div className={inlineHelp}>{help.text}</div>
      )}
      {renderElement(elementConfig, fields)}
      {help && help.position !== "top" && (
        <div className={inlineHelp}>{help.text}</div>
      )}
    </div>
  );
}
