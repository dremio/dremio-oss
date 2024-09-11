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

import { useCallback, useEffect, useState } from "react";
import {
  Button,
  DialogContent,
  ModalContainer,
  IconButton,
  Input,
  Tooltip,
  Select,
  SelectOption,
  useSelect,
  Spinner,
  Label,
} from "dremio-ui-lib/components";
import { Toggle } from "../Fields";
import { getIntlContext } from "dremio-ui-common/contexts/IntlContext.js";
import NavPanel from "@app/components/Nav/NavPanel";
import Immutable from "immutable";
import {
  Controller,
  ControllerRenderProps,
  FieldError,
  FieldValues,
  FormProvider,
  useForm,
  useFormContext,
} from "react-hook-form";
import clsx from "clsx";
import {
  FormComponent,
  FormTypeProps,
  InputTypeProps,
  ModalFormTabs,
  SelectTypeProps,
  TextAreaTypeProps,
} from "./ModalForm.types";
import { zodResolver } from "@hookform/resolvers/zod";
import LeaveModalForm from "./LeaveModalForm/LeaveModalForm";

import * as classes from "./ModalForm.module.less";

type ModalFormProps = {
  tabs: ModalFormTabs;
  isOpen: boolean;
  closeDialog: () => void;
  title: string;
  submit: (values: any) => void;
  defaultValues?: Record<string, any>;
  validationSchema?: any;
  isSubmitting?: boolean;
  isLoading?: boolean;
  isCustomTabDirty?: boolean; // used if a custom tab is dirty
};

const ModalForm = ({
  isOpen,
  closeDialog,
  tabs,
  title,
  defaultValues,
  submit,
  validationSchema,
  isLoading,
  isCustomTabDirty,
  isSubmitting,
}: ModalFormProps) => {
  const { t } = getIntlContext();
  const [tab, setTab] = useState(0);
  const [leaveOpen, setLeaveOpen] = useState({
    state: false,
    action: () => {},
  });

  const methods = useForm({
    mode: "onChange",
    defaultValues: defaultValues,
    resolver: validationSchema ? zodResolver(validationSchema) : undefined,
  });

  const { handleSubmit, formState, reset } = methods;

  useEffect(() => {
    reset(defaultValues);
  }, [defaultValues, reset]);

  const handleTabChange = (newTab: number) => {
    if (isCustomTabDirty) {
      setLeaveOpen({ state: true, action: () => setTab(newTab) });
    } else setTab(newTab);
  };

  const handleCloseDialog = useCallback(() => {
    if (formState.isDirty || isCustomTabDirty) {
      setLeaveOpen({ state: true, action: closeDialog });
    } else closeDialog();
  }, [formState.isDirty, closeDialog, isCustomTabDirty]);

  const currentTabContent =
    tabs.find((_, i) => i === tab)?.contentOutline ?? tabs[0].contentOutline;

  const onSubmit = (values: any) => {
    submit(values);
  };

  const isSaving = isSubmitting || formState.isSubmitting;

  return (
    <>
      <ModalContainer isOpen={isOpen} open={() => {}} close={handleCloseDialog}>
        <FormProvider {...methods}>
          <form
            onSubmit={(e) => {
              e.stopPropagation();
              if (!isSaving) handleSubmit(onSubmit)(e);
            }}
          >
            <DialogContent
              title={title}
              toolbar={
                <IconButton
                  aria-label="Close Dialog"
                  onClick={handleCloseDialog}
                >
                  <dremio-icon name="interface/close-big" alt="Close Button" />
                </IconButton>
              }
              className={classes["modal-form"]}
              actions={
                <div className="dremio-button-group">
                  <Button variant="secondary" onClick={handleCloseDialog}>
                    {t("Common.Actions.Cancel")}
                  </Button>
                  <Button variant="primary" type="submit" disabled={isLoading}>
                    {isSaving ? <Spinner /> : t("Common.Actions.Save")}
                  </Button>
                </div>
              }
            >
              <div className="flex flex-row full-height">
                {isLoading ? (
                  <Spinner className={classes["modal-form__spinner"]} />
                ) : (
                  <>
                    <NavPanel
                      showSingleTab
                      tabs={Immutable.fromJS(tabs.map((t) => t.label))}
                      changeTab={handleTabChange}
                      activeTab={tab}
                      className={classes["modal-form__navPanel"]}
                    />
                    {!!defaultValues && (
                      <div
                        className={clsx(
                          "flex flex-1",
                          classes["modal-form__scroll"],
                        )}
                      >
                        <div
                          className={clsx(
                            "flex flex-col flex-1",
                            classes["modal-form__content"],
                          )}
                        >
                          {currentTabContent.map((component) => (
                            <FormInputController
                              key={component.key}
                              element={component}
                              disabled={!!isSaving}
                            />
                          ))}
                        </div>
                      </div>
                    )}
                  </>
                )}
              </div>
            </DialogContent>
          </form>
        </FormProvider>
      </ModalContainer>
      <LeaveModalForm
        isOpen={leaveOpen.state}
        onCancel={() => setLeaveOpen({ state: false, action: () => {} })}
        onConfirm={leaveOpen.action}
      />
    </>
  );
};

export const FormInputController = ({
  element,
  disabled,
}: {
  element: FormComponent;
  disabled: boolean;
}) => {
  const { watch, control } = useFormContext();
  const getWatchingDependencies = (dependencies: string[]) => {
    const watching: Record<string, string> = {};
    dependencies.forEach((d: string) => {
      watching[d] = watch(d);
    });
    return watching;
  };
  const renderController = (comp: FormComponent, className?: string) => {
    return (
      <Controller
        key={comp.key}
        name={comp.key}
        control={control}
        render={({ field, fieldState: { error } }) => {
          return (
            <div
              className={clsx(
                "flex flex-1",
                comp.className ?? className ?? "flex-col",
              )}
            >
              {!element.noLabel && (
                <div className="flex flex-row pb-05">
                  <Label
                    value={
                      <p
                        className={clsx(
                          "pr-05",
                          !comp.label && classes["modal-form__hideLabel"],
                        )}
                      >
                        {comp.label || comp.key}
                      </p>
                    }
                  />
                  {comp.tooltipHint && (
                    <Tooltip content={comp.tooltipHint}>
                      <dremio-icon
                        name="interface/information"
                        class={classes["modal-form__tooltipHint"]}
                        alt={comp.tooltipHint}
                      />
                    </Tooltip>
                  )}
                </div>
              )}
              <ElementInput
                component={comp}
                field={field}
                error={error}
                disabled={disabled}
              />
            </div>
          );
        }}
      />
    );
  };

  if (element.formInputType === "multi" && element.components) {
    return (
      <>
        {element.sectionLabel && (
          <p className="text-lg text-semibold">{element.sectionLabel}</p>
        )}
        <div className="flex flex-col">
          <div className={clsx("flex flex-row gap-2")}>
            {element.components.map((comp) => renderController(comp))}
          </div>
          {element.helpText && (
            <p className="pt-2">
              {typeof element.helpText === "object"
                ? element.helpText.getText?.(
                    getWatchingDependencies(element.helpText.dependencies),
                  )
                : element.helpText}
            </p>
          )}
        </div>
      </>
    );
  } else if (
    element.formInputType === "checkbox-subsection" &&
    element.components
  ) {
    const showSubsection = watch(element.key) === true;
    return (
      <>
        {element.sectionLabel && (
          <p className="text-lg text-semibold pt-2 pb-1">
            {element.sectionLabel}
          </p>
        )}
        <div className="flex flex-col">
          {renderController(element, "flex-row justify-between pb-2")}
          {showSubsection && (
            <div className={clsx("flex flex-row gap-2 pl-2")}>
              {element.components.map((comp) => renderController(comp))}
            </div>
          )}
        </div>
      </>
    );
  } else if (
    element.formInputType === "custom" &&
    element.renderCustomComponent
  ) {
    return element.renderCustomComponent({ element: element, disabled });
  } else
    return (
      <>
        {element.sectionLabel && (
          <p className="text-lg text-semibold">{element.sectionLabel}</p>
        )}
        {renderController(element)}
      </>
    );
};

const ElementInput = ({
  component,
  field,
  error,
  disabled,
}: {
  component: FormComponent;
  field: ControllerRenderProps<FieldValues, string>;
  error?: FieldError;
  disabled?: boolean;
}) => {
  const { defaultValue, ...compProps } =
    (component.typeProps as FormTypeProps) ?? {};
  const selectOptions = useSelect(field.value ?? defaultValue ?? "");

  switch (component.formInputType) {
    case "input":
      return (
        <>
          <Input
            {...field}
            disabled={disabled}
            {...compProps}
            value={field?.value ?? defaultValue}
            {...((compProps as InputTypeProps).type === "number" && {
              onChange: ({ target }: React.FormEvent<HTMLInputElement>) => {
                field.onChange({
                  target: {
                    value:
                      (target as HTMLInputElement).value == ""
                        ? 0
                        : parseInt((target as HTMLInputElement).value),
                  },
                });
              },
            })}
          />
          {error && <p className="pt-05 text-error text-sm">{error.message}</p>}
        </>
      );
    case "textarea":
      return (
        <textarea
          className="form-control"
          style={{ resize: "none", height: "auto" }}
          {...field}
          value={field?.value ?? defaultValue}
          rows={(compProps as TextAreaTypeProps)?.rows ?? 3}
          disabled={disabled}
        />
      );
    case "select":
      return (
        <Select
          {...(selectOptions as any)}
          value={selectOptions.value}
          onChange={(value) => {
            field.onChange({ target: { value } });
            selectOptions.onChange(value as string);
          }}
          renderButtonLabel={(option) =>
            (compProps as SelectTypeProps).options.find(
              (opt) => opt.value === option,
            )?.label ?? option
          }
          disabled={disabled}
        >
          {(compProps as SelectTypeProps).options.map((opt) => (
            <SelectOption key={opt.value} value={opt.value}>
              {opt.label}
            </SelectOption>
          ))}
        </Select>
      );
    case "checkbox":
    case "checkbox-subsection":
      return (
        <Toggle
          {...field}
          value={field?.value ?? defaultValue}
          disabled={disabled}
        />
      );
    default:
      return <Input {...field} />;
  }
};

export default ModalForm;
