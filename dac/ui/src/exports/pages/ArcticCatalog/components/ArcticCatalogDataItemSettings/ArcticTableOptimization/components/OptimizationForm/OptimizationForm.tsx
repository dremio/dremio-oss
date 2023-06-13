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
import { useEffect } from "react";
import { useForm, FormProvider } from "react-hook-form";
import { zodResolver } from "@hookform/resolvers/zod";
import { DaySelector } from "../DaySelector/DaySelector";
import { WeekInput } from "../WeekInput/WeekInput";
import { TimeInput } from "../TimeInput/TimeInput";
import { MonthInput } from "../MonthInput/MonthInput";
import { AdvancedConfig } from "../AdvancedConfig/AdvancedConfig";
import { FormSubmit } from "../FormSubmit/FormSubmit";
import {
  cronGenerator,
  transformAdvancedConfigToString,
} from "../../compactionUtils";
import { createSchedule } from "@app/exports/endpoints/ArcticCatalogs/Optimization/createSchedule";
import { modifySchedule } from "@app/exports/endpoints/ArcticCatalogs/Optimization/modifySchedule";
import { JobScheduleInfo } from "@app/exports/endpoints/ArcticCatalogs/Optimization/Schedule.types";
import { optimizationFormProps } from "../../Optimization.types";
import { optimizationScheduleResource } from "@app/exports/resources/OptimizationScheduleResource";
import { validationSchema } from "./validationSchema";
import * as classes from "./OptimizationForm.module.less";

export const OptimizationForm = ({
  reference,
  tableId,
  catalogId,
  initialValues,
  cron,
  scheduleId,
  setCron,
  setShowForm,
  setMessage,
}: optimizationFormProps) => {
  const methods = useForm<any>({
    mode: "onChange",
    resolver: zodResolver(validationSchema),
    defaultValues: {
      ...initialValues,
    },
  });

  const { reset, watch, handleSubmit, getValues } = methods;
  const scheduleType = watch("schedule.scheduleType");

  const onSubmit = async ({
    advancedConfig: { maxFileSize, minFileSize, minFiles, targetFileSize },
  }: any) => {
    const transformedAdvancedConfig = {
      maxFileSize: transformAdvancedConfigToString(maxFileSize),
      minFileSize: transformAdvancedConfigToString(minFileSize),
      minFiles,
      targetFileSize: transformAdvancedConfigToString(targetFileSize),
    };

    const operation = scheduleId ? modifySchedule : createSchedule;
    try {
      await operation({
        catalogId,
        //@ts-ignore
        scheduleId,
        payload: {
          config: { ...transformedAdvancedConfig, reference, tableId },
          schedule: cron,
          type: JobScheduleInfo.TypeEnum.OPTIMIZE,
        },
      });
      setShowForm(false);
      optimizationScheduleResource.fetch({
        catalogId,
        tableId,
        reference,
      });
    } catch (e: any) {
      setMessage({ text: e?.responseBody?.errorMessage, type: "error" });
    }
  };

  useEffect(() => {
    const {
      schedule: {
        timeInput,
        weekInput,
        dayOfMonth,
        ordinalOfEveryMonth,
        weekdayOfEveryMonth,
        scheduleType,
        monthScheduleOption,
        hourInput,
      },
    } = getValues();
    const cron = cronGenerator({
      everyDayAt: hourInput,
      minute: timeInput.getMinutes(),
      hour: timeInput.getHours(),
      weekValues: weekInput,
      dayOfMonth,
      ordinalOfEveryMonth,
      weekdayOfEveryMonth,
      scheduleType,
      monthScheduleOption,
    });
    setCron(cron);
  }, [getValues, setCron]);

  useEffect(() => {
    watch(
      ({
        schedule: {
          timeInput,
          weekInput,
          dayOfMonth,
          ordinalOfEveryMonth,
          weekdayOfEveryMonth,
          scheduleType,
          monthScheduleOption,
          hourInput,
        },
      }) => {
        if (hourInput > 0 && dayOfMonth > 0) {
          const cron = cronGenerator({
            everyDayAt: hourInput,
            minute: timeInput.getMinutes(),
            hour: timeInput.getHours(),
            weekValues: weekInput,
            dayOfMonth,
            ordinalOfEveryMonth,
            weekdayOfEveryMonth,
            scheduleType,
            monthScheduleOption,
          });
          setCron(cron);
        }
      }
    );
  }, [watch, setCron]);

  const onCancel = () => {
    setShowForm(false);
    reset();
  };

  return (
    <FormProvider {...methods}>
      <form onSubmit={handleSubmit(onSubmit)}>
        <div className={classes["form-container"]}>
          <DaySelector name="schedule.scheduleType" />
          {scheduleType === "@week" && <WeekInput name="schedule.weekInput" />}
          {scheduleType === "@month" && <MonthInput />}
          {scheduleType !== "@hours" && <TimeInput name="schedule.timeInput" />}
        </div>
        <AdvancedConfig />
        <FormSubmit onCancel={onCancel} />
      </form>
    </FormProvider>
  );
};
