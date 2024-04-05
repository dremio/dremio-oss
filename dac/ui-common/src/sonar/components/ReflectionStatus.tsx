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
import { Spinner, Tooltip } from "dremio-ui-lib/components";
import { getIntlContext } from "../../contexts/IntlContext";

const ICON_BASE = "/static/icons/dremio";

// Migrated from accelerationUtils into ui-common
const getTextWithFailureCount = (status: any, statusMessage: string) => {
  const { t } = getIntlContext();
  const msgId =
    status.refreshStatus === "MANUAL"
      ? "Sonar.Reflection.StatusFailedNoReattempt"
      : "Sonar.Reflection.StatusFailedNonFinal";
  return t(msgId, {
    status: statusMessage,
    failCount: status.failureCount,
  });
};
const getReflectionStatusInfo = (reflection: any) => {
  const icon = (() => {
    const { t } = getIntlContext();
    const { status } = reflection;
    const statusMessage =
      status.availabilityStatus === "AVAILABLE"
        ? t("Sonar.Reflection.StatusCanAccelerate")
        : t("Sonar.Reflection.StatusCannotAccelerate");

    let icon = null;
    let text = "";
    let iconId = "job-state/warning";

    if (!reflection.isEnabled) {
      iconId = "job-state/cancel";
      text = t("Sonar.Reflection.StatusDisabled");
    } else if (status.combinedStatus === "CANNOT_ACCELERATE_INITIALIZING") {
      text = t(
        "Sonar.Reflection.AccelerationStatus.CANNOT_ACCELERATE_INITIALIZING.Hint",
      );
    } else if (status.configStatus === "INVALID") {
      iconId = "job-state/failed";
      text = t("Sonar.Reflection.StatusInvalidConfiguration", {
        status: statusMessage,
      });
    } else if (status.refreshStatus === "GIVEN_UP") {
      iconId = "job-state/failed";
      text = t("Sonar.Reflection.StatusFailedFinal", {
        status: statusMessage,
      });
    } else if (status.availabilityStatus === "INCOMPLETE") {
      iconId = "job-state/failed";
      text = t("Sonar.Reflection.StatusIncomplete", {
        status: statusMessage,
      });
    } else if (status.availabilityStatus === "EXPIRED") {
      iconId = "job-state/failed";
      text = t("Sonar.Reflection.StatusExpired", {
        status: statusMessage,
      });
    } else if (
      status.refreshStatus === "RUNNING" ||
      status.refreshStatus === "PENDING"
    ) {
      if (status.availabilityStatus === "AVAILABLE") {
        iconId = "job-state/completed";
        if (status.refreshStatus === "PENDING") {
          text = t(`Sonar.Reflection.StatusPending`, {
            status: statusMessage,
          });
        } else {
          text = t(`Sonar.Reflection.StatusRefreshing`, {
            status: statusMessage,
          });
        }
      } else {
        icon = <Spinner />;
        text = t("Sonar.Reflection.StatusBuilding", {
          status: statusMessage,
        });
      }
    } else if (status.availabilityStatus === "AVAILABLE") {
      if (status.failureCount > 0) {
        iconId = "job-state/warning";
        text = getTextWithFailureCount(status, statusMessage);
      } else if (status.refreshStatus === "MANUAL") {
        iconId = "job-state/completed";
        text = t("Sonar.Reflection.StatusManual", {
          status: statusMessage,
        });
      } else {
        iconId = "job-state/completed";
        text = t("Sonar.Reflection.StatusCanAccelerate");
      }
    } else if (status.failureCount > 0) {
      iconId = "job-state/warning";
      text = getTextWithFailureCount(status, statusMessage);
    } else if (status.refreshStatus === "SCHEDULED") {
      iconId = "job-state/queued";
      text = t("Sonar.Reflection.Scheduled", {
        status: statusMessage,
      });
    } else if (status.refreshStatus === "MANUAL") {
      iconId = "job-state/warning";
      text = t("Sonar.Reflection.StatusManual", {
        status: statusMessage,
      });
    }

    return {
      icon,
      text,
      iconId,
    };
  })();

  return {
    ...icon,
    path: `${ICON_BASE}/${icon.iconId}.svg`,
  };
};

export const ReflectionStatus = (props: { reflection: any }) => {
  const { icon, path, text } = getReflectionStatusInfo(props.reflection);
  return (
    <Tooltip content={text} portal style={{ maxWidth: 300 }}>
      {icon ? icon : <img src={path} alt="" />}
    </Tooltip>
  );
};
