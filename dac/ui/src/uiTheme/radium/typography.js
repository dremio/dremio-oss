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

const menlo = "Menlo, monospace";
const inter = "var(--dremio--font-family)";

export const bodySmall = {
  fontFamily: inter,
  fontWeight: 400,
  fontSize: 11,
  color: "var(--dremio--color--text--main)",
};

export const formLabel = {
  fontFamily: inter,
  fontWeight: 400,
  color: "var(--dremio--color--text--main)",
  display: "flex",
  alignItems: "center",
  height: "32px",
};

export const formDefault = {
  fontFamily: inter,
  fontWeight: 400,
  fontSize: 13,
  color: "var(--dremio--color--text--main)",
};

export const formDescription = {
  ...formDefault,
  color: "var(--dremio--color--text--faded)",
};

export const formContext = {
  ...formDescription,
  fontStyle: "italic",
  fontSize: 11,
};

export const formPlaceholder = {
  fontFamily: inter,
  fontWeight: 400,
  fontStyle: "italic",
  fontSize: 14,
  color: "var(--dremio--color--text--main)",
};

export const fixedWidthDefault = {
  fontFamily: menlo,
  fontWeight: 400,
  fontSize: 12,
  color: "var(--dremio--color--text--main)",
};

export const fixedWidthSmall = {
  fontFamily: menlo,
  fontWeight: 400,
  fontSize: 11,
  color: "var(--dremio--color--text--main)",
};

export const fixedWidthBold = {
  fontFamily: menlo,
  fontWeight: 700,
  fontSize: 11,
  color: "var(--dremio--color--text--main)",
};

export const unavailable = {
  fontFamily: inter,
  fontWeight: 400,
  fontSize: 12,
  color: "#BBBBBB",
};

export const datasetTitle = {
  fontFamily: inter,
  fontWeight: 500,
  fontSize: 13,
  color: "var(--dremio--color--text--main)",
};

export const pathLink = {
  fontFamily: inter,
  fontWeight: 400,
  fontSize: 11,
  color: "#46B4D5",
};

export const SSOLink = {
  fontFamily: inter,
  fontWeight: 400,
  color: "#81D2EB",
};

export const lightLink = {
  fontFamily: inter,
  fontWeight: 400,
  fontSize: 11,
  color: "#81D2EB",
};

export const pathEnd = {
  fontFamily: inter,
  fontWeight: 400,
  fontSize: 11,
  color: "var(--dremio--color--text--main)",
};

export const keyLabel = {
  fontFamily: inter,
  fontWeight: 400,
  fontSize: 11,
  color: "#999999",
};

export const keyValue = {
  fontFamily: inter,
  fontWeight: 400,
  fontSize: 11,
  color: "var(--dremio--color--text--main)",
};

export const h2White = {
  fontFamily: inter,
  fontWeight: 300,
  fontSize: 18,
  color: "var(--dremio--color--neutral--000)",
};

export const h3White = {
  fontFamily: inter,
  fontWeight: 300,
  fontSize: 16,
  color: "var(--dremio--color--neutral--000)",
};

export const h4White = {
  fontFamily: inter,
  fontWeight: 500,
  fontSize: 13,
  color: "var(--dremio--color--neutral--000)",
};

export const h5White = {
  fontFamily: inter,
  fontWeight: 500,
  fontSize: 12,
  color: "var(--dremio--color--neutral--000)",
};

export const bodyWhite = {
  fontFamily: inter,
  fontWeight: 400,
  fontSize: 12,
  color: "var(--dremio--color--neutral--000)",
};

export const bodySmallWhite = {
  fontFamily: inter,
  fontWeight: 400,
  fontSize: 11,
  color: "var(--dremio--color--neutral--000)",
};

export const metadataWhite = {
  fontFamily: inter,
  fontWeight: 400,
  fontSize: 11,
  color: "#DDDDDD",
};

export const linkLightWhite = {
  fontFamily: inter,
  fontWeight: 400,
  fontSize: 11,
  color: "#81D2EB",
};

export default {
  bodySmall,
  formLabel,
  formDefault,
  formDescription,
  formPlaceholder,
  fixedWidthDefault,
  fixedWidthSmall,
  fixedWidthBold,
  unavailable,
  datasetTitle,
  pathLink,
  pathEnd,
  keyLabel,
  keyValue,
  h2White,
  h3White,
  h4White,
  h5White,
  bodyWhite,
  bodySmallWhite,
  metadataWhite,
  linkLightWhite,
  formContext,
};
