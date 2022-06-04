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
import moment from 'moment';
import { selectUnit } from '@formatjs/intl-utils';
import { intl } from './intl';

export const DEFAULT_FORMAT = 'MM/DD/YYYY';
export const DEFAULT_FORMAT_WITH_TIME = 'MM/DD/YYYY h:mmA';
export const DEFAULT_FORMAT_WITH_TIME_SECONDS = 'MM/DD/YYYY h:mm:ss A'

export function formatDateRelative(date: string) {
  const d = new Date(date);
  const { value, unit } = selectUnit(d.getTime());
  return intl.formatRelativeTime(value, unit);
}

export function formatDate(date: string, format = DEFAULT_FORMAT) {
  return moment(date).format(format);
}

export function formatDateSince(date: string, format = DEFAULT_FORMAT, daysAgo = 6) {
  const limit = moment().subtract(daysAgo, 'days').startOf('day');
  const wrappedDate = moment(date);
  return wrappedDate > limit ? formatDateRelative(date) : formatDate(date, format);
}
