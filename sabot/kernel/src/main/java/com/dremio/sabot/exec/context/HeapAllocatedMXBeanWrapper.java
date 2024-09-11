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
package com.dremio.sabot.exec.context;

import com.dremio.common.SuppressForbidden;
import java.lang.management.ManagementFactory;
import java.lang.management.ThreadMXBean;

@SuppressForbidden
public class HeapAllocatedMXBeanWrapper {
  private static final ThreadMXBean mxBean = ManagementFactory.getThreadMXBean();
  private static volatile boolean featureSupported = false;

  public static void setFeatureSupported(boolean isSupported) {
    featureSupported = isSupported;
  }

  public static long getCurrentThreadAllocatedBytes() {
    try {
      if (featureSupported && mxBean instanceof com.sun.management.ThreadMXBean) {
        com.sun.management.ThreadMXBean sunMXBean = (com.sun.management.ThreadMXBean) mxBean;
        long val = sunMXBean.getThreadAllocatedBytes(Thread.currentThread().getId());
        if (val < 0) {
          sunMXBean.setThreadAllocatedMemoryEnabled(true);
          val = sunMXBean.getThreadAllocatedBytes(Thread.currentThread().getId());
        }
        if (val < 0) {
          featureSupported = false;
        }
        return val;
      }
      featureSupported = false;
    } catch (Throwable t) {
      featureSupported = false;
    }
    return -1L;
  }

  public static boolean isFeatureSupported() {
    return featureSupported;
  }
}
