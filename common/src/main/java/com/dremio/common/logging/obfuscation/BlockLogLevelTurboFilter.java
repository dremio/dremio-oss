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
package com.dremio.common.logging.obfuscation;

import java.util.ArrayList;
import java.util.List;

import org.slf4j.Marker;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.turbo.TurboFilter;
import ch.qos.logback.core.spi.FilterReply;

/**
 * CustomTurbofilter class for blocking log levels below threshold.
 * We find the logger's threshold and compare the level to this threshold. If the level is below this threshold we return FilterReply.ACCEPT , else we return FilterReply.DENY
 *  TRACE < DEBUG. < INFO < WARN < ERROR.
 *  To add a logger with a threshold, add the name of the logger and the threshold both comma separated in a logback.xml file, using appropriate tags.
 */
public class BlockLogLevelTurboFilter extends TurboFilter {

  //arrays that contain the name of the class of the logger and the threshold (comma separated)
  private List<String> packageThresholdLevelException = new ArrayList<>();
  private List<String> packageThresholdLevel = new ArrayList<>();
  //default- used for third party loggers
  private Level defaultLogLevelThreshold;
  private boolean start = false;

  @Override
  public FilterReply decide(Marker marker, Logger logger, Level level, String s, Object[] objects, Throwable throwable) {

    for (int i = 0; i < this.packageThresholdLevelException.size(); i++) {
      //since arrOfStr has the name of the logger and threshold comma separated
      String[] arrOfStr = this.packageThresholdLevelException.get(i).split(",");
      if (logger.getName().startsWith(arrOfStr[0])) {
        if (level.isGreaterOrEqual(Level.toLevel(arrOfStr[1]))) {
          return FilterReply.ACCEPT;
        } else {
          return FilterReply.DENY;
        }
      }
    }

    for (int i = 0; i < this.packageThresholdLevel.size(); i++) {
      String[] arrOfStr = this.packageThresholdLevel.get(i).split(",");
      if (logger.getName().startsWith(arrOfStr[0])) {
        if (level.isGreaterOrEqual(Level.toLevel(arrOfStr[1]))) {
          return FilterReply.ACCEPT;
        } else {
          return FilterReply.DENY;
        }
      }
    }

    //if we do not find the logger in packageThresholdLevel or packageThresholdLevelException we resort to using the defaultThresholdLevel (used by third party loggers)
    if (level.isGreaterOrEqual(defaultLogLevelThreshold)) {
      return FilterReply.ACCEPT;
    }

    return FilterReply.DENY;
  }

  public void addExceptionPackageLogLevel(String exceptionPackageLogLevel) {
    this.packageThresholdLevelException.add(exceptionPackageLogLevel);
  }

  public void addPackageLogLevel(String packageLogLevel) {
    this.packageThresholdLevel.add(packageLogLevel);
  }

  public void setDefaultLogLevelThreshold(String defaultLogLevelThreshold) {
    this.defaultLogLevelThreshold = Level.toLevel(defaultLogLevelThreshold);
  }

  public void stop() {
    this.start = false;
  }

  public void start() {
    if (this.defaultLogLevelThreshold != null) {
      super.start();
      this.start = true;
    }
  }
}
