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

  //array that contain the name of the class of the logger and the threshold (comma separated)
  private List<String[]> packageThresholdLevel = new ArrayList<>();
  //default- used for third party loggers
  private Level defaultLogLevelThreshold;
  private boolean start = false;

  private boolean matches(String classPath, String logger)
  {
    int i;
    for(i=0 ; i<classPath.length();i++)
    {
      if(logger.charAt(i)!=classPath.charAt(i))
      {
        return false;
      }
    }
    //if i is pointing to the end of the logger -> there is an exact match
    if(i == logger.length())
    {
      return true;
    }
    //classpath is a prefix of logger according to sl4j convention
    if(logger.charAt(i) == '.')
    {
      return true;
    }
    return false;

  }


  @Override
  public FilterReply decide(Marker marker, Logger logger, Level level, String s, Object[] objects, Throwable throwable) {

    int maximumMatchingPackageLength = 0;
    //we initialise longestMatchingPackageThreshold to the default threshold. Hence if no match is found we use the default
    String[] longestMatchingPackageThreshold = new String[]{"defaultLog",defaultLogLevelThreshold.levelStr};
    for (int i = 0; i < packageThresholdLevel.size(); i++) {
      if (matches(this.packageThresholdLevel.get(i)[0], logger.getName())) {
        if (this.packageThresholdLevel.get(i)[0].length() > maximumMatchingPackageLength) {
          longestMatchingPackageThreshold = this.packageThresholdLevel.get(i);
          maximumMatchingPackageLength = this.packageThresholdLevel.get(i)[0].length();
        }
      }
    }

    if (!level.isGreaterOrEqual(Level.toLevel(longestMatchingPackageThreshold[1]))) {
      return FilterReply.DENY;
    }

    return FilterReply.NEUTRAL;


  }

  public void addPackageLogLevel(String packageLogLevel) {
    String[] arrOfStr = packageLogLevel.split(",");
    this.packageThresholdLevel.add(arrOfStr);
  }

  public void setDefaultLogLevelThreshold(String defaultLogLevelThreshold) {
    this.defaultLogLevelThreshold = Level.toLevel(defaultLogLevelThreshold);
  }

  @Override
  public void stop() {
    this.start = false;
  }

  @Override
  public void start() {
    if (this.defaultLogLevelThreshold != null) {
      super.start();
      this.start = true;
    }
  }
}
