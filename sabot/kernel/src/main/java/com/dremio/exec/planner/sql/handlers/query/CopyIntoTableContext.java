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

package com.dremio.exec.planner.sql.handlers.query;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.apache.commons.io.FilenameUtils;
import org.apache.commons.lang3.EnumUtils;
import org.apache.commons.text.StringEscapeUtils;

import com.dremio.common.exceptions.UserException;
import com.dremio.common.utils.PathUtils;
import com.dremio.exec.planner.sql.parser.SqlCopyIntoTable;
import com.dremio.service.namespace.file.proto.FileType;

/**
 * Internal structure to hold input parameters/options for 'COPY INTO' command
 */
public final class CopyIntoTableContext {

  public static final int MAX_FILES_ALLOWED = 1000;
  private static final char SOURCE_NAME_LEADING_CHAR = '@';

  private String storageLocation;
  private String providedStorageLocation;
  private String fileNameFromStorageLocation;
  private List<String> files = new ArrayList<>();
  private Optional<String> filePattern;
  private FileType fileFormat;
  private String derivedFileFormat;
  private Map<FormatOption, Object> formatOptions = new HashMap<>();
  private Map<CopyOption, Object> copyOptions = new HashMap<>();

  public CopyIntoTableContext(SqlCopyIntoTable call) {

    validateAndConvertStorageLocation(call.getStorageLocation());

    validateAndConvertFiles(call.getFiles());

    validateFilePattern(call.getFilePattern());

    validateAndConvertToFileType(call.getFileFormat());

    validateAndConvertOptions(call.getOptionsList(), call.getOptionsValueList());
  }

  private void validateAndConvertStorageLocation(String location) {
    // validate storage location
    if (stringIsNullOrEmpty(location)) {
      throw UserException.parseError()
        .message("Input source location cannot be accessed")
        .buildSilently();
    }

    List<String> pathComponents = PathUtils.toPathComponents(location);
    String sourceNameWithAtSign = pathComponents.get(0);
    if (stringIsNullOrEmpty(sourceNameWithAtSign)) {
      throw UserException.parseError()
        .message("Input source location cannot be accessed")
        .buildSilently();
    }

    if (sourceNameWithAtSign.charAt(0) !=  SOURCE_NAME_LEADING_CHAR) {
      throw UserException.parseError()
        .message("Specified location clause not found, Dremio sources should precede with '@'")
        .buildSilently();
    }

    // remove '@'
    String sourceName = sourceNameWithAtSign.substring(1);
    pathComponents.set(0, sourceName);

    // a file is specified
    if (pathComponents.size() > 1) {
      String fileCandidate = pathComponents.get(pathComponents.size() - 1);
      String fileExtension = FilenameUtils.getExtension(fileCandidate);
      if (!fileExtension.isEmpty()) {
        fileNameFromStorageLocation = fileCandidate;
        // remove the file name from the path
        pathComponents.remove(pathComponents.size() - 1);
      }
    }
    providedStorageLocation = location;
    storageLocation = PathUtils.constructFullPath(pathComponents);
  }

  private void validateFilePattern(Optional<String> filePattern) {
    if (filePattern.isPresent() && !stringIsNullOrEmpty(fileNameFromStorageLocation)) {
      throw UserException.parseError()
        .message("When specifying 'FILES' or 'REGEX' location_clause must end with a directory, found a file")
        .buildSilently();
    }
    this.filePattern = filePattern;
  }

  private void validateAndConvertFiles(List<String> inputFiles) {
    if (!stringIsNullOrEmpty(fileNameFromStorageLocation)) {
      if (!inputFiles.isEmpty()) {
        throw UserException.parseError()
          .message("When specifying 'FILES' or 'REGEX' location_clause must end with a directory, found a file")
          .buildSilently();
      }
      files.add(fileNameFromStorageLocation);
    } else if (inputFiles.isEmpty()) {
        return;
    } else {
      files.addAll(inputFiles);
    }

    if(files.size() > MAX_FILES_ALLOWED) {
      throw UserException.parseError()
        .message(String.format("Maximum number of files allowed in the FILES clause is %s", MAX_FILES_ALLOWED))
        .buildSilently();
    }

    // validate all files have same extension
    // derive file format from files before file name normalization
    derivedFileFormat = deriveFileFormatFromFiles(files);
  }

  private static String deriveFileFormatFromFiles(List<String> files) {
    String fileExtension = "";
    if (!files.isEmpty()) {
      for (String file : files) {
        String currentFileExtension = FilenameUtils.getExtension(file);
        if (fileExtension.isEmpty()) {
          fileExtension = currentFileExtension;
          continue;
        }
        if (!fileExtension.equals(currentFileExtension)) {
          throw UserException.parseError()
            .message("Files with only one type of extension are allowed in the files list.")
            .buildSilently();
        }
      }
    }
    return fileExtension;
  }

  private void validateAndConvertToFileType(Optional<String> userFileFormat) {
    String fileFormatString;

    // user specified format has the highest priority
    if (userFileFormat.isPresent()) {
      fileFormatString = userFileFormat.get();
    } else {
      // try to get derived file format from regex
      if (stringIsNullOrEmpty(derivedFileFormat) && filePattern.isPresent()) {
        derivedFileFormat = FilenameUtils.getExtension(filePattern.get());
      }

      if (stringIsNullOrEmpty(derivedFileFormat)){
        throw UserException.parseError()
          .message("File format could not be inferred from the file extension, please specify FILE_FORMAT option.")
          .buildSilently();
      }
      fileFormatString = derivedFileFormat;
    }

    fileFormatString = fileFormatString.toUpperCase();
    switch (fileFormatString) {
      case "CSV":
        fileFormat = FileType.TEXT;
        break;
      case "JSON":
        fileFormat = FileType.JSON;
        break;
      default:
        throw UserException.parseError()
          .message("Specified File Format '%s' is not supported", fileFormatString)
          .buildSilently();
    }
  }

  private static Object convertStringFormatOptionValue(FormatOption option, String value) {
    switch (option) {
      case RECORD_DELIMITER:
        return StringEscapeUtils.unescapeJava(value);
      case FIELD_DELIMITER:
      case QUOTE_CHAR:
      case ESCAPE_CHAR:
        // validate it is a char
        value = StringEscapeUtils.unescapeJava(value);
        if (value.length() != 1) {
          break;
        }
        return value;
      case TRIM_SPACE:
      case EMPTY_AS_NULL:
        String upperValue = value.toUpperCase();
        if (!upperValue.equals("FALSE") && !upperValue.equals("TRUE") ) {
          break;
        }
        return Boolean.valueOf(upperValue);
      case DATE_FORMAT:
      case TIME_FORMAT:
      case TIMESTAMP_FORMAT:
        return value;
    }

    throw UserException.parseError()
      .message("Specified value '%s' is not valid for Format Option '%s' ", value, option)
      .buildSilently();
  }

  private static Object convertCopyOptionValue(CopyOption option, String value) {
    switch (option) {
      case ON_ERROR:
        if (!EnumUtils.isValidEnumIgnoreCase(OnErrorAction.class, value)) {
          throw UserException.parseError()
            .message("Specified value '%s' is not valid for Copy Option ON_ERROR", value)
            .buildSilently();
        }
        return OnErrorAction.valueOf(value.toUpperCase());
      default:
        throw UserException.parseError()
          .message("Specified Copy Option '%s' is not supported'", option)
          .buildSilently();
    }
  }

  public String getStorageLocation() {
    return storageLocation;
  }

  public String getProvidedStorageLocation() {
    return providedStorageLocation;
  }

  public List<String> getFiles() {
    return files;
  }

  public Optional<String> getFilePattern() {
    return filePattern;
  }

  public FileType getFileFormat() {
    return fileFormat;
  }

  public Map<FormatOption, Object> getFormatOptions() {
    return formatOptions;
  }

  public Map<CopyOption, Object> getCopyOptions() {
    return copyOptions;
  }

  private void validateAndConvertOptions(List<String> optionsList, List<Object> optionsValueList) {
    if (optionsList.size() != optionsValueList.size()) {
      throw UserException.parseError()
        .message("Option names do not match option values")
        .buildSilently();
    }

    for (int index = 0; index < optionsList.size(); index++) {
      String optionString = optionsList.get(index);
      Object optionValue= optionsValueList.get(index);
      if (EnumUtils.isValidEnumIgnoreCase(FormatOption.class, optionString)) {
        FormatOption option = FormatOption.valueOf(optionString);
        Object convertedOptionValue = optionValue;
        if (optionValue instanceof String) {
          convertedOptionValue = convertStringFormatOptionValue(option, (String)optionValue);
        }
        formatOptions.put(option, convertedOptionValue);
      } else if (EnumUtils.isValidEnumIgnoreCase(CopyOption.class, optionString)) {
        CopyOption option = CopyOption.valueOf(optionString);
        copyOptions.put(option, convertCopyOptionValue(option, (String)optionValue));
      }
    }
  }

  private static boolean stringIsNullOrEmpty(String string) {
    return string ==null || string.trim().isEmpty();
  }

  public enum FormatOption {
    // Common
    NULL_IF,

    // CSV and JSON
    DATE_FORMAT,
    TIME_FORMAT,
    TIMESTAMP_FORMAT,
    TRIM_SPACE,

    // CSV specific
    RECORD_DELIMITER,
    FIELD_DELIMITER,
    SKIP_HEADER,
    QUOTE_CHAR,
    ESCAPE_CHAR,
    EMPTY_AS_NULL
  }

  public enum CopyOption {
    ON_ERROR
  }

  public enum OnErrorAction {
    // Todo: support CONTINUE and SKIP_FILE
    //CONTINUE,
    //SKIP_FILE,
    ABORT_COPY
  }
}
