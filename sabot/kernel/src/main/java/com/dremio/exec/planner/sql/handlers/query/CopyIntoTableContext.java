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

import com.dremio.common.exceptions.UserException;
import com.dremio.common.expression.BasePath;
import com.dremio.common.utils.PathUtils;
import com.dremio.exec.physical.config.copyinto.CopyIntoFileLoadInfo;
import com.dremio.exec.planner.sql.parser.SqlCopyIntoTable;
import com.dremio.exec.record.RecordBatchData;
import com.dremio.exec.record.VectorAccessible;
import com.dremio.exec.store.dfs.copyinto.CopyJobHistoryTableSchemaProvider;
import com.dremio.service.namespace.file.proto.FileType;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSet;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.lang3.EnumUtils;
import org.apache.commons.text.StringEscapeUtils;

/** Internal structure to hold input parameters/options for 'COPY INTO' command */
public final class CopyIntoTableContext {

  public static final int MAX_FILES_ALLOWED = 1000;
  private static final char SOURCE_NAME_LEADING_CHAR = '@';

  private String storageSource;
  private String storageLocation;
  private String providedStorageLocation;
  private String fileNameFromStorageLocation;
  private final List<String> files = new ArrayList<>();
  private final Optional<String> filePattern;
  private FileType fileFormat;
  private String derivedFileFormat;
  private Map<FormatOption, Object> formatOptions = new HashMap<>();
  private final Map<CopyOption, Object> copyOptions = new HashMap<>();
  private Map<IngestionOption, String> ingestionOptions = new HashMap<>();
  private String originalQueryId;
  private final boolean isValidationMode;

  public CopyIntoTableContext(SqlCopyIntoTable call) {

    validateAndConvertStorageLocation(
        call.getStorageLocation(), !call.getFiles().isEmpty() || call.getFilePattern().isPresent());

    validateAndConvertFiles(call.getFiles());

    this.filePattern = call.getFilePattern();

    validateAndConvertToFileType(call.getFileFormat());

    validateAndConvertOptions(call.getOptionsList(), call.getOptionsValueList());

    this.isValidationMode = false;
  }

  // CTOR for copy_errors use case
  private CopyIntoTableContext(
      String validatedStorageLocation,
      List<String> files,
      FileType fileFormat,
      String originalQueryId,
      Map<FormatOption, Object> formatOptions,
      Map<IngestionOption, String> ingestionOptions) {
    this.storageLocation = validatedStorageLocation;
    this.providedStorageLocation = validatedStorageLocation;
    validateAndConvertFiles(files);
    this.fileFormat = fileFormat;
    this.formatOptions = formatOptions;
    this.ingestionOptions = ingestionOptions;
    this.filePattern = Optional.empty();
    this.originalQueryId = originalQueryId;
    this.isValidationMode = true;
  }

  private void validateAndConvertStorageLocation(String location, boolean filesOrRegexSpecified) {
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

    if (sourceNameWithAtSign.charAt(0) != SOURCE_NAME_LEADING_CHAR) {
      throw UserException.parseError()
          .message("Specified location clause not found, Dremio sources should precede with '@'")
          .buildSilently();
    }

    // remove '@'
    storageSource = sourceNameWithAtSign.substring(1);
    pathComponents.set(0, storageSource);

    // a file is specified
    if (pathComponents.size() > 1) {
      String fileCandidate = pathComponents.get(pathComponents.size() - 1);
      String fileExtension = FilenameUtils.getExtension(fileCandidate);
      // not setting fileNameFromStorageLocation if FILES or REGEX is defined
      // in this case we assume we found a directory with dots in its name rather a file with file
      // extension
      // otherwise we go with the assumption of this being a file, either way proper error will be
      // thrown later
      if (!fileExtension.isEmpty() && !filesOrRegexSpecified) {
        fileNameFromStorageLocation = fileCandidate;
        // remove the file name from the path
        pathComponents.remove(pathComponents.size() - 1);
      }
    }
    providedStorageLocation = location;
    storageLocation = PathUtils.constructFullPath(pathComponents);
  }

  private void validateAndConvertFiles(List<String> inputFiles) {
    if (!stringIsNullOrEmpty(fileNameFromStorageLocation)) {
      if (!inputFiles.isEmpty()) {
        files.addAll(inputFiles);
      } else {
        files.add(fileNameFromStorageLocation);
      }
    } else if (inputFiles.isEmpty()) {
      return;
    } else {
      files.addAll(inputFiles);
    }

    if (files.size() > MAX_FILES_ALLOWED) {
      throw UserException.parseError()
          .message(
              String.format(
                  "Maximum number of files allowed in the FILES clause is %s", MAX_FILES_ALLOWED))
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

      if (stringIsNullOrEmpty(derivedFileFormat)) {
        throw UserException.parseError()
            .message(
                "File format could not be inferred from the file extension, please specify FILE_FORMAT option.")
            .buildSilently();
      }
      fileFormatString = derivedFileFormat;
    }

    fileFormat = fileTypeFromString(fileFormatString);
  }

  // COPY INTO specific file format mapping
  private static FileType fileTypeFromString(String fileFormatString) {
    fileFormatString = fileFormatString.toUpperCase();
    switch (fileFormatString) {
      case "CSV":
        return FileType.TEXT;
      case "JSON":
        return FileType.JSON;
      case "PARQUET":
        return FileType.PARQUET;
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
      case EXTRACT_HEADER:
        String upperValue = value.toUpperCase();
        if (!"FALSE".equals(upperValue) && !"TRUE".equals(upperValue)) {
          break;
        }
        return Boolean.valueOf(upperValue);
      case DATE_FORMAT:
      case TIME_FORMAT:
      case TIMESTAMP_FORMAT:
        return value;
      case SKIP_LINES:
        try {
          int parsed = Integer.parseInt(value);
          if (parsed >= 0) {
            return parsed;
          } else {
            throw UserException.parseError()
                .message(
                    "Invalid value for SKIP_LINES: must be a non-negative integer, but got '%s'",
                    value)
                .buildSilently();
          }
        } catch (NumberFormatException nfE) {
          throw UserException.parseError(nfE)
              .message(
                  "Invalid value for SKIP_LINES: '%s' can not be parsed into an integer", value)
              .buildSilently();
        }
      default:
        throw UserException.parseError()
            .message("Unhandled Format Option '%s' with value '%s' ", option, value)
            .buildSilently();
    }
    throw UserException.parseError()
        .message("Specified value '%s' is not valid for Format Option '%s' ", value, option)
        .buildSilently();
  }

  private static Object convertCopyOptionValue(CopyOption option, String value) {
    if (Objects.requireNonNull(option) == CopyOption.ON_ERROR) {
      if (!EnumUtils.isValidEnumIgnoreCase(OnErrorAction.class, value)) {
        throw UserException.parseError()
            .message("Specified value '%s' is not valid for Copy Option ON_ERROR", value)
            .buildSilently();
      }
      return OnErrorAction.valueOf(value.toUpperCase());
    }
    throw UserException.parseError()
        .message("Specified Copy Option '%s' is not supported'", option)
        .buildSilently();
  }

  public String getStorageLocation() {
    return storageLocation;
  }

  public String getStorageSource() {
    return storageSource;
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

  public String getOriginalQueryId() {
    return originalQueryId;
  }

  public boolean isValidationMode() {
    return isValidationMode;
  }

  private void validateAndConvertOptions(List<String> optionsList, List<Object> optionsValueList) {
    if (optionsList.size() != optionsValueList.size()) {
      throw UserException.parseError()
          .message("Option names do not match option values")
          .buildSilently();
    }

    for (int index = 0; index < optionsList.size(); index++) {
      String optionString = optionsList.get(index);
      Object optionValue = optionsValueList.get(index);
      if (EnumUtils.isValidEnumIgnoreCase(FormatOption.class, optionString)) {
        FormatOption option = FormatOption.valueOf(optionString);
        FileTypeSpecificFormatOptions fileTypeSpecificFormatOptions =
            FileTypeSpecificFormatOptions.valueOf(fileFormat.name().toUpperCase());
        if (!fileTypeSpecificFormatOptions.options.contains(option)) {
          throw UserException.parseError()
              .message(
                  "Unsupported format option %s for file type %s. Supported format options are: %s",
                  option.name(),
                  fileFormat.name(),
                  fileTypeSpecificFormatOptions.options.stream()
                      .map(Enum::name)
                      .collect(Collectors.joining(", ")))
              .buildSilently();
        }
        Object convertedOptionValue = optionValue;
        if (optionValue instanceof String) {
          convertedOptionValue = convertStringFormatOptionValue(option, (String) optionValue);
        }
        formatOptions.put(option, convertedOptionValue);
      } else if (EnumUtils.isValidEnumIgnoreCase(CopyOption.class, optionString)) {
        CopyOption option = CopyOption.valueOf(optionString);
        copyOptions.put(option, convertCopyOptionValue(option, (String) optionValue));
        if (fileFormat == FileType.PARQUET
            && copyOptions.containsKey(CopyOption.ON_ERROR)
            && copyOptions.get(CopyOption.ON_ERROR) == OnErrorAction.CONTINUE) {
          throw UserException.parseError()
              .message("ON_ERROR 'continue' option is not supported for parquet file format")
              .buildSilently();
        }
      } else if (EnumUtils.isValidEnumIgnoreCase(IngestionOption.class, optionString)) {
        IngestionOption option = IngestionOption.valueOf(optionString);
        ingestionOptions.put(option, (String) optionValue);
      } else {
        throw UserException.parseError()
            .message("Specified '%s' option is not supported", optionString)
            .buildSilently();
      }
    }
  }

  private static boolean stringIsNullOrEmpty(String string) {
    return string == null || string.trim().isEmpty();
  }

  /**
   * Takes a JOIN query result from internal copy_job_history and copy_file_history tables &
   * constructs a CopyIntoTableContext instance from it by selecting and transforming (if necessary)
   * to appropriate record fields.
   *
   * @param recordBatchData query result
   * @param schemaVersion copy_errors table schema version
   * @return CopyIntoTableContext instance
   */
  public static CopyIntoTableContext createFromCopyErrorsQueryResult(
      RecordBatchData recordBatchData, long schemaVersion) {
    Preconditions.checkNotNull(recordBatchData, "recordBatchData must not be null");

    VectorAccessible container = recordBatchData.getVectorAccessible();

    String queryId =
        valueFromVectorContainerByName(
                container, CopyJobHistoryTableSchemaProvider.getJobIdColName(schemaVersion))
            .toString();
    String storageLocation =
        valueFromVectorContainerByName(
                container,
                CopyJobHistoryTableSchemaProvider.getStorageLocationColName(schemaVersion))
            .toString();

    String fileFormatString =
        valueFromVectorContainerByName(
                container, CopyJobHistoryTableSchemaProvider.getFileFormatColName(schemaVersion))
            .toString();
    FileType fileFormat = fileTypeFromString(fileFormatString);

    String formatOptionsJson =
        valueFromVectorContainerByName(
                container, CopyJobHistoryTableSchemaProvider.getCopyOptionsColName(schemaVersion))
            .toString();
    Map<FormatOption, Object> formatOptionsMap =
        CopyIntoFileLoadInfo.Util.getFormatOptions(formatOptionsJson);

    String rejectedFilesFullPath =
        valueFromVectorContainerByName(container, "file_paths").toString();
    List<String> filesWithRejection = Arrays.asList(rejectedFilesFullPath.split(","));

    return new CopyIntoTableContext(
        storageLocation,
        filesWithRejection,
        fileFormat,
        queryId,
        formatOptionsMap,
        Collections.emptyMap());
  }

  private static Object valueFromVectorContainerByName(
      VectorAccessible container, String fieldName) {
    int[] fieldIds = container.getValueVectorId(BasePath.getSimple(fieldName)).getFieldIds();
    return container
        .getValueAccessorById(
            container.getValueVectorId(BasePath.getSimple(fieldName)).getIntermediateClass(),
            fieldIds)
        .getValueVector()
        .getObject(0);
  }

  public boolean isContinuousIngestionCopyTrigger() {
    return !getIngestionOptions().isEmpty();
  }

  public Map<IngestionOption, String> getIngestionOptions() {
    return ingestionOptions;
  }

  public String getFileNameFromStorageLocation() {
    return fileNameFromStorageLocation;
  }

  public enum FileTypeSpecificFormatOptions {
    TEXT(
        ImmutableSet.of(
            FormatOption.DATE_FORMAT,
            FormatOption.EMPTY_AS_NULL,
            FormatOption.ESCAPE_CHAR,
            FormatOption.EXTRACT_HEADER,
            FormatOption.FIELD_DELIMITER,
            FormatOption.NULL_IF,
            FormatOption.QUOTE_CHAR,
            FormatOption.RECORD_DELIMITER,
            FormatOption.SKIP_LINES,
            FormatOption.TIME_FORMAT,
            FormatOption.TIMESTAMP_FORMAT,
            FormatOption.TRIM_SPACE)),
    JSON(
        ImmutableSet.of(
            FormatOption.DATE_FORMAT,
            FormatOption.NULL_IF,
            FormatOption.TIME_FORMAT,
            FormatOption.TIMESTAMP_FORMAT,
            FormatOption.TRIM_SPACE,
            FormatOption.EMPTY_AS_NULL)),
    PARQUET(Collections.EMPTY_SET);

    private final Set<FormatOption> options;

    FileTypeSpecificFormatOptions(Set<FormatOption> options) {
      this.options = options;
    }
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
    EXTRACT_HEADER,
    SKIP_LINES,
    QUOTE_CHAR,
    ESCAPE_CHAR,
    EMPTY_AS_NULL
  }

  public enum CopyOption {
    ON_ERROR
  }

  public enum IngestionOption {
    PIPE_NAME,
    BATCH_ID
  }

  public enum OnErrorAction {
    CONTINUE,
    SKIP_FILE,
    ABORT
  }
}
