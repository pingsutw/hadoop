/**
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License. See accompanying LICENSE file.
 */

package org.apache.hadoop.yarn.submarine.client.cli.param;

import java.io.IOException;

import org.apache.commons.cli.ParseException;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.submarine.client.cli.CliConstants;
import org.apache.hadoop.yarn.submarine.common.ClientContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CreateModelParameters extends BaseParameters {
  private static final Logger LOG =
      LoggerFactory.getLogger(CreateModelParameters.class);
  private String modelPath;
  private String modelVersion;

  @Override
  public void updateParameters(ParametersHolder parametersHolder,
      ClientContext clientContext)
      throws ParseException, IOException, YarnException {
    super.updateParameters(parametersHolder, clientContext);

    String modelPath = parametersHolder.getOptionValue(CliConstants.MODEL_PATH);
    if (modelPath == null) {
      throw new ParseException(
          "\"--" + CliConstants.MODEL_PATH + "\" is absent");
    }
    this.setModelPath(modelPath);

    String modelVersion = parametersHolder.getOptionValue(CliConstants.VERSION);
    if (modelVersion == null) {
      modelVersion = "default";
    }
    LOG.info("Model Version : {}", modelVersion);
    this.setModelVersion(modelVersion);
  }

  public String getModelVersion() {
    return modelVersion;
  }

  public CreateModelParameters setModelVersion(String modelVersion) {
    this.modelVersion = modelVersion;
    return this;
  }

  public String getModelPath() {
    return modelPath;
  }

  public CreateModelParameters setModelPath(String modelPath) {
    this.modelPath = modelPath;
    return this;
  }
}
