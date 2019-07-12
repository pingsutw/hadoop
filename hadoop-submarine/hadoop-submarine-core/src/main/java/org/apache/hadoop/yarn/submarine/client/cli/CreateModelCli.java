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

package org.apache.hadoop.yarn.submarine.client.cli;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.submarine.client.cli.param.CreateModelParameters;
import org.apache.hadoop.yarn.submarine.client.cli.param.ParametersHolder;
import org.apache.hadoop.yarn.submarine.common.ClientContext;
import org.apache.hadoop.yarn.submarine.common.exception.SubmarineException;
import org.apache.hadoop.yarn.submarine.runtimes.common.ModelSubmitter;
import org.apache.hadoop.yarn.submarine.runtimes.common.StorageKeyConstants;
import org.apache.hadoop.yarn.submarine.runtimes.common.SubmarineStorage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;

public class CreateModelCli extends AbstractCli {
  private static final Logger LOG =
      LoggerFactory.getLogger(CreateModelCli.class);

  private Options options;
  private ParametersHolder parametersHolder;
  private ModelSubmitter modelSubmitter;

  public CreateModelCli(ClientContext cliContext) {
    super(cliContext);
    options = generateOptions();
    modelSubmitter = new ModelSubmitter(cliContext);
  }

  public void printUsages() {
    new HelpFormatter().printHelp("model create", options);
  }

  private Options generateOptions() {
    Options options = new Options();
    options.addOption(CliConstants.NAME, true, "Name of the model");
    options.addOption(CliConstants.MODEL_PATH, true, "Path to the model");
    options.addOption(CliConstants.VERSION, true,
        "version of the model. The default version is \"default\"");
    options.addOption("h", "help", false, "Print help");
    return options;
  }

  private void parseCommandLineAndGetModelCreateParameters(String[] args)
      throws IOException, YarnException, ParseException {
    try {
      GnuParser parser = new GnuParser();
      CommandLine cli = parser.parse(options, args);
      parametersHolder =
          ParametersHolder.createWithCmdLine(cli, Command.CREATE_MODEL);
      parametersHolder.updateParameters(clientContext);
    } catch (ParseException e) {
      LOG.error("Exception in parse: {}", e.getMessage());
      printUsages();
      throw e;
    }
  }

  @VisibleForTesting
  public CreateModelParameters getParameters() {
    return (CreateModelParameters) parametersHolder.getParameters();
  }

  private boolean modelExists() throws IOException {
    SubmarineStorage storage =
        clientContext.getRuntimeFactory().getSubmarineStorage();
    Map<String, String> modelInfo = null;
    try {
      modelInfo = storage.getModelInfoByName(getParameters().getName(),
          getParameters().getModelVersion());
    } catch (Exception e) {
      LOG.error("Failed to retrieve job info", e);
    }
    if (modelInfo == null) {
      return false;
    }
    return true;
  }

  @VisibleForTesting
  protected void storeAndSubmitModel() throws IOException, YarnException {
    boolean modelexists = modelExists();
    if (modelexists) {
      LOG.error("Model already exists !");
      throw new IOException();
    }

    String modelName = getParameters().getName();
    String modelversion = getParameters().getModelVersion();
    LOG.info(modelversion);

    Map<String, String> modelInfo = new HashMap<>();
    modelInfo.put(StorageKeyConstants.MODEL_NAME, modelName);

    if (getParameters().getModelVersion() != null) {
      modelInfo.put(StorageKeyConstants.MODEL_VERSION,
          getParameters().getModelVersion());
    }
    if (getParameters().getModelPath() != null) {
      modelInfo.put(StorageKeyConstants.MODEL_PATH,
          getParameters().getModelPath());
    }

    clientContext.getRuntimeFactory().getSubmarineStorage()
        .addNewModel(modelName, modelversion, modelInfo);
    modelSubmitter.submitModel(getParameters());
  }

  @Override
  public int run(String[] args) throws ParseException, IOException,
      YarnException, InterruptedException, SubmarineException {
    if (CliUtils.argsForHelp(args)) {
      printUsages();
      return 0;
    }
    parseCommandLineAndGetModelCreateParameters(args);
    storeAndSubmitModel();

    LOG.info("Create model successfully");
    return 0;
  }

}
