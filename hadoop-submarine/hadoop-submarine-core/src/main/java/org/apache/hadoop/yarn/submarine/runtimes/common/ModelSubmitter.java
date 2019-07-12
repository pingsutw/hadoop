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

package org.apache.hadoop.yarn.submarine.runtimes.common;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.submarine.client.cli.param.BaseParameters;
import org.apache.hadoop.yarn.submarine.client.cli.param.CreateModelParameters;
import org.apache.hadoop.yarn.submarine.common.ClientContext;
import org.apache.hadoop.yarn.submarine.common.fs.RemoteDirectoryManager;

/**
 * Submit Model to cluster master.
 */
public class ModelSubmitter {

  private final Configuration yarnConfig;
  private RemoteDirectoryManager remoteDirectoryManager;

  public ModelSubmitter(ClientContext clientContext) {
    this.yarnConfig = clientContext.getYarnConfig();
    this.remoteDirectoryManager = clientContext.getRemoteDirectoryManager();
  }

  public boolean submitModel(BaseParameters parameters)
      throws IOException, YarnException {
    CreateModelParameters createparameters = (CreateModelParameters) parameters;
    Path modelDir =
        remoteDirectoryManager.getModelDir(createparameters.getName(), true);
    String modelPath = createparameters.getModelPath();
    Path srcPath = new Path(modelPath);

    FileSystem fs = FileSystem.get(yarnConfig);
    fs.copyFromLocalFile(srcPath, modelDir);
    return true;
  }
}
