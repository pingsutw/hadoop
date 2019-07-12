/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.yarn.submarine.client.cli;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.cli.ParseException;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.submarine.client.cli.param.CreateModelParameters;
import org.apache.hadoop.yarn.submarine.common.MockClientContext;
import org.apache.hadoop.yarn.submarine.common.conf.SubmarineLogs;
import org.apache.hadoop.yarn.submarine.common.exception.SubmarineException;
import org.apache.hadoop.yarn.submarine.runtimes.RuntimeFactory;
import org.apache.hadoop.yarn.submarine.runtimes.common.MemorySubmarineStorage;
import org.apache.hadoop.yarn.submarine.runtimes.common.ModelSubmitter;
import org.apache.hadoop.yarn.submarine.runtimes.common.StorageKeyConstants;
import org.apache.hadoop.yarn.submarine.runtimes.common.SubmarineStorage;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

public class TestCreateModelCliParsing {

  @Before
  public void before() {
    SubmarineLogs.verboseOff();
  }

  public static MockClientContext getMockClientContext()
      throws IOException, YarnException {
    MockClientContext mockClientContext = new MockClientContext();
    ModelSubmitter modelSubmitter = mock(ModelSubmitter.class);
    when(modelSubmitter.submitModel(any(CreateModelParameters.class)))
        .thenReturn(true);

    SubmarineStorage storage = mock(SubmarineStorage.class);
    RuntimeFactory rtFactory = mock(RuntimeFactory.class);

    when(rtFactory.getSubmarineStorage()).thenReturn(storage);

    mockClientContext.setRuntimeFactory(rtFactory);
    return mockClientContext;
  }

  @Rule
  public ExpectedException expectedException = ExpectedException.none();

  @Test
  public void testPrintHelp() {
    MockClientContext mockClientContext = new MockClientContext();
    CreateModelCli createModelCli = new CreateModelCli(mockClientContext);
    createModelCli.printUsages();
  }

  @Test
  public void testCreateModel() throws InterruptedException, SubmarineException,
      YarnException, ParseException, IOException {
    CreateModelCli createModelCli = new CreateModelCli(getMockClientContext()) {
      @Override
      protected void storeAndSubmitModel() {
        // do nothing
      }
    };
    createModelCli
        .run(new String[] { "--name", "my-model", "--modelpath", "/tmp" });
    CreateModelParameters parameters = createModelCli.getParameters();
    Assert.assertEquals(parameters.getName(), "my-model");
  }

  private Map<String, String> getMockModelInfo(String modelName) {
    Map<String, String> map = new HashMap<>();
    map.put(StorageKeyConstants.MODEL_NAME, modelName);
    map.put(StorageKeyConstants.MODEL_VERSION, "default");
    map.put(StorageKeyConstants.MODEL_PATH, "/tmp");
    return map;
  }

  @Test
  public void testCreateTwoIdenticalModel() throws InterruptedException,
      SubmarineException, YarnException, ParseException, IOException {
    CreateModelCli createModelCli = new CreateModelCli(getMockClientContext());
    String modelName = "my-model";

    SubmarineStorage storage = new MemorySubmarineStorage();
    storage.addNewModel("my-model", "default", getMockModelInfo(modelName));
    
    expectedException.expect(IOException.class);

    createModelCli.run(
        new String[] { "--name", modelName, "--modelpath", "/tmp" });
  }
}
