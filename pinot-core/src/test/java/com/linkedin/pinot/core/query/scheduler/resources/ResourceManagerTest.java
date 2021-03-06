/**
 * Copyright (C) 2014-2016 LinkedIn Corp. (pinot-core@linkedin.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.linkedin.pinot.core.query.scheduler.resources;

import com.linkedin.pinot.common.query.ServerQueryRequest;
import com.linkedin.pinot.core.query.scheduler.SchedulerGroupAccountant;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.testng.annotations.Test;

import static org.mockito.Mockito.*;
import static org.testng.Assert.*;


public class ResourceManagerTest {

  @Test
  public void testCanSchedule() throws Exception {
    ResourceManager rm = getResourceManager(2, 5, 1, 3);

    SchedulerGroupAccountant accountant = mock(SchedulerGroupAccountant.class);
    when(accountant.totalReservedThreads()).thenReturn(3);
    assertFalse(rm.canSchedule(accountant));

    when(accountant.totalReservedThreads()).thenReturn(2);
    assertTrue(rm.canSchedule(accountant));
  }

  private ResourceManager getResourceManager(int runners, int workers, final int softLimit, final int hardLimit) {

    return new ResourceManager(getConfig(runners, workers)) {

      @Override
      public QueryExecutorService getExecutorService(ServerQueryRequest query, SchedulerGroupAccountant accountant) {
        return new QueryExecutorService() {
          @Override
          public void execute(Runnable command) {
            getQueryWorkers().execute(command);
          }
        };
      }

      @Override
      public int getTableThreadsHardLimit() {
        return hardLimit;
      }

      @Override
      public int getTableThreadsSoftLimit() {
        return softLimit;
      }
    };
  }
  private Configuration getConfig(int runners, int workers) {
    Configuration config = new PropertiesConfiguration();
    config.setProperty(ResourceManager.QUERY_RUNNER_CONFIG_KEY, runners);
    config.setProperty(ResourceManager.QUERY_WORKER_CONFIG_KEY, workers);
    return config;
  }
}
