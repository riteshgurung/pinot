/**
 * Copyright (C) 2014-2015 LinkedIn Corp. (pinot-core@linkedin.com)
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
package com.linkedin.thirdeye.hadoop;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobStatus;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.ISODateTimeFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.linkedin.thirdeye.hadoop.derivedcolumn.transformation.DerivedColumnTransformationPhaseConstants;
import com.linkedin.thirdeye.hadoop.derivedcolumn.transformation.DerivedColumnTransformationPhaseJob;
import com.linkedin.thirdeye.hadoop.push.SegmentPushPhase;
import com.linkedin.thirdeye.hadoop.push.SegmentPushPhaseConstants;
import com.linkedin.thirdeye.hadoop.segment.creation.SegmentCreationPhaseConstants;
import com.linkedin.thirdeye.hadoop.segment.creation.SegmentCreationPhaseJob;
import com.linkedin.thirdeye.hadoop.topk.TopKPhaseConstants;
import com.linkedin.thirdeye.hadoop.topk.TopKPhaseJob;

/**
 * Wrapper to manage segment create and segment push jobs for thirdeye
 */
public class ThirdEyeJob {
  private static final Logger LOGGER = LoggerFactory.getLogger(ThirdEyeJob.class);

  private static final String USAGE = "usage: phase_name job.properties";
  private static final String AVRO_SCHEMA = "schema.avsc";
  private static final String TRANSFORMATION_SCHEMA = "transformation_schema.avsc";
  private static final String TOPK_VALUES_FILE = "topk_values";
  public static final DateTimeFormatter DATE_TIME_FORMATTER = DateTimeFormat.forPattern("YYYY-MM-dd-HHmmss");

  private final String phaseName;
  private final Properties inputConfig;

  public ThirdEyeJob(String jobName, Properties config) {
    String phaseFromConfig = config.getProperty(ThirdEyeJobConstants.THIRDEYE_PHASE.getName());
    if (phaseFromConfig != null) {
      this.phaseName = phaseFromConfig;
    } else {
      this.phaseName = jobName;
    }
    this.inputConfig = config;
  }

  private enum PhaseSpec {

    TOPK {
      @Override
      Class<?> getKlazz() {
        return TopKPhaseJob.class;
      }

      @Override
      String getDescription() {
        return "Topk";
      }

      @Override
      Properties getJobProperties(Properties inputConfig, String root, String collection,
          DateTime minTime, DateTime maxTime, String inputPaths)
              throws Exception {
        Properties config = new Properties();

        config.setProperty(TopKPhaseConstants.TOPK_PHASE_SCHEMA_PATH.toString(),
            getSchemaPath(root, collection));

        config.setProperty(TopKPhaseConstants.TOPK_PHASE_INPUT_PATH.toString(),
            inputPaths);

        config.setProperty(TopKPhaseConstants.TOPK_PHASE_OUTPUT_PATH.toString(),
            getIndexDir(root, collection, minTime, maxTime) + File.separator
                + TOPK.getName());

        return config;
      }
    },
    DERIVED_COLUMN_TRANSFORMATION {
      @Override
      Class<?> getKlazz() {
        return DerivedColumnTransformationPhaseJob.class;
      }

      @Override
      String getDescription() {
        return "Adds new columns for dimensions with topk or whitelist";
      }

      @Override
      Properties getJobProperties(Properties inputConfig, String root, String collection,
          DateTime minTime, DateTime maxTime, String inputPaths)
              throws Exception {
        Properties config = new Properties();

        config.setProperty(DerivedColumnTransformationPhaseConstants.DERIVED_COLUMN_TRANSFORMATION_PHASE_INPUT_SCHEMA_PATH.toString(),
            getSchemaPath(root, collection));

        config.setProperty(DerivedColumnTransformationPhaseConstants.DERIVED_COLUMN_TRANSFORMATION_PHASE_INPUT_PATH.toString(),
            inputPaths);

        config.setProperty(DerivedColumnTransformationPhaseConstants.DERIVED_COLUMN_TRANSFORMATION_PHASE_OUTPUT_SCHEMA_PATH.toString(),
            getIndexDir(root, collection, minTime, maxTime));

        config.setProperty(DerivedColumnTransformationPhaseConstants.DERIVED_COLUMN_TRANSFORMATION_PHASE_OUTPUT_PATH.toString(),
            getIndexDir(root, collection, minTime, maxTime) + File.separator
              + DERIVED_COLUMN_TRANSFORMATION.getName());

        config.setProperty(DerivedColumnTransformationPhaseConstants.DERIVED_COLUMN_TRANSFORMATION_PHASE_TOPK_PATH.toString(),
            getIndexDir(root, collection, minTime, maxTime) + File.separator
                + TOPK.getName() + File.separator + TOPK_VALUES_FILE);

        return config;
      }
    },
    SEGMENT_CREATION {
      @Override
      Class<?> getKlazz() {
        return SegmentCreationPhaseJob.class;
      }

      @Override
      String getDescription() {
        return "Generates pinot segments";
      }

      @Override
      Properties getJobProperties(Properties inputConfig, String root, String collection,
          DateTime minTime, DateTime maxTime, String inputPaths)
              throws Exception {
        Properties config = new Properties();

        config.setProperty(SegmentCreationPhaseConstants.SEGMENT_CREATION_SCHEMA_PATH.toString(),
            getIndexDir(root, collection, minTime, maxTime) + File.separator + TRANSFORMATION_SCHEMA);
        config.setProperty(SegmentCreationPhaseConstants.SEGMENT_CREATION_INPUT_PATH.toString(),
            getIndexDir(root, collection, minTime, maxTime)
            + File.separator + DERIVED_COLUMN_TRANSFORMATION.getName());
        config.setProperty(SegmentCreationPhaseConstants.SEGMENT_CREATION_OUTPUT_PATH.toString(),
            getIndexDir(root, collection, minTime, maxTime) + File.separator + SEGMENT_CREATION.getName());
        config.setProperty(SegmentCreationPhaseConstants.SEGMENT_CREATION_WALLCLOCK_START_TIME.toString(),
            String.valueOf(minTime.getMillis()));
        config.setProperty(SegmentCreationPhaseConstants.SEGMENT_CREATION_WALLCLOCK_END_TIME.toString(),
            String.valueOf(maxTime.getMillis()));

        String schedule = inputConfig.getProperty(ThirdEyeJobConstants.THIRDEYE_FLOW_SCHEDULE.getName());
        config.setProperty(SegmentCreationPhaseConstants.SEGMENT_CREATION_SCHEDULE.toString(), schedule);
        return config;
      }
    },
    SEGMENT_PUSH {
      @Override
      Class<?> getKlazz() {
        return SegmentPushPhase.class;
      }

      @Override
      String getDescription() {
        return "Pushes pinot segments to pinot controller";
      }

      @Override
      Properties getJobProperties(Properties inputConfig, String root, String collection,
           DateTime minTime, DateTime maxTime, String inputPaths)
              throws Exception {
        Properties config = new Properties();

        config.setProperty(SegmentPushPhaseConstants.SEGMENT_PUSH_INPUT_PATH.toString(),
            getIndexDir(root, collection, minTime, maxTime) + File.separator + SEGMENT_CREATION.getName());
        config.setProperty(SegmentPushPhaseConstants.SEGMENT_PUSH_CONTROLLER_HOSTS.toString(),
            inputConfig.getProperty(ThirdEyeJobConstants.THIRDEYE_PINOT_CONTROLLER_HOSTS.getName()));
        config.setProperty(SegmentPushPhaseConstants.SEGMENT_PUSH_CONTROLLER_PORT.toString(),
            inputConfig.getProperty(ThirdEyeJobConstants.THIRDEYE_PINOT_CONTROLLER_PORT.getName()));
        return config;
      }
    };

    abstract Class<?> getKlazz();

    abstract String getDescription();

    abstract Properties getJobProperties(Properties inputConfig, String root, String collection,
        DateTime minTime, DateTime maxTime, String inputPaths) throws Exception;

    String getName() {
      return this.name().toLowerCase();
    }

    String getIndexDir(String root, String collection, DateTime minTime,
        DateTime maxTime) throws IOException {
      return getCollectionDir(root, collection) + File.separator
          + "data_" + DATE_TIME_FORMATTER.print(minTime) + "_"
          + DATE_TIME_FORMATTER.print(maxTime);
    }

    String getSchemaPath(String root, String collection) {
      return getCollectionDir(root, collection) + File.separator + AVRO_SCHEMA;
    }

  }

  private static void usage() {
    System.err.println(USAGE);
    for (PhaseSpec phase : PhaseSpec.values()) {
      System.err.printf("%-30s : %s\n", phase.getName(), phase.getDescription());
    }
  }

  private static String getAndCheck(String name, Properties properties) {
    String value = properties.getProperty(name);
    if (value == null) {
      throw new IllegalArgumentException("Must provide " + name);
    }
    return value;
  }


  private static String getCollectionDir(String root, String collection) {
    return root == null ? collection : root + File.separator + collection;
  }

  private void setMapreduceConfig(Configuration configuration) {
    String mapreduceConfig =
        inputConfig.getProperty(ThirdEyeJobConstants.THIRDEYE_MR_CONF.getName());
    if (mapreduceConfig != null && !mapreduceConfig.isEmpty()) {
      String[] options = mapreduceConfig.split(",");
      for (String option : options) {
        String[] configs = option.split("=", 2);
        if (configs.length == 2) {
          LOGGER.info("Setting job configuration {} to {}", configs[0], configs[1]);
          configuration.set(configs[0], configs[1]);
        }
      }
    }
  }

  @SuppressWarnings("unchecked")
  public void run() throws Exception {
    LOGGER.info("Input config:{}", inputConfig);
    PhaseSpec phaseSpec;
    try {
      phaseSpec = PhaseSpec.valueOf(phaseName.toUpperCase());
    } catch (Exception e) {
      usage();
      throw e;
    }

    // Get root, collection, input paths
    String root = getAndCheck(ThirdEyeJobConstants.THIRDEYE_ROOT.getName(), inputConfig);
    String collection =
        getAndCheck(ThirdEyeJobConstants.THIRDEYE_COLLECTION.getName(), inputConfig);
    String inputPaths = getAndCheck(ThirdEyeJobConstants.INPUT_PATHS.getName(), inputConfig);

    // Get min / max time
    DateTime minTime;
    DateTime maxTime;

    String minTimeProp = inputConfig.getProperty(ThirdEyeJobConstants.THIRDEYE_TIME_MIN.getName());
    String maxTimeProp = inputConfig.getProperty(ThirdEyeJobConstants.THIRDEYE_TIME_MAX.getName());

    minTime = ISODateTimeFormat.dateTimeParser().parseDateTime(minTimeProp);
    maxTime = ISODateTimeFormat.dateTimeParser().parseDateTime(maxTimeProp);

    Properties jobProperties = phaseSpec.getJobProperties(inputConfig, root, collection,
        minTime, maxTime, inputPaths);
    for (Object key : inputConfig.keySet()) {
      jobProperties.setProperty(key.toString(), inputConfig.getProperty(key.toString()));
    }

    // Instantiate the job
    Constructor<Configured> constructor = (Constructor<Configured>) phaseSpec.getKlazz()
        .getConstructor(String.class, Properties.class);
    Configured instance = constructor.newInstance(phaseSpec.getName(), jobProperties);
    setMapreduceConfig(instance.getConf());

    // Run the job
    Method runMethod = instance.getClass().getMethod("run");
    Job job = (Job) runMethod.invoke(instance);
    if (job != null) {
      JobStatus status = job.getStatus();
      if (status.getState() != JobStatus.State.SUCCEEDED) {
        throw new RuntimeException(
            "Job " + job.getJobName() + " failed to execute: Ran with config:" + jobProperties);
      }
    }
  }

  public static void main(String[] args) throws Exception {
    if (args.length != 2) {
      usage();
      System.exit(1);
    }

    String phaseName = args[0];
    Properties config = new Properties();
    config.load(new FileInputStream(args[1]));
    new ThirdEyeJob(phaseName, config).run();
  }
}