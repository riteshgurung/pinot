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
package com.linkedin.pinot.core.operator.transform.function;

import com.linkedin.pinot.common.request.transform.TransformExpressionTree;
import com.linkedin.pinot.core.common.DataSource;
import com.linkedin.pinot.core.common.DataSourceMetadata;
import com.linkedin.pinot.core.operator.blocks.ProjectionBlock;
import com.linkedin.pinot.core.operator.transform.transformer.timeunit.TimeUnitTransformer;
import com.linkedin.pinot.core.operator.transform.transformer.timeunit.TimeUnitTransformerFactory;
import com.linkedin.pinot.core.plan.DocIdSetPlanNode;
import com.linkedin.pinot.core.query.exception.BadQueryRequestException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nonnull;


/**
 * The <code>TimeConversionTransformFunction</code> class implements the time conversion transform function.
 * <ul>
 *   <li>
 *     Inputs:
 *     <ul>
 *       <li>Input time values</li>
 *       <li>Input time unit as defined in {@link TimeUnit}</li>
 *       <li>Output time unit as defined in {@link TimeUnit}</li>
 *     </ul>
 *   </li>
 *   <li>
 *     Outputs:
 *     <ul>
 *       <li>Time values converted into desired time unit</li>
 *     </ul>
 *   </li>
 * </ul>
 */
public class TimeConversionTransformFunction extends BaseTransformFunction {
  public static final String FUNCTION_NAME = "timeConvert";

  private final long[] _outputTimes = new long[DocIdSetPlanNode.MAX_DOC_PER_CALL];

  private TransformFunction _mainTransformFunction;
  private TimeUnitTransformer _timeUnitTransformer;

  @Override
  public String getName() {
    return FUNCTION_NAME;
  }

  @Override
  public void init(@Nonnull TransformExpressionTree expression, @Nonnull Map<String, DataSource> dataSourceMap) {
    List<TransformExpressionTree> arguments = expression.getChildren();

    // Check that there are exactly 3 arguments
    if (arguments.size() != 3) {
      throw new BadQueryRequestException("Exactly 3 arguments are required for TIME_CONVERT transform function");
    }

    _mainTransformFunction = TransformFunctionFactory.get(arguments.get(0), dataSourceMap);
    TimeUnit inputTimeUnit = TimeUnit.valueOf(arguments.get(1).getValue().toUpperCase());
    _timeUnitTransformer =
        TimeUnitTransformerFactory.getTimeUnitTransformer(inputTimeUnit, arguments.get(2).getValue());
  }

  @Override
  public DataSourceMetadata getResultMetadata() {
    return LONG_SV_NO_DICTIONARY_METADATA;
  }

  @Override
  public long[] transformToLongValuesSV(@Nonnull ProjectionBlock projectionBlock) {
    _timeUnitTransformer.transform(_mainTransformFunction.transformToLongValuesSV(projectionBlock), _outputTimes,
        projectionBlock.getNumDocs());
    return _outputTimes;
  }
}
