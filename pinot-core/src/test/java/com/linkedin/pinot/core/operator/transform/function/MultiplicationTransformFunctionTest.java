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
import org.testng.Assert;
import org.testng.annotations.Test;


public class MultiplicationTransformFunctionTest extends BaseTransformFunctionTest {

  @Test
  public void testMultiplicationTransformFunction() {
    TransformExpressionTree expression = TransformExpressionTree.compileToExpressionTree(
        String.format("mult(%s,%s,%s,%s,%s)", INT_SV_COLUMN, LONG_SV_COLUMN, FLOAT_SV_COLUMN, DOUBLE_SV_COLUMN,
            STRING_SV_COLUMN));
    TransformFunction transformFunction = TransformFunctionFactory.get(expression, _dataSourceMap);
    Assert.assertTrue(transformFunction instanceof MultiplicationTransformFunction);
    Assert.assertEquals(transformFunction.getName(), MultiplicationTransformFunction.FUNCTION_NAME);
    double[] expectedValues = new double[NUM_ROWS];
    for (int i = 0; i < NUM_ROWS; i++) {
      expectedValues[i] =
          (double) _intSVValues[i] * (double) _longSVValues[i] * (double) _floatSVValues[i] * _doubleSVValues[i]
              * Double.parseDouble(_stringSVValues[i]);
    }
    testTransformFunction(transformFunction, expectedValues);

    expression = TransformExpressionTree.compileToExpressionTree(
        String.format("mult(mult(12,%s),%s,mult(mult(%s,%s),0.34,%s),%s)", STRING_SV_COLUMN, DOUBLE_SV_COLUMN,
            FLOAT_SV_COLUMN, LONG_SV_COLUMN, INT_SV_COLUMN, DOUBLE_SV_COLUMN));
    transformFunction = TransformFunctionFactory.get(expression, _dataSourceMap);
    Assert.assertTrue(transformFunction instanceof MultiplicationTransformFunction);
    for (int i = 0; i < NUM_ROWS; i++) {
      expectedValues[i] = ((12d * Double.parseDouble(_stringSVValues[i])) * _doubleSVValues[i] * (
          ((double) _floatSVValues[i] * (double) _longSVValues[i]) * 0.34 * (double) _intSVValues[i])
          * _doubleSVValues[i]);
    }
  }
}