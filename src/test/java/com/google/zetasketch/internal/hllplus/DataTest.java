/*
 * Copyright 2019 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.google.zetasketch.internal.hllplus;

import static com.google.common.truth.Truth.assertThat;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Unit tests for {@link Data}. */
@RunWith(JUnit4.class)
@SuppressWarnings("boxing")
public class DataTest {

  /** Absolute difference to tolerate when comparing floating point numbers below. */
  private static final double TOLERANCE = 0.0001;

  @Test
  public void alpha() {
    assertThat(Data.alpha(14)).isWithin(TOLERANCE).of(0.7213);
  }

  @Test
  public void estimateBias_WhenExactlyDefined() {
    // Defined exactly in bias tables.
    assertThat(Data.estimateBias(738.1256, 10)).isWithin(TOLERANCE).of(737.1256);
    assertThat(Data.estimateBias(14573.7784, 14)).isWithin(TOLERANCE).of(9248.7784);
  }

  @Test
  public void estimateBias_WhenInterpolationNeeded() {
    // Interpolated results with values computed by hand starting with the empirical data tables
    // from the HLL++ paper (https://goo.gl/pc916Z). For example, for the first value we get:
    //
    //   means   = [738.1256, 750.4234, 763.1064, 775.4732, 788.4636, 801.0644]
    //   biases  = [737.1256, 724.4234, 711.1064, 698.4732, 685.4636, 673.0644]
    //   weights = [1.0/(estimate - mean)^2 for mean in means]
    //   bias    = sum(w*b for (w, b) in zip(weights, biases)) / sum(weights)
    //           = 736.4957464911646
    //
    // We test interpolations on the left, in the center and on the right side of the bias tables.
    assertThat(Data.estimateBias(1490, 11)).isWithin(TOLERANCE).of(1456.8144);
    assertThat(Data.estimateBias(16300, 14)).isWithin(TOLERANCE).of(8005.2257);
    assertThat(Data.estimateBias(653000, 17)).isWithin(TOLERANCE).of(-411.7805);
  }

  /**
   * Tests that the bias is zero when the bias table defines values for the given precision but the
   * estimate is off the left or right side of the defined values.
   */
  @Test
  public void estimateBias_ReturnsZeroWhenMeanOutOfRange() {
    assertThat(Data.estimateBias(738, 10)).isWithin(TOLERANCE).of(0);
    assertThat(Data.estimateBias(1310000, 18)).isWithin(TOLERANCE).of(0);
  }

  @Test
  public void estimateBias_ReturnsZeroWhenPrecisionOutOfRange() {
    assertThat(Data.estimateBias(1000, Data.MINIMUM_PRECISION - 1)).isWithin(TOLERANCE).of(0);
    assertThat(Data.estimateBias(1000, Data.MAXIMUM_PRECISION + 1)).isWithin(TOLERANCE).of(0);
  }

  @Test
  public void linearCountingThreshold_WhenPreciselyDefined() {
    assertThat(Data.linearCountingThreshold(14)).isEqualTo(11500);
  }

  @Test
  public void linearCountingThreshold_WhenComputed() {
    assertThat(Data.linearCountingThreshold(19)).isEqualTo(1310720);
  }
}
