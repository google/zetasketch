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

package com.google.zetasketch;

import static com.google.common.truth.Truth.assertThat;

import com.google.protos.zetasketch.Aggregator.AggregatorStateProto;
import com.google.protos.zetasketch.HllplusUnique;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests that exercise the {@link HyperLogLogPlusPlus} merging logic. */
@RunWith(JUnit4.class)
@SuppressWarnings("boxing") // We do not care about boxing performance in tests.
public class HyperLogLogPlusPlusMergeTest {

  @Test
  public void merge_MultipleSparseRepresentationsIntoANormalOne() {
    int normalPrecision = 13;
    HyperLogLogPlusPlus.Builder hllBuilder =
        new HyperLogLogPlusPlus.Builder().normalPrecision(normalPrecision).sparsePrecision(16);

    int numSketches = 100;
    Random random = new Random(123);

    // AggregatorStateProtos to be merged; each gets a subset of values.
    List<AggregatorStateProto> aggStateProtos = new ArrayList<>();
    // The merge result should be equal to this aggregator, to which all values are added.
    HyperLogLogPlusPlus<Long> overallAggregator = hllBuilder.buildForLongs();

    for (int i = 0; i < numSketches; i++) {
      int numValues = random.nextInt((1 << normalPrecision) / 2) + 1;
      HyperLogLogPlusPlus<Long> aggregator = hllBuilder.buildForLongs();

      for (int k = 0; k < numValues; k++) {
        long value = random.nextLong();
        aggregator.add(value);
        overallAggregator.add(value);
      }
      AggregatorStateProto proto = aggregator.serializeToProto();
      assertThat(proto.getExtension(HllplusUnique.hyperloglogplusUniqueState).hasSparseData())
          .isTrue();
      aggStateProtos.add(proto);
    }

    AggregatorStateProto expectedProto = overallAggregator.serializeToProto();
    assertThat(expectedProto.getExtension(HllplusUnique.hyperloglogplusUniqueState).hasSparseData())
        .isFalse();

    HyperLogLogPlusPlus<?> aggregator = HyperLogLogPlusPlus.forProto(aggStateProtos.get(0));
    for (AggregatorStateProto aggProto : aggStateProtos.subList(1, aggStateProtos.size())) {
      aggregator.merge(aggProto);
    }
    assertThat(aggregator.serializeToProto()).isEqualTo(expectedProto);
  }
}
