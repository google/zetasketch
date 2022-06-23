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
import static com.google.common.truth.extensions.proto.ProtoTruth.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import com.google.common.collect.Range;
import com.google.protobuf.ByteString;
import com.google.protobuf.ExtensionRegistry;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protos.zetasketch.Aggregator.AggregatorStateProto;
import com.google.protos.zetasketch.Aggregator.AggregatorType;
import com.google.protos.zetasketch.Aggregator.DefaultOpsType;
import com.google.protos.zetasketch.HllplusUnique;
import com.google.protos.zetasketch.HllplusUnique.HyperLogLogPlusUniqueStateProto;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Basic unit tests for {@link HyperLogLogPlusPlus}. More extensive testing is provided by the
 * golden tests. In particular, we don't test the encoding correctness here.
 */
@RunWith(JUnit4.class)
public class HyperLogLogPlusPlusTest {

  private static final HyperLogLogPlusPlus.Builder hllBuilder =
      new HyperLogLogPlusPlus.Builder().sparsePrecision(25);

  /** Empty aggregator state of type {@link DefaultOpsType.Id#BYTES_OR_UTF8_STRING}. */
  private static final AggregatorStateProto BYTE_OR_STRING_TYPE_STATE_PROTO =
      ValueType.forStandardType(DefaultOpsType.Id.BYTES_OR_UTF8_STRING)
          .copyToBuilder(
              AggregatorStateProto.newBuilder()
                  .setType(AggregatorType.HYPERLOGLOG_PLUS_UNIQUE)
                  .setEncodingVersion(2)
                  .setNumValues(0)
                  .setExtension(
                      HllplusUnique.hyperloglogplusUniqueState,
                      HyperLogLogPlusUniqueStateProto.newBuilder()
                          .setPrecisionOrNumBuckets(15)
                          .setSparsePrecisionOrNumBuckets(25)
                          .build()))
          .build();

  /** Empty aggregator state of type {@link DefaultOpsType.Id#UNKNOWN}. */
  private static final AggregatorStateProto UNKNOWN_TYPE_STATE_PROTO =
      AggregatorStateProto.newBuilder()
          .setType(AggregatorType.HYPERLOGLOG_PLUS_UNIQUE)
          .setEncodingVersion(2)
          .setNumValues(0)
          .setExtension(
              HllplusUnique.hyperloglogplusUniqueState,
              HyperLogLogPlusUniqueStateProto.newBuilder()
                  .setPrecisionOrNumBuckets(15)
                  .setSparsePrecisionOrNumBuckets(25)
                  .build())
          .build();

  private static final ExtensionRegistry EXTENSIONS;

  static {
    ExtensionRegistry registry = ExtensionRegistry.newInstance();
    HllplusUnique.registerAllExtensions(registry);
    EXTENSIONS = registry.getUnmodifiable();
  }

  @Test
  public void addBytes() {
    HyperLogLogPlusPlus<ByteString> aggregator = hllBuilder.buildForBytes();

    aggregator.add(new byte[] {12});

    assertEquals(1, aggregator.longResult());
    assertEquals(1, aggregator.numValues());
  }

  @Test
  public void addBytes_ThrowsWhenOtherType() {
    HyperLogLogPlusPlus<Long> aggregator = hllBuilder.buildForLongs();

    IllegalArgumentException e =
        assertThrows(IllegalArgumentException.class, () -> aggregator.add(new byte[] {12}));
    assertThat(e)
        .hasMessageThat()
        .isEqualTo("unable to add type BYTES to aggregator of type [LONG]");
  }

  @Test
  @SuppressWarnings("unchecked")
  public void addBytes_ToByteOrStringType() {
    HyperLogLogPlusPlus<?> aggregator =
        HyperLogLogPlusPlus.forProto(BYTE_OR_STRING_TYPE_STATE_PROTO);

    // First add sets the type.
    aggregator.add(new byte[] {12});

    // Second add with a different type is no longer possible.
    IllegalArgumentException e =
        assertThrows(
            IllegalArgumentException.class,
            () -> ((HyperLogLogPlusPlus<String>) aggregator).add("foo"));
    assertThat(e)
        .hasMessageThat()
        .isEqualTo("unable to add type STRING to aggregator of type [BYTES]");
  }

  @Test
  public void addBytes_ToUninitalized() {
    HyperLogLogPlusPlus<?> aggregator = HyperLogLogPlusPlus.forProto(UNKNOWN_TYPE_STATE_PROTO);

    // First add sets the type.
    aggregator.add(new byte[] {12});

    // Second add with a different type is no longer possible.
    IllegalArgumentException e =
        assertThrows(IllegalArgumentException.class, () -> aggregator.add(42L));
    assertThat(e)
        .hasMessageThat()
        .isEqualTo("unable to add type LONG to aggregator of type [BYTES]");
  }

  @Test
  public void addInteger() {
    HyperLogLogPlusPlus<Integer> aggregator = hllBuilder.buildForIntegers();

    aggregator.add(1);

    assertEquals(1, aggregator.longResult());
    assertEquals(1, aggregator.numValues());
  }

  @Test
  public void addInteger_ThrowsWhenOtherType() {
    HyperLogLogPlusPlus<Long> aggregator = hllBuilder.buildForLongs();

    IllegalArgumentException e =
        assertThrows(IllegalArgumentException.class, () -> aggregator.add(1));
    assertThat(e)
        .hasMessageThat()
        .isEqualTo("unable to add type INTEGER to aggregator of type [LONG]");
  }

  @Test
  public void addInteger_ToUninitalized() {
    HyperLogLogPlusPlus<?> aggregator = HyperLogLogPlusPlus.forProto(UNKNOWN_TYPE_STATE_PROTO);

    // First add sets the type.
    aggregator.add(42);

    // Second add with a different type is no longer possible.
    IllegalArgumentException e =
        assertThrows(IllegalArgumentException.class, () -> aggregator.add(42L));
    assertThat(e)
        .hasMessageThat()
        .isEqualTo("unable to add type LONG to aggregator of type [INTEGER]");
  }

  @Test
  public void addLong() {
    HyperLogLogPlusPlus<Long> aggregator = hllBuilder.buildForLongs();

    aggregator.add(1L);

    assertEquals(1, aggregator.longResult());
    assertEquals(1, aggregator.numValues());
  }

  @Test
  public void addLong_ThrowsWhenOtherType() {
    HyperLogLogPlusPlus<Integer> aggregator = hllBuilder.buildForIntegers();

    IllegalArgumentException e =
        assertThrows(IllegalArgumentException.class, () -> aggregator.add(1L));
    assertThat(e)
        .hasMessageThat()
        .isEqualTo("unable to add type LONG to aggregator of type [INTEGER]");
  }

  @Test
  public void addLong_ToUninitalized() {
    HyperLogLogPlusPlus<?> aggregator = HyperLogLogPlusPlus.forProto(UNKNOWN_TYPE_STATE_PROTO);

    // First add sets the type.
    aggregator.add(42L);

    // Second add with a different type is no longer possible.
    IllegalArgumentException e =
        assertThrows(IllegalArgumentException.class, () -> aggregator.add(42));
    assertThat(e)
        .hasMessageThat()
        .isEqualTo("unable to add type INTEGER to aggregator of type [LONG]");
  }

  @Test
  public void addObject_Bytes() {
    HyperLogLogPlusPlus<ByteString> aggregator = hllBuilder.buildForBytes();

    aggregator.add(ByteString.copyFrom(new byte[] {12}));

    assertEquals(1, aggregator.longResult());
    assertEquals(1, aggregator.numValues());
  }

  @Test
  public void addObject_Integer() {
    HyperLogLogPlusPlus<Integer> aggregator = hllBuilder.buildForIntegers();

    aggregator.add(Integer.valueOf(1));

    assertEquals(1, aggregator.longResult());
    assertEquals(1, aggregator.numValues());
  }

  @Test
  public void addObject_Long() {
    HyperLogLogPlusPlus<Long> aggregator = hllBuilder.buildForLongs();

    aggregator.add(Long.valueOf(1L));

    assertEquals(1, aggregator.longResult());
    assertEquals(1, aggregator.numValues());
  }

  @Test
  @SuppressWarnings({"unchecked", "rawtypes"})
  public void addObject_ThrowsOnWrongObject() {
    HyperLogLogPlusPlus aggregator = hllBuilder.buildForStrings();

    IllegalArgumentException e =
        assertThrows(IllegalArgumentException.class, () -> aggregator.add(new Object()));
    assertThat(e)
        .hasMessageThat()
        .isEqualTo("unable to add class java.lang.Object to aggregator of type [STRING, BYTES]");
  }

  @Test
  public void addString() {
    HyperLogLogPlusPlus<String> aggregator = hllBuilder.buildForStrings();

    aggregator.add("foo");

    assertEquals(1, aggregator.longResult());
    assertEquals(1, aggregator.numValues());
  }

  @Test
  @SuppressWarnings("unchecked")
  public void addString_ToByteOrStringType() {
    HyperLogLogPlusPlus<?> aggregator =
        HyperLogLogPlusPlus.forProto(BYTE_OR_STRING_TYPE_STATE_PROTO);

    // First add sets the type.
    ((HyperLogLogPlusPlus<String>) aggregator).add("foo");

    // Second add with a different type is no longer possible.
    IllegalArgumentException e =
        assertThrows(IllegalArgumentException.class, () -> aggregator.add(new byte[] {1}));
    assertThat(e)
        .hasMessageThat()
        .isEqualTo("unable to add type BYTES to aggregator of type [STRING]");
  }

  @Test
  public void addString_ToUninitalized() {
    @SuppressWarnings("unchecked")
    HyperLogLogPlusPlus<String> aggregator =
        (HyperLogLogPlusPlus<String>) HyperLogLogPlusPlus.forProto(UNKNOWN_TYPE_STATE_PROTO);

    // First add sets the type.
    aggregator.add("foo");

    // Second add with a different type is no longer possible.
    IllegalArgumentException e =
        assertThrows(IllegalArgumentException.class, () -> aggregator.add(42));
    assertThat(e)
        .hasMessageThat()
        .isEqualTo("unable to add type INTEGER to aggregator of type [STRING]");
  }

  @Test
  public void create_ThrowsWhenPrecisionTooLarge() {
    // Assuming this holds also for buildForLongs, buildForStrings, etc..
    IllegalArgumentException e =
        assertThrows(
            IllegalArgumentException.class,
            () ->
                new HyperLogLogPlusPlus.Builder()
                    .normalPrecision(HyperLogLogPlusPlus.MAXIMUM_PRECISION + 1)
                    .sparsePrecision(25)
                    .buildForIntegers());
    assertThat(e)
        .hasMessageThat()
        .isEqualTo("Expected normal precision to be >= 4 and <= 24 but was 25");
  }

  @Test
  public void create_ThrowsWhenPrecisionTooSmall() {
    // Assuming this holds also for buildForLongs, buildForStrings, etc..
    IllegalArgumentException e =
        assertThrows(
            IllegalArgumentException.class,
            () ->
                new HyperLogLogPlusPlus.Builder()
                    .normalPrecision(HyperLogLogPlusPlus.MINIMUM_PRECISION - 1)
                    .sparsePrecision(25)
                    .buildForIntegers());
    assertThat(e)
        .hasMessageThat()
        .isEqualTo("Expected normal precision to be >= 4 and <= 24 but was 3");
  }

  @Test
  public void fromProto_ThrowsWhenNoExtension() {
    IllegalArgumentException e =
        assertThrows(
            IllegalArgumentException.class,
            () ->
                HyperLogLogPlusPlus.forProto(
                    AggregatorStateProto.newBuilder()
                        .setType(AggregatorType.HYPERLOGLOG_PLUS_UNIQUE)
                        .setEncodingVersion(2)
                        .setNumValues(0)
                        .build()));
    assertThat(e)
        .hasMessageThat()
        .isEqualTo("Expected normal precision to be >= 4 and <= 24 but was 0");
  }

  @Test
  public void fromProto_ThrowsWhenNormalPrecisionTooLarge() {
    IllegalArgumentException e =
        assertThrows(
            IllegalArgumentException.class,
            () ->
                HyperLogLogPlusPlus.forProto(
                    ValueType.forStandardType(DefaultOpsType.Id.UINT32)
                        .copyToBuilder(
                            AggregatorStateProto.newBuilder()
                                .setType(AggregatorType.HYPERLOGLOG_PLUS_UNIQUE)
                                .setEncodingVersion(2)
                                .setExtension(
                                    HllplusUnique.hyperloglogplusUniqueState,
                                    HyperLogLogPlusUniqueStateProto.newBuilder()
                                        .setPrecisionOrNumBuckets(25)
                                        .build())
                                .setNumValues(0))
                        .build()));
    assertThat(e)
        .hasMessageThat()
        .isEqualTo("Expected normal precision to be >= 4 and <= 24 but was 25");
  }

  @Test
  public void fromProto_ThrowsWhenNormalPrecisionTooSmall() {
    IllegalArgumentException e =
        assertThrows(
            IllegalArgumentException.class,
            () ->
                HyperLogLogPlusPlus.forProto(
                    ValueType.forStandardType(DefaultOpsType.Id.UINT32)
                        .copyToBuilder(
                            AggregatorStateProto.newBuilder()
                                .setType(AggregatorType.HYPERLOGLOG_PLUS_UNIQUE)
                                .setEncodingVersion(2)
                                .setExtension(
                                    HllplusUnique.hyperloglogplusUniqueState,
                                    HyperLogLogPlusUniqueStateProto.newBuilder()
                                        .setPrecisionOrNumBuckets(3)
                                        .build())
                                .setNumValues(0))
                        .build()));
    assertThat(e)
        .hasMessageThat()
        .isEqualTo("Expected normal precision to be >= 4 and <= 24 but was 3");
  }

  @Test
  public void fromProto_ThrowsWhenNotHyperLogLogPlusPlus() {
    IllegalArgumentException e =
        assertThrows(
            IllegalArgumentException.class,
            () ->
                HyperLogLogPlusPlus.forProto(
                    AggregatorStateProto.newBuilder()
                        .setType(AggregatorType.SUM)
                        .setNumValues(0)
                        .build()));
    assertThat(e)
        .hasMessageThat()
        .isEqualTo("Expected proto to be of type HYPERLOGLOG_PLUS_UNIQUE but was SUM");
  }

  @Test
  public void fromProto_ThrowsWhenSparseIsMissingSparsePrecision() {
    IllegalArgumentException e =
        assertThrows(
            IllegalArgumentException.class,
            () ->
                HyperLogLogPlusPlus.forProto(
                    ValueType.forStandardType(DefaultOpsType.Id.UINT32)
                        .copyToBuilder(
                            AggregatorStateProto.newBuilder()
                                .setType(AggregatorType.HYPERLOGLOG_PLUS_UNIQUE)
                                .setEncodingVersion(2)
                                .setExtension(
                                    HllplusUnique.hyperloglogplusUniqueState,
                                    HyperLogLogPlusUniqueStateProto.newBuilder()
                                        .setPrecisionOrNumBuckets(15)
                                        .setSparsePrecisionOrNumBuckets(0)
                                        .setSparseData(ByteString.copyFrom(new byte[] {1}))
                                        .build())
                                .setNumValues(0))
                        .build()));
    assertThat(e)
        .hasMessageThat()
        .isEqualTo("Must have a sparse precision when sparse data is set");
  }

  @Test
  public void fromProto_ThrowsWhenSparsePrecisionTooLarge() {
    IllegalArgumentException e =
        assertThrows(
            IllegalArgumentException.class,
            () ->
                HyperLogLogPlusPlus.forProto(
                    ValueType.forStandardType(DefaultOpsType.Id.UINT32)
                        .copyToBuilder(
                            AggregatorStateProto.newBuilder()
                                .setType(AggregatorType.HYPERLOGLOG_PLUS_UNIQUE)
                                .setEncodingVersion(2)
                                .setExtension(
                                    HllplusUnique.hyperloglogplusUniqueState,
                                    HyperLogLogPlusUniqueStateProto.newBuilder()
                                        .setPrecisionOrNumBuckets(15)
                                        .setSparsePrecisionOrNumBuckets(26)
                                        .build())
                                .setNumValues(0))
                        .build()));
    assertThat(e).hasMessageThat().isEqualTo(
        "Expected sparse precision to be >= normal precision (15) and <= 25 but was 26.");
  }

  @Test
  public void fromProto_ThrowsWhenSparsePrecisionTooSmall() {
    IllegalArgumentException e =
        assertThrows(
            IllegalArgumentException.class,
            () ->
                HyperLogLogPlusPlus.forProto(
                    ValueType.forStandardType(DefaultOpsType.Id.UINT32)
                        .copyToBuilder(
                            AggregatorStateProto.newBuilder()
                                .setType(AggregatorType.HYPERLOGLOG_PLUS_UNIQUE)
                                .setEncodingVersion(2)
                                .setExtension(
                                    HllplusUnique.hyperloglogplusUniqueState,
                                    HyperLogLogPlusUniqueStateProto.newBuilder()
                                        .setPrecisionOrNumBuckets(15)
                                        .setSparsePrecisionOrNumBuckets(14)
                                        .build())
                                .setNumValues(0))
                        .build()));
    assertThat(e).hasMessageThat().isEqualTo(
        "Expected sparse precision to be >= normal precision (15) and <= 25 but was 14.");
  }

  @Test
  public void fromProto_WhenNormal() {
    HyperLogLogPlusPlus<?> aggregator =
        HyperLogLogPlusPlus.forProto(
            ValueType.forStandardType(DefaultOpsType.Id.UINT32)
                .copyToBuilder(
                    AggregatorStateProto.newBuilder()
                        .setType(AggregatorType.HYPERLOGLOG_PLUS_UNIQUE)
                        .setEncodingVersion(2)
                        .setExtension(
                            HllplusUnique.hyperloglogplusUniqueState,
                            HyperLogLogPlusUniqueStateProto.newBuilder()
                                .setPrecisionOrNumBuckets(15)
                                .setData(ByteString.copyFrom(new byte[1 << 15]))
                                .build())
                        .setNumValues(1))
                .build());

    assertEquals(0, aggregator.longResult());
    assertEquals(1, aggregator.numValues());
  }

  @Test
  public void fromProto_WhenSparse() {
    HyperLogLogPlusPlus<?> aggregator =
        HyperLogLogPlusPlus.forProto(
            ValueType.forStandardType(DefaultOpsType.Id.UINT32)
                .copyToBuilder(
                    AggregatorStateProto.newBuilder()
                        .setType(AggregatorType.HYPERLOGLOG_PLUS_UNIQUE)
                        .setEncodingVersion(2)
                        .setExtension(
                            HllplusUnique.hyperloglogplusUniqueState,
                            HyperLogLogPlusUniqueStateProto.newBuilder()
                                .setPrecisionOrNumBuckets(15)
                                .setSparsePrecisionOrNumBuckets(25)
                                .setSparseData(ByteString.copyFrom(new byte[] {1}))
                                .setSparseSize(1)
                                .build())
                        .setNumValues(2))
                .build());

    assertEquals(1, aggregator.longResult());
    assertEquals(2, aggregator.numValues());
  }

  @Test
  public void fromProtoByteString() {
    ByteString data =
        ValueType.forStandardType(DefaultOpsType.Id.UINT32)
            .copyToBuilder(
                AggregatorStateProto.newBuilder()
                    .setType(AggregatorType.HYPERLOGLOG_PLUS_UNIQUE)
                    .setEncodingVersion(2)
                    .setExtension(
                        HllplusUnique.hyperloglogplusUniqueState,
                        HyperLogLogPlusUniqueStateProto.newBuilder()
                            .setPrecisionOrNumBuckets(15)
                            .setSparsePrecisionOrNumBuckets(25)
                            .setSparseData(ByteString.copyFrom(new byte[] {1}))
                            .setSparseSize(1)
                            .build())
                    .setNumValues(2))
            .build()
            .toByteString();

    HyperLogLogPlusPlus<?> aggregator = HyperLogLogPlusPlus.forProto(data);
    assertEquals(1, aggregator.longResult());
    assertEquals(2, aggregator.numValues());
  }

  @Test
  public void fromProtoByteString_ThrowsWhenInvalid() {
    IllegalArgumentException e =
        assertThrows(
            IllegalArgumentException.class,
            () -> HyperLogLogPlusPlus.forProto(ByteString.copyFrom(new byte[] {1})));
    assertThat(e).hasCauseThat().isInstanceOf(InvalidProtocolBufferException.class);
  }

  @Test
  public void fromProtoBytes() {
    byte[] data =
        ValueType.forStandardType(DefaultOpsType.Id.UINT32)
            .copyToBuilder(
                AggregatorStateProto.newBuilder()
                    .setType(AggregatorType.HYPERLOGLOG_PLUS_UNIQUE)
                    .setEncodingVersion(2)
                    .setExtension(
                        HllplusUnique.hyperloglogplusUniqueState,
                        HyperLogLogPlusUniqueStateProto.newBuilder()
                            .setPrecisionOrNumBuckets(15)
                            .setSparsePrecisionOrNumBuckets(25)
                            .setSparseData(ByteString.copyFrom(new byte[] {1}))
                            .setSparseSize(1)
                            .build())
                    .setNumValues(2))
            .build()
            .toByteArray();

    HyperLogLogPlusPlus<?> aggregator = HyperLogLogPlusPlus.forProto(data);
    assertEquals(1, aggregator.longResult());
    assertEquals(2, aggregator.numValues());
  }

  @Test
  public void fromProtoBytes_ThrowsWhenInvalid() {
    IllegalArgumentException e =
        assertThrows(
            IllegalArgumentException.class, () -> HyperLogLogPlusPlus.forProto(new byte[] {1}));
    assertThat(e).hasCauseThat().isInstanceOf(InvalidProtocolBufferException.class);
  }

  @Test
  public void longResult() {
    HyperLogLogPlusPlus<Integer> aggregator = hllBuilder.buildForIntegers();

    aggregator.add(1);
    aggregator.add(2);
    aggregator.add(3);
    aggregator.add(2);
    aggregator.add(3);

    assertEquals(3, aggregator.longResult());
  }

  @Test
  public void longResult_ZeroWhenEmpty() {
    HyperLogLogPlusPlus<Integer> aggregator = hllBuilder.buildForIntegers();
    assertEquals(0, aggregator.longResult());
  }

  @Test
  public void merge_FromProto() {
    HyperLogLogPlusPlus<Integer> aggregator = hllBuilder.buildForIntegers();

    AggregatorStateProto proto =
        ValueType.forStandardType(DefaultOpsType.Id.UINT32)
            .copyToBuilder(
                AggregatorStateProto.newBuilder()
                    .setType(AggregatorType.HYPERLOGLOG_PLUS_UNIQUE)
                    .setEncodingVersion(2)
                    .setExtension(
                        HllplusUnique.hyperloglogplusUniqueState,
                        HyperLogLogPlusUniqueStateProto.newBuilder()
                            .setPrecisionOrNumBuckets(15)
                            .setSparsePrecisionOrNumBuckets(25)
                            .setSparseData(ByteString.copyFrom(new byte[] {1}))
                            .setSparseSize(1)
                            .build())
                    .setNumValues(2))
            .build();

    aggregator.merge(proto);

    assertEquals(1, aggregator.longResult());
    assertEquals(2, aggregator.numValues());
  }

  /**
   * Tests that the aggregator checks for incompatible types during a {@code merge} operation. This
   * is a bit of a pathological case as it requires an unchecked cast on behalf of the client.
   */
  @Test
  public void merge_IncompatibleTypes() {
    HyperLogLogPlusPlus<Integer> a = hllBuilder.buildForIntegers();

    @SuppressWarnings("unchecked")
    HyperLogLogPlusPlus<Integer> b =
        (HyperLogLogPlusPlus<Integer>)
            HyperLogLogPlusPlus.forProto(
                ValueType.forStandardType(DefaultOpsType.Id.UINT64)
                    .copyToBuilder(
                        AggregatorStateProto.newBuilder()
                            .setType(AggregatorType.HYPERLOGLOG_PLUS_UNIQUE)
                            .setEncodingVersion(2)
                            .setExtension(
                                HllplusUnique.hyperloglogplusUniqueState,
                                HyperLogLogPlusUniqueStateProto.newBuilder()
                                    .setPrecisionOrNumBuckets(15)
                                    .setSparsePrecisionOrNumBuckets(25)
                                    .setSparseData(ByteString.copyFrom(new byte[] {1}))
                                    .setSparseSize(1)
                                    .build())
                            .setNumValues(2))
                    .build());

    IllegalArgumentException e = assertThrows(IllegalArgumentException.class, () -> a.merge(b));
    assertThat(e)
        .hasMessageThat()
        .isEqualTo("Aggregator of type [INTEGER] is incompatible with aggregator of type [LONG]");
  }

  /**
   * Tests that the aggregator type is fixed during a {@code merge} operation. This is a bit of a
   * pathological case as it requires an unchecked cast on behalf of the client.
   */
  @Test
  public void merge_KnownIntoUnknownType() {
    @SuppressWarnings("unchecked")
    HyperLogLogPlusPlus<Long> a =
        (HyperLogLogPlusPlus<Long>)
            HyperLogLogPlusPlus.forProto(
                AggregatorStateProto.newBuilder()
                    .setType(AggregatorType.HYPERLOGLOG_PLUS_UNIQUE)
                    .setEncodingVersion(2)
                    .setExtension(
                        HllplusUnique.hyperloglogplusUniqueState,
                        HyperLogLogPlusUniqueStateProto.newBuilder()
                            .setPrecisionOrNumBuckets(15)
                            .setSparsePrecisionOrNumBuckets(25)
                            .build())
                    .setNumValues(0)
                    .build());

    HyperLogLogPlusPlus<Long> b = hllBuilder.buildForLongs();

    a.merge(b);

    // Check that the type has been fixed.
    IllegalArgumentException e = assertThrows(IllegalArgumentException.class, () -> a.add(12));
    assertThat(e)
        .hasMessageThat()
        .isEqualTo("unable to add type INTEGER to aggregator of type [LONG]");
  }

  @Test
  public void merge_NormalIntoNormal() {
    HyperLogLogPlusPlus<Integer> a =
        new HyperLogLogPlusPlus.Builder().noSparseMode().buildForIntegers();
    a.add(1);
    a.add(2);
    a.add(3);

    HyperLogLogPlusPlus<Integer> b =
        new HyperLogLogPlusPlus.Builder().noSparseMode().buildForIntegers();
    b.add(3);
    b.add(4);

    a.merge(b);
    assertEquals(4, a.longResult());
    assertEquals(5, a.numValues());
    assertEquals(2, b.longResult());
    assertEquals(2, b.numValues());
  }

  @Test
  public void merge_NormalIntoNormalWithHigherPrecision() {
    HyperLogLogPlusPlus<Integer> a =
        new HyperLogLogPlusPlus.Builder().noSparseMode().buildForIntegers();
    a.add(1);
    a.add(2);
    a.add(3);

    HyperLogLogPlusPlus<Integer> b =
        new HyperLogLogPlusPlus.Builder().normalPrecision(13).noSparseMode().buildForIntegers();
    b.add(3);
    b.add(4);

    a.merge(b);
    assertEquals(13, a.getNormalPrecision());
    assertEquals(0, a.getSparsePrecision());
    assertEquals(4, a.longResult());
    assertEquals(5, a.numValues());
    assertEquals(2, b.longResult());
    assertEquals(2, b.numValues());
  }

  @Test
  public void merge_NormalIntoNormalWithLowerPrecision() {
    HyperLogLogPlusPlus<Integer> a =
        new HyperLogLogPlusPlus.Builder().normalPrecision(13).noSparseMode().buildForIntegers();
    a.add(1);
    a.add(2);
    a.add(3);

    HyperLogLogPlusPlus<Integer> b =
        new HyperLogLogPlusPlus.Builder().noSparseMode().buildForIntegers();
    b.add(3);
    b.add(4);

    a.merge(b);
    assertEquals(13, a.getNormalPrecision());
    assertEquals(0, a.getSparsePrecision());
    assertEquals(4, a.longResult());
    assertEquals(5, a.numValues());
    assertEquals(2, b.longResult());
    assertEquals(2, b.numValues());
  }

  @Test
  public void merge_NormalIntoSparse() {
    HyperLogLogPlusPlus<Integer> a = hllBuilder.buildForIntegers();
    a.add(1);
    a.add(2);
    a.add(3);

    HyperLogLogPlusPlus<Integer> b =
        new HyperLogLogPlusPlus.Builder().noSparseMode().buildForIntegers();
    b.add(3);
    b.add(4);

    a.merge(b);
    assertEquals(4, a.longResult());
    assertEquals(5, a.numValues());
    assertEquals(2, b.longResult());
    assertEquals(2, b.numValues());
  }

  @Test
  public void merge_SparseIntoNormal() {
    HyperLogLogPlusPlus<Integer> a =
        new HyperLogLogPlusPlus.Builder().noSparseMode().buildForIntegers();
    a.add(1);
    a.add(2);
    a.add(3);

    HyperLogLogPlusPlus<Integer> b = hllBuilder.buildForIntegers();
    b.add(3);
    b.add(4);

    a.merge(b);
    assertEquals(4, a.longResult());
    assertEquals(5, a.numValues());
    assertEquals(2, b.longResult());
    assertEquals(2, b.numValues());
  }

  @Test
  public void merge_SparseIntoSparse() {
    HyperLogLogPlusPlus<Integer> a = hllBuilder.buildForIntegers();
    a.add(1);
    a.add(2);
    a.add(3);

    HyperLogLogPlusPlus<Integer> b = hllBuilder.buildForIntegers();
    b.add(3);
    b.add(4);

    a.merge(b);
    assertEquals(4, a.longResult());
    assertEquals(5, a.numValues());
    assertEquals(2, b.longResult());
    assertEquals(2, b.numValues());
  }

  @Test
  public void mergeFromBytes() {
    HyperLogLogPlusPlus<Integer> aggregator = hllBuilder.buildForIntegers();

    byte[] proto =
        ValueType.forStandardType(DefaultOpsType.Id.UINT32)
            .copyToBuilder(
                AggregatorStateProto.newBuilder()
                    .setType(AggregatorType.HYPERLOGLOG_PLUS_UNIQUE)
                    .setEncodingVersion(2)
                    .setExtension(
                        HllplusUnique.hyperloglogplusUniqueState,
                        HyperLogLogPlusUniqueStateProto.newBuilder()
                            .setPrecisionOrNumBuckets(15)
                            .setSparsePrecisionOrNumBuckets(25)
                            .setSparseData(ByteString.copyFrom(new byte[] {1}))
                            .setSparseSize(1)
                            .build())
                    .setNumValues(2))
            .build()
            .toByteArray();

    aggregator.merge(proto);

    assertEquals(1, aggregator.longResult());
    assertEquals(2, aggregator.numValues());
  }

  @Test
  public void mergeFromBytes_DoesNothingWhenEmpty() {
    HyperLogLogPlusPlus<Integer> aggregator = hllBuilder.buildForIntegers();
    aggregator.merge(new byte[0]);

    assertEquals(0, aggregator.longResult());
    assertEquals(0, aggregator.numValues());
  }

  @Test
  public void mergeFromBytes_DoesNothingWhenNull() {
    HyperLogLogPlusPlus<Integer> aggregator = hllBuilder.buildForIntegers();
    aggregator.merge((byte[]) null);

    assertEquals(0, aggregator.longResult());
    assertEquals(0, aggregator.numValues());
  }

  @Test
  public void mergeFromBytes_ThrowsWhenInvalid() {
    HyperLogLogPlusPlus<Integer> aggregator = hllBuilder.buildForIntegers();

    IllegalArgumentException e =
        assertThrows(IllegalArgumentException.class, () -> aggregator.merge(new byte[] {1}));
    assertThat(e).hasCauseThat().isInstanceOf(InvalidProtocolBufferException.class);
  }

  @Test
  public void mergeFromByteString() {
    HyperLogLogPlusPlus<Integer> aggregator = hllBuilder.buildForIntegers();

    ByteString proto =
        ValueType.forStandardType(DefaultOpsType.Id.UINT32)
            .copyToBuilder(
                AggregatorStateProto.newBuilder()
                    .setType(AggregatorType.HYPERLOGLOG_PLUS_UNIQUE)
                    .setEncodingVersion(2)
                    .setExtension(
                        HllplusUnique.hyperloglogplusUniqueState,
                        HyperLogLogPlusUniqueStateProto.newBuilder()
                            .setPrecisionOrNumBuckets(15)
                            .setSparsePrecisionOrNumBuckets(25)
                            .setSparseData(ByteString.copyFrom(new byte[] {1}))
                            .setSparseSize(1)
                            .build())
                    .setNumValues(2))
            .build()
            .toByteString();

    aggregator.merge(proto);

    assertEquals(1, aggregator.longResult());
    assertEquals(2, aggregator.numValues());
  }

  @Test
  public void mergeFromByteString_DoesNothingWhenEmpty() {
    HyperLogLogPlusPlus<Integer> aggregator = hllBuilder.buildForIntegers();
    aggregator.merge(ByteString.EMPTY);

    assertEquals(0, aggregator.longResult());
    assertEquals(0, aggregator.numValues());
  }

  @Test
  public void mergeFromByteString_DoesNothingWhenNull() {
    HyperLogLogPlusPlus<Integer> aggregator = hllBuilder.buildForIntegers();
    aggregator.merge((ByteString) null);

    assertEquals(0, aggregator.longResult());
    assertEquals(0, aggregator.numValues());
  }

  @Test
  public void mergeFromByteString_ThrowsWhenInvalid() {
    HyperLogLogPlusPlus<Integer> aggregator = hllBuilder.buildForIntegers();

    IllegalArgumentException e =
        assertThrows(
            IllegalArgumentException.class,
            () -> aggregator.merge(ByteString.copyFrom(new byte[] {1})));
    assertThat(e).hasCauseThat().isInstanceOf(InvalidProtocolBufferException.class);
  }

  @Test
  public void mergeFromProto_KnownIntoUnknownType() {
    HyperLogLogPlusPlus<?> aggregator =
        HyperLogLogPlusPlus.forProto(
            AggregatorStateProto.newBuilder()
                .setType(AggregatorType.HYPERLOGLOG_PLUS_UNIQUE)
                .setEncodingVersion(2)
                .setExtension(
                    HllplusUnique.hyperloglogplusUniqueState,
                    HyperLogLogPlusUniqueStateProto.newBuilder()
                        .setPrecisionOrNumBuckets(15)
                        .setSparsePrecisionOrNumBuckets(25)
                        .build())
                .setNumValues(0)
                .build());

    AggregatorStateProto proto =
        ValueType.forStandardType(DefaultOpsType.Id.UINT64)
            .copyToBuilder(
                AggregatorStateProto.newBuilder()
                    .setType(AggregatorType.HYPERLOGLOG_PLUS_UNIQUE)
                    .setEncodingVersion(2)
                    .setExtension(
                        HllplusUnique.hyperloglogplusUniqueState,
                        HyperLogLogPlusUniqueStateProto.newBuilder()
                            .setPrecisionOrNumBuckets(15)
                            .setSparsePrecisionOrNumBuckets(25)
                            .build())
                    .setNumValues(0))
            .build();

    aggregator.merge(proto);

    // Check that the type has been fixed.
    IllegalArgumentException e =
        assertThrows(IllegalArgumentException.class, () -> aggregator.add(12));
    assertThat(e)
        .hasMessageThat()
        .isEqualTo("unable to add type INTEGER to aggregator of type [LONG]");
  }

  @Test
  public void mergeFromProto_ThrowsWhenIncompatibleType() {
    HyperLogLogPlusPlus<Integer> aggregator = hllBuilder.buildForIntegers();

    AggregatorStateProto proto =
        ValueType.forStandardType(DefaultOpsType.Id.UINT64)
            .copyToBuilder(
                AggregatorStateProto.newBuilder()
                    .setType(AggregatorType.HYPERLOGLOG_PLUS_UNIQUE)
                    .setEncodingVersion(2)
                    .setExtension(
                        HllplusUnique.hyperloglogplusUniqueState,
                        HyperLogLogPlusUniqueStateProto.newBuilder()
                            .setPrecisionOrNumBuckets(15)
                            .setSparsePrecisionOrNumBuckets(25)
                            .setSparseData(ByteString.copyFrom(new byte[] {1}))
                            .setSparseSize(1)
                            .build())
                    .setNumValues(2))
            .build();

    IllegalArgumentException e =
        assertThrows(IllegalArgumentException.class, () -> aggregator.merge(proto));
    assertThat(e)
        .hasMessageThat()
        .isEqualTo("Aggregator of type [INTEGER] is incompatible with aggregator of type [LONG]");
  }

  @Test
  public void mergeFromProto_UnknownIntoKnownType() {
    HyperLogLogPlusPlus<?> aggregator =
        HyperLogLogPlusPlus.forProto(
            ValueType.forStandardType(DefaultOpsType.Id.UINT64)
                .copyToBuilder(
                    AggregatorStateProto.newBuilder()
                        .setType(AggregatorType.HYPERLOGLOG_PLUS_UNIQUE)
                        .setEncodingVersion(2)
                        .setExtension(
                            HllplusUnique.hyperloglogplusUniqueState,
                            HyperLogLogPlusUniqueStateProto.newBuilder()
                                .setPrecisionOrNumBuckets(15)
                                .setSparsePrecisionOrNumBuckets(25)
                                .build())
                        .setNumValues(0))
                .build());

    AggregatorStateProto proto =
        AggregatorStateProto.newBuilder()
            .setType(AggregatorType.HYPERLOGLOG_PLUS_UNIQUE)
            .setEncodingVersion(2)
            .setExtension(
                HllplusUnique.hyperloglogplusUniqueState,
                HyperLogLogPlusUniqueStateProto.newBuilder()
                    .setPrecisionOrNumBuckets(15)
                    .setSparsePrecisionOrNumBuckets(25)
                    .build())
            .setNumValues(0)
            .build();

    aggregator.merge(proto);

    // Check that the type has been fixed.
    IllegalArgumentException e =
        assertThrows(IllegalArgumentException.class, () -> aggregator.add(12));
    assertThat(e)
        .hasMessageThat()
        .isEqualTo("unable to add type INTEGER to aggregator of type [LONG]");
  }

  @Test
  public void mergeFromProto_UnknownIntoUnknownType() {
    HyperLogLogPlusPlus<?> aggregator =
        HyperLogLogPlusPlus.forProto(
            AggregatorStateProto.newBuilder()
                .setType(AggregatorType.HYPERLOGLOG_PLUS_UNIQUE)
                .setEncodingVersion(2)
                .setExtension(
                    HllplusUnique.hyperloglogplusUniqueState,
                    HyperLogLogPlusUniqueStateProto.newBuilder()
                        .setPrecisionOrNumBuckets(15)
                        .setSparsePrecisionOrNumBuckets(25)
                        .build())
                .setNumValues(0)
                .build());

    AggregatorStateProto proto =
        AggregatorStateProto.newBuilder()
            .setType(AggregatorType.HYPERLOGLOG_PLUS_UNIQUE)
            .setEncodingVersion(2)
            .setExtension(
                HllplusUnique.hyperloglogplusUniqueState,
                HyperLogLogPlusUniqueStateProto.newBuilder()
                    .setPrecisionOrNumBuckets(15)
                    .setSparsePrecisionOrNumBuckets(25)
                    .build())
            .setNumValues(0)
            .build();

    aggregator.merge(proto);

    // Check that the type is still open.
    aggregator.add(12);
  }

  @Test
  public void numValues() {
    HyperLogLogPlusPlus<Integer> aggregator = hllBuilder.buildForIntegers();

    aggregator.add(1);
    aggregator.add(2);
    aggregator.add(3);
    aggregator.add(2);
    aggregator.add(3);

    assertEquals(5, aggregator.numValues());
  }

  @Test
  public void numValues_ZeroWhenEmpty() {
    HyperLogLogPlusPlus<Integer> aggregator = hllBuilder.buildForIntegers();
    assertEquals(0, aggregator.numValues());
  }

  @Test
  public void serializeToBytes() throws Exception {
    HyperLogLogPlusPlus<Integer> aggregator = hllBuilder.buildForIntegers();
    aggregator.add(1);
    aggregator.add(2);
    aggregator.add(3);

    AggregatorStateProto actual =
        AggregatorStateProto.parseFrom(aggregator.serializeToByteArray(), EXTENSIONS);

    // Don't worry about the exact value for data, the golden tests cover that.
    assertThat(
            ValueType.forStandardType(DefaultOpsType.Id.UINT32)
                .copyToBuilder(
                    AggregatorStateProto.newBuilder()
                        .setType(AggregatorType.HYPERLOGLOG_PLUS_UNIQUE)
                        .setEncodingVersion(2)
                        .setNumValues(3)
                        .setExtension(
                            HllplusUnique.hyperloglogplusUniqueState,
                            HyperLogLogPlusUniqueStateProto.newBuilder()
                                .setPrecisionOrNumBuckets(15)
                                .setSparsePrecisionOrNumBuckets(25)
                                .setSparseSize(3)
                                .setData(ByteString.EMPTY)
                                .build()))
                .build())
        .ignoringFieldDescriptors(
            HyperLogLogPlusUniqueStateProto.getDescriptor().findFieldByName("sparse_data"))
        .ignoringFieldAbsence()
        .isEqualTo(actual);
    assertTrue(actual.getExtension(HllplusUnique.hyperloglogplusUniqueState).hasSparseData());
  }

  @Test
  public void serializeToByteString() throws Exception {
    HyperLogLogPlusPlus<Integer> aggregator = hllBuilder.buildForIntegers();
    aggregator.add(1);
    aggregator.add(2);
    aggregator.add(3);

    AggregatorStateProto actual =
        AggregatorStateProto.parseFrom(aggregator.serializeToByteString(), EXTENSIONS);

    // Don't worry about the exact value for data, the golden tests cover that.
    assertThat(
            ValueType.forStandardType(DefaultOpsType.Id.UINT32)
                .copyToBuilder(
                    AggregatorStateProto.newBuilder()
                        .setType(AggregatorType.HYPERLOGLOG_PLUS_UNIQUE)
                        .setEncodingVersion(2)
                        .setNumValues(3)
                        .setExtension(
                            HllplusUnique.hyperloglogplusUniqueState,
                            HyperLogLogPlusUniqueStateProto.newBuilder()
                                .setPrecisionOrNumBuckets(15)
                                .setSparsePrecisionOrNumBuckets(25)
                                .setSparseSize(3)
                                .setData(ByteString.EMPTY)
                                .build()))
                .build())
        .ignoringFieldDescriptors(
            HyperLogLogPlusUniqueStateProto.getDescriptor().findFieldByName("sparse_data"))
        .ignoringFieldAbsence()
        .isEqualTo(actual);
    assertTrue(actual.getExtension(HllplusUnique.hyperloglogplusUniqueState).hasSparseData());
  }

  @Test
  public void serializeToProto() {
    HyperLogLogPlusPlus<Integer> aggregator = hllBuilder.buildForIntegers();
    aggregator.add(1);
    aggregator.add(2);
    aggregator.add(3);

    AggregatorStateProto actual = aggregator.serializeToProto();

    // Don't worry about the exact value for data, the golden tests cover that.
    assertThat(
            ValueType.forStandardType(DefaultOpsType.Id.UINT32)
                .copyToBuilder(
                    AggregatorStateProto.newBuilder()
                        .setType(AggregatorType.HYPERLOGLOG_PLUS_UNIQUE)
                        .setEncodingVersion(2)
                        .setNumValues(3)
                        .setExtension(
                            HllplusUnique.hyperloglogplusUniqueState,
                            HyperLogLogPlusUniqueStateProto.newBuilder()
                                .setPrecisionOrNumBuckets(15)
                                .setSparsePrecisionOrNumBuckets(25)
                                .setSparseSize(3)
                                .setData(ByteString.EMPTY)
                                .build()))
                .build())
        .ignoringFieldDescriptors(
            HyperLogLogPlusUniqueStateProto.getDescriptor().findFieldByName("sparse_data"))
        .ignoringFieldAbsence()
        .isEqualTo(actual);
    assertTrue(actual.getExtension(HllplusUnique.hyperloglogplusUniqueState).hasSparseData());
  }

  @Test
  public void serializeToProto_EmptyAggregatorSetsEmptySparseDataField() {
    HyperLogLogPlusPlus<ByteString> aggregator =
        new HyperLogLogPlusPlus.Builder().normalPrecision(13).sparsePrecision(16).buildForBytes();

    AggregatorStateProto actual = aggregator.serializeToProto();
    HyperLogLogPlusUniqueStateProto ext =
        actual.getExtension(HllplusUnique.hyperloglogplusUniqueState);
    assertTrue(ext.hasSparseData());
    assertEquals(0, ext.getSparseData().size());
  }

  /**
   * Tests that the hash and value type is correctly mapped when the aggregator type is an ambiguous
   * [BYTE, STRING] type.
   */
  @Test
  public void serializeToProto_TypeWhenBytesAndStringType() {
    HyperLogLogPlusPlus<?> aggregator =
        HyperLogLogPlusPlus.forProto(
            ValueType.forStandardType(DefaultOpsType.Id.BYTES_OR_UTF8_STRING)
                .copyToBuilder(
                    AggregatorStateProto.newBuilder()
                        .setType(AggregatorType.HYPERLOGLOG_PLUS_UNIQUE)
                        .setEncodingVersion(2)
                        .setNumValues(0)
                        .setExtension(
                            HllplusUnique.hyperloglogplusUniqueState,
                            HyperLogLogPlusUniqueStateProto.newBuilder()
                                .setPrecisionOrNumBuckets(15)
                                .setSparsePrecisionOrNumBuckets(25)
                                .build()))
                .build());

    AggregatorStateProto actual = aggregator.serializeToProto();
    assertThat(ValueType.forStateProto(actual))
        .isEqualTo(ValueType.forStandardType(DefaultOpsType.Id.BYTES_OR_UTF8_STRING));
  }

  @Test
  public void lowestPrecision_basicOps_normalOnly() {
    HyperLogLogPlusPlus<Long> aggregator =
        new HyperLogLogPlusPlus.Builder().normalPrecision(4).noSparseMode().buildForLongs();

    assertThat(aggregator.getNormalPrecision()).isEqualTo(4);
    assertThat(aggregator.getSparsePrecision()).isEqualTo(0);

    aggregator.add(42L);

    assertThat(aggregator.longResult()).isEqualTo(1);

    AggregatorStateProto stateProto = aggregator.serializeToProto();
    assertThat(stateProto)
        .isEqualTo(
            ValueType.forStandardType(DefaultOpsType.Id.UINT64)
                .copyToBuilder(
                    AggregatorStateProto.newBuilder()
                        .setType(AggregatorType.HYPERLOGLOG_PLUS_UNIQUE)
                        .setEncodingVersion(2)
                        .setNumValues(1)
                        .setExtension(
                            HllplusUnique.hyperloglogplusUniqueState,
                            HyperLogLogPlusUniqueStateProto.newBuilder()
                                .setPrecisionOrNumBuckets(4)
                                .setData(
                                    ByteString.copyFrom(
                                        new byte[] {
                                          0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0
                                        }))
                                .build()))
                .build());

    HyperLogLogPlusPlus<?> fromProto = HyperLogLogPlusPlus.forProto(stateProto);
    assertThat(fromProto.getNormalPrecision()).isEqualTo(4);
    assertThat(fromProto.getSparsePrecision()).isEqualTo(0);
    assertThat(fromProto.longResult()).isEqualTo(1);
  }

  @Test
  public void lowestPrecision_basicOps_withSparse() {
    HyperLogLogPlusPlus<Long> aggregator =
        new HyperLogLogPlusPlus.Builder().normalPrecision(4).sparsePrecision(4).buildForLongs();

    assertThat(aggregator.getNormalPrecision()).isEqualTo(4);
    assertThat(aggregator.getSparsePrecision()).isEqualTo(4);

    aggregator.add(42L);

    assertThat(aggregator.longResult()).isEqualTo(1);

    AggregatorStateProto stateProto = aggregator.serializeToProto();
    assertThat(stateProto)
        .isEqualTo(
            ValueType.forStandardType(DefaultOpsType.Id.UINT64)
                .copyToBuilder(
                    AggregatorStateProto.newBuilder()
                        .setType(AggregatorType.HYPERLOGLOG_PLUS_UNIQUE)
                        .setEncodingVersion(2)
                        .setNumValues(1)
                        .setExtension(
                            HllplusUnique.hyperloglogplusUniqueState,
                            HyperLogLogPlusUniqueStateProto.newBuilder()
                                .setPrecisionOrNumBuckets(4)
                                .setSparsePrecisionOrNumBuckets(4)
                                .setSparseSize(1)
                                .setSparseData(ByteString.copyFrom(new byte[] {-63, 10}))
                                .build()))
                .build());

    HyperLogLogPlusPlus<?> fromProto = HyperLogLogPlusPlus.forProto(stateProto);
    assertThat(fromProto.getNormalPrecision()).isEqualTo(4);
    assertThat(fromProto.getSparsePrecision()).isEqualTo(4);
    assertThat(fromProto.longResult()).isEqualTo(1);
  }

  @Test
  public void builder_usesBothPrecisionDefaultsWhenUnspecified() {
    HyperLogLogPlusPlus<String> aggregator = new HyperLogLogPlusPlus.Builder().buildForStrings();

    assertThat(aggregator.getNormalPrecision())
        .isEqualTo(HyperLogLogPlusPlus.DEFAULT_NORMAL_PRECISION);
    assertThat(aggregator.getSparsePrecision())
        .isEqualTo(
            HyperLogLogPlusPlus.DEFAULT_NORMAL_PRECISION
                + HyperLogLogPlusPlus.DEFAULT_SPARSE_PRECISION_DELTA);
  }

  @Test
  public void builder_usesNormalPrecisionDefaultWhenUnspecified() {
    HyperLogLogPlusPlus<Integer> aggregator =
        new HyperLogLogPlusPlus.Builder().sparsePrecision(18).buildForIntegers();

    assertThat(aggregator.getNormalPrecision())
        .isEqualTo(HyperLogLogPlusPlus.DEFAULT_NORMAL_PRECISION);
    assertThat(aggregator.getSparsePrecision()).isEqualTo(18);
  }

  @Test
  public void builder_usesSparsePrecisionDefaultWhenUnspecified() {
    HyperLogLogPlusPlus<Long> aggregator =
        new HyperLogLogPlusPlus.Builder().normalPrecision(18).buildForLongs();

    assertThat(aggregator.getNormalPrecision()).isEqualTo(18);
    assertThat(aggregator.getSparsePrecision())
        .isEqualTo(18 + HyperLogLogPlusPlus.DEFAULT_SPARSE_PRECISION_DELTA);
  }

  @Test
  public void builder_usesBothPrecisionsAsSpecified() {
    HyperLogLogPlusPlus<ByteString> aggregator =
        new HyperLogLogPlusPlus.Builder().normalPrecision(14).sparsePrecision(17).buildForBytes();

    assertThat(aggregator.getNormalPrecision()).isEqualTo(14);
    assertThat(aggregator.getSparsePrecision()).isEqualTo(17);
  }

  @Test
  public void builder_invocationOrderDoesNotMatter() {
    HyperLogLogPlusPlus<ByteString> aggregator =
        new HyperLogLogPlusPlus.Builder().sparsePrecision(17).normalPrecision(14).buildForBytes();

    assertThat(aggregator.getNormalPrecision()).isEqualTo(14);
    assertThat(aggregator.getSparsePrecision()).isEqualTo(17);
  }

  @Test
  public void builder_noSparseMode() {
    HyperLogLogPlusPlus<ByteString> aggregator =
        new HyperLogLogPlusPlus.Builder().noSparseMode().normalPrecision(16).buildForBytes();

    assertThat(aggregator.getSparsePrecision())
        .isEqualTo(HyperLogLogPlusPlus.SPARSE_PRECISION_DISABLED);
    assertThat(aggregator.getNormalPrecision()).isEqualTo(16);
  }

  @Test
  public void builder_sparsePrecision_setMultipleTimes() {
    HyperLogLogPlusPlus.Builder hllBuilder =
        new HyperLogLogPlusPlus.Builder().noSparseMode().normalPrecision(16);

    HyperLogLogPlusPlus<ByteString> noSparseAggregator = hllBuilder.buildForBytes();

    assertThat(noSparseAggregator.getSparsePrecision())
        .isEqualTo(HyperLogLogPlusPlus.SPARSE_PRECISION_DISABLED);
    assertThat(noSparseAggregator.getNormalPrecision()).isEqualTo(16);

    // Re-enable sparse precision.
    HyperLogLogPlusPlus<String> sparseAggregator = hllBuilder.sparsePrecision(20).buildForStrings();

    assertThat(sparseAggregator.getSparsePrecision()).isEqualTo(20);
    assertThat(sparseAggregator.getNormalPrecision()).isEqualTo(16);

    // Disable it again.
    HyperLogLogPlusPlus<Long> noSparseAggregator2 = hllBuilder.noSparseMode().buildForLongs();

    assertThat(noSparseAggregator2.getSparsePrecision())
        .isEqualTo(HyperLogLogPlusPlus.SPARSE_PRECISION_DISABLED);
    assertThat(noSparseAggregator2.getNormalPrecision()).isEqualTo(16);
  }

  @Test
  public void builder_defaultSparsePrecisionWithHighNormalPrecision_capsSparsePrecision() {
    // Normal precision 22 would optimally lead to sparse precision 27 but the max sparse precision
    // is only 25. This test verifies we adjust the default sparse precision correctly.
    HyperLogLogPlusPlus<ByteString> aggregator =
        new HyperLogLogPlusPlus.Builder().normalPrecision(22).buildForBytes();

    assertThat(aggregator.getSparsePrecision()).isEqualTo(25);
    assertThat(aggregator.getNormalPrecision()).isEqualTo(22);
  }

  @Test
  public void builder_reuse() {
    HyperLogLogPlusPlus.Builder hllBuilder =
        new HyperLogLogPlusPlus.Builder().normalPrecision(13).sparsePrecision(16);

    HyperLogLogPlusPlus<ByteString> bytesAggregator = hllBuilder.buildForBytes();

    bytesAggregator.add(new byte[] {12});

    assertEquals(1, bytesAggregator.longResult());
    assertEquals(1, bytesAggregator.numValues());
    assertThat(bytesAggregator.getNormalPrecision()).isEqualTo(13);
    assertThat(bytesAggregator.getSparsePrecision()).isEqualTo(16);

    HyperLogLogPlusPlus<Long> longsAggregator = hllBuilder.buildForLongs();

    longsAggregator.add(1L);

    assertEquals(1, longsAggregator.longResult());
    assertEquals(1, longsAggregator.numValues());
    assertThat(longsAggregator.getNormalPrecision()).isEqualTo(13);
    assertThat(longsAggregator.getSparsePrecision()).isEqualTo(16);

    // Change precisions.
    hllBuilder.sparsePrecision(20).normalPrecision(18);

    HyperLogLogPlusPlus<String> stringAggregator = hllBuilder.buildForStrings();

    stringAggregator.add("foo");

    assertEquals(1, stringAggregator.longResult());
    assertEquals(1, stringAggregator.numValues());
    assertThat(stringAggregator.getNormalPrecision()).isEqualTo(18);
    assertThat(stringAggregator.getSparsePrecision()).isEqualTo(20);
  }
}
