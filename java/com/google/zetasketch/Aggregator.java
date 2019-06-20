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

import com.google.protobuf.ByteString;
import com.google.protos.zetasketch.Aggregator.AggregatorStateProto;

/**
 * This class provides the public interface for one-pass, distributed, online aggregation algorithms
 * in the Java version of the aggregation library.
 *
 * @param <V> The type of values being aggregated.
 * @param <R> The type of the result of the aggregation.
 * @param <A> The type of aggregation implementation. Used to enforce type safety on calls to {@link
 *     #merge(Aggregator)}.
 */
public interface Aggregator<V, R, A extends Aggregator<V, R, A>> {

  /** Adds {@code value} to the aggregator. */
  void add(V value);

  /**
   * Merges the state from {@code aggregator} into this one.
   *
   * <p>In general, the supplied aggregator must be of the same type and initialized with the same
   * parameters as this one. Implementations which are more accommodating will document this.
   *
   * <p>Clients should use the most direct merge operation possible as it allows aggregators to
   * implement performance optimizations. In general, assume that {@code merge(bytes)} is faster
   * than {@code merge(AggregatorStateProto.parseFrom(bytes))} or that {@code merge(proto)} is
   * faster than {@code merge(SomeAggregator.fromProto(proto))}.
   */
  void merge(A aggregator);

  /**
   * Merges the state from {@code proto} into this one.
   *
   * <p>In general, the supplied aggregator state must be of the same type and initialized with the
   * same parameters as this one. Implementations which are more accommodating will document this.
   *
   * <p>Clients should use the most direct merge operation possible as it allows aggregators to
   * implement performance optimizations. In general, assume that {@code merge(bytes)} is faster
   * than {@code merge(AggregatorStateProto.parseFrom(bytes))} or that {@code merge(proto)} is
   * faster than {@code merge(SomeAggregator.fromProto(proto))}.
   */
  void merge(AggregatorStateProto proto);

  /**
   * Merges the state from {@code data} into this one.
   *
   * <p>In general, the supplied aggregator state must be of the same type and initialized with the
   * same parameters as this one. Implementations which are more accommodating will document this.
   *
   * <p>Clients should use the most direct merge operation possible as it allows aggregators to
   * implement performance optimizations. In general, assume that {@code merge(bytes)} is faster
   * than {@code merge(AggregatorStateProto.parseFrom(bytes))} or that {@code merge(proto)} is
   * faster than {@code merge(SomeAggregator.fromProto(proto))}.
   *
   * @param data A marshaled {@link AggregatorStateProto} with a serialized state compatible to this
   *     aggregator.
   * @throws IllegalArgumentException if the proto can not be unmarshaled from the given bytes or if
   *     the proto does not contain a state compatible with this type of aggregator.
   */
  void merge(ByteString data) throws IllegalArgumentException;

  /** Returns the total number of input values that this aggregator has seen. */
  long numValues();

  /** Returns the aggregated result of this aggregator. */
  R result();

  /**
   * Returns the internal state of the aggregator as a serialized {@link ByteString}. The returned
   * value can be deserialized into an {@link AggregatorStateProto} or passed to {@link
   * #merge(ByteString)}.
   *
   * <p>For some aggregators, this may be faster than calling the semantically equivalent {@code
   * aggregator.serializeToProto().toByteString()} as it permits individual aggregators to
   * implement performance improvements that do not use the default proto serializer.
   */
  ByteString serializeToByteString();

  /**
   * Returns the internal state of the aggregator as a protocol buffer. The returned value can be
   * passed in to {@link #merge(AggregatorStateProto)}.
   *
   * <p>It may be faster to call {@link #serializeToByteArray()} or {@link #serializeToByteString()}
   * directly instead of {@code aggregator.serializeToProto().toByteArray()} or {@code
   * aggregator.serializeToProto().toByteString()} as it permits aggregators to implement
   * performance improvements that do not use the default proto serializer.
   */
  AggregatorStateProto serializeToProto();
}
