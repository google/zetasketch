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

package com.google.zetasketch.testing;

import com.google.common.truth.FailureMetadata;
import com.google.common.truth.Subject;
import com.google.common.truth.Truth;
import javax.annotation.Nullable;

/**
 * A truth subject that renders integers in binary rather than in decimal form. This is useful as it
 * can be hard to see what failed in a bit twiddling method from the decimal integer representation
 * (e.g. "791 is not equal to 919" vs. "0b1100010111 is not equal to 0b1110010111").
 */
public class IntegerBitSubject extends Subject {

  private static final Subject.Factory<IntegerBitSubject, Integer> FACTORY = IntegerBitSubject::new;

  public static IntegerBitSubject assertThat(Integer value) {
    return Truth.assertAbout(FACTORY).that(value);
  }

  private final Integer actual;

  private IntegerBitSubject(FailureMetadata failureMetadata, Integer subject) {
    super(failureMetadata, subject);
    this.actual = subject;
  }

  @Override
  public void isEqualTo(@Nullable Object other) {
    Object displayObject =
        (other instanceof Integer) ? toBinaryString(((Integer) other).intValue()) : other;
    if (!this.actual.equals(other)) {
      this.failWithActual("expected to be equal to", displayObject);
    }
  }

  @Override
  protected String actualCustomStringRepresentation() {
    return toBinaryString(getIntSubject());
  }

  private static String toBinaryString(int value) {
    return "0b" + Integer.toBinaryString(value);
  }

  private int getIntSubject() {
    return actual.intValue();
  }
}
