<!--
    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at

        https://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.
-->

# ZetaSketch
ZetaSketch is a collection of libraries for single-pass, distributed,
approximate aggregation and sketching algorithms.

These algorithms estimate statistics that are often too expensive to compute
exactly.

The estimates use far fewer memory resources than exact calculations. For
example, the HyperLogLog++ algorithm can estimate daily active users with:

* [Kilobytes](https://cloud.google.com/bigquery/docs/reference/standard-sql/hll_functions) instead of [gigabytes](https://ai.google/research/pubs/pub40671) of memory
* Less than [0.5%](https://cloud.google.com/bigquery/docs/reference/standard-sql/hll_functions) relative error

ZetaSketch currently includes libraries to implement the following algorithms:

| Algorithm | Statistics | Libraries |
| --- | --- | --- |
| [HyperLogLog++](#hyperloglog) | Estimates the [number of distinct values](https://en.wikipedia.org/wiki/Count-distinct_problem) | [Java](https://github.com/google/zetasketch/blob/master/java/com/google/zetasketch/HyperLogLogPlusPlus.java) |

## What is a sketch?
ZetaSketch libraries calculate statistics from **sketches**. A sketch is a
summary of a large data stream. You can extract statistics from a sketch to
estimate particular statistics of the original data, or merge sketches to
summarize multiple data streams.

After choosing an algorithm, you can use its corresponding libraries to:

* Create sketches
* Add new data to existing sketches
* Merge multiple sketches
* Extract statistics from sketches

## HyperLogLog++
The HyperLogLog++ (HLL++) algorithm estimates the number of distinct values in a
data stream. HLL++ is based on
[HyperLogLog](https://en.wikipedia.org/wiki/HyperLogLog); HLL++
[more accurately](https://ai.google/research/pubs/pub40671) estimates the number
of distinct values in very large and small data streams.

### Creating a sketch

```java
// Create a sketch for estimating the number of unique strings in a data stream.
// You can also create sketches for estimating the number of unique byte
// sequences, integers, and longs.

HyperLogLogPlusPlus<String> hll = new HyperLogLogPlusPlus.Builder().buildForStrings();

// You can also set a custom precision. The default normal and sparse precisions
// are 15 and 20, respectively.
HyperLogLogPlusPlus<String> hllCustomPrecision = new HyperLogLogPlusPlus.Builder()
    .normalPrecision(13).sparsePrecision(19).buildForStrings();
```

### Adding new data to a sketch

```java
// Add three strings to the `hll` sketch. You must first initialize an empty
// sketch and then add data to it.
hll.add("apple");
hll.add("orange");
hll.add("banana");
```

### Merging sketches

```java
// Merge `hll2` and `hll3` with `hll`. The sketches must have the same
// original data type and precision.
hll.merge(hll2);
hll.merge(hll3);
```

### Extracting cardinality estimates

```java
// Return the estimate of the number of distinct values.
long result = hll.result();
```

## How to use ZetaSketch
Please find the instructions for your build tool on the right side of
https://search.maven.org/artifact/com.google.zetasketch/zetasketch

## How to build ZetaSketch
ZetaSketch uses [Gradle](https://gradle.org/) as its build system. To build the
project, simply run:

```
./gradlew build
```

## License
[Apache License 2.0](LICENSE)

## Contributing
We are not currently accepting contributions to this project. Please feel free
to file bugs and feature requests using
[GitHub's issue tracker](https://github.com/google/zetasketch/issues/new).

## Disclaimer
This is not an officially supported Google product.
