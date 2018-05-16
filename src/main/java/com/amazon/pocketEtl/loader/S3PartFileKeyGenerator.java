/*
 *   Copyright 2018 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 *   Licensed under the Apache License, Version 2.0 (the "License").
 *   You may not use this file except in compliance with the License.
 *   A copy of the License is located at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *   or in the "license" file accompanying this file. This file is distributed
 *   on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 *   express or implied. See the License for the specific language governing
 *   permissions and limitations under the License.
 */

package com.amazon.pocketEtl.loader;

import javax.annotation.Nonnull;

/**
 * Functional interface that describes a method that generates an s3Key (analogous to canonical filename) based on
 * two sequence numbers: thread identifier and file part number. Used by S3 based loaders such as S3FastLoader.
 *
 * Example:
 * (threadNum, partNum) -> String.format("etl-output/%02d/data-%d.csv")
 *
 * Will typically produce keys that look like :
 * etl-output/01/data-1.csv
 * etl-output/01/data-2.csv
 * etl-output/02/data-1.csv
 * ...etc
 *
 */
@FunctionalInterface
public interface S3PartFileKeyGenerator {
    @Nonnull
    String generateS3Key(int threadNum, int partNumber);
}
