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

package functionalTests;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import javax.annotation.Nonnull;

@Data
@AllArgsConstructor
@NoArgsConstructor
class TestDTO implements Comparable<TestDTO> {
    private String value;

    @Override
    public int compareTo(@Nonnull TestDTO testDTO) {
        return value.compareTo(testDTO.value);
    }

    static final TestDTO ZERO = new TestDTO("ZERO");
    static final TestDTO ONE = new TestDTO("ONE");
    static final TestDTO TWO = new TestDTO("TWO");
    static final TestDTO THREE = new TestDTO("THREE");
    static final TestDTO FOUR = new TestDTO("FOUR");
    static final TestDTO FIVE = new TestDTO("FIVE");
    static final TestDTO SIX = new TestDTO("SIX");
    static final TestDTO SEVEN = new TestDTO("SEVEN");
    static final TestDTO EIGHT = new TestDTO("EIGHT");
    static final TestDTO NINE = new TestDTO("NINE");
    static final TestDTO TEN = new TestDTO("TEN");
    static final TestDTO ELEVEN = new TestDTO("ELEVEN");
}
