/*
 * Copyright 2017-2018 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 */

package functionalTests;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.joda.time.DateTime;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class TestDTO2 {
    private Integer id;
    private String aString;
    private Integer aNumber;
    private DateTime aDateTime;
    private Boolean aBoolean;
}
