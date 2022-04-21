/*
 * Copyright 2020 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy of this
 * software and associated documentation files (the "Software"), to deal in the Software
 * without restriction, including without limitation the rights to use, copy, modify,
 * merge, publish, distribute, sublicense, and/or sell copies of the Software, and to
 * permit persons to whom the Software is furnished to do so.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED,
 * INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A
 * PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT
 * HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION
 * OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE
 * SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
 */

package com.amazonaws.kinesisanalytics.flink.streaming.etl.utils;

import org.apache.commons.lang.exception.ExceptionUtils;

import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.Date;


public class FormatUtils {
    public static Timestamp getTimestamp(String dateString) {
        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSSSS");
        try {
            Date parsedDate = dateFormat.parse(dateString);
            Timestamp timestamp = new java.sql.Timestamp(parsedDate.getTime());

            return timestamp;
        } catch(Exception e) {
            // log.error(ExceptionUtils.getStackTrace(e));
            Date parsedDate = new Date();
            Timestamp timestamp = new java.sql.Timestamp(parsedDate.getTime());
            return timestamp;
        }
    }
}
