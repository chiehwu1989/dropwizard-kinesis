/*
 * Copyright 2014 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Amazon Software License (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 * http://aws.amazon.com/asl/
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */
package io.codemonastery.dropwizard.kinesis.multilang.messages;

import com.amazonaws.services.kinesis.model.Record;

/**
 * Class for encoding Record objects to json. Needed because Records have byte buffers for their data field which causes
 * problems for the json library we're using.
 */
public class JsonFriendlyRecord {
    private byte[] data;
    private String partitionKey;
    private String sequenceNumber;

    /**
     * Default Constructor.
     */
    public JsonFriendlyRecord() {
    }

    /**
     * Convenience constructor.
     * 
     * @param record The record that this message will represent.
     */
    public JsonFriendlyRecord(Record record) {
        this.withData(record.getData() == null ? null : record.getData().array())
                .withPartitionKey(record.getPartitionKey()).withSequenceNumber(record.getSequenceNumber());
    }

    /**
     * @return The data.
     */
    public byte[] getData() {
        return data;
    }

    /**
     * @return The partition key.
     */
    public String getPartitionKey() {
        return partitionKey;
    }

    /**
     * @return The sequence number.
     */
    public String getSequenceNumber() {
        return sequenceNumber;
    }

    /**
     * @param data The data.
     */
    public void setData(byte[] data) {
        this.data = data;
    }

    /**
     * @param partitionKey The partition key.
     */
    public void setPartitionKey(String partitionKey) {
        this.partitionKey = partitionKey;
    }

    /**
     * @param sequenceNumber The sequence number.
     */
    public void setSequenceNumber(String sequenceNumber) {
        this.sequenceNumber = sequenceNumber;
    }

    /**
     * @param data The data.
     * 
     * @return this
     */
    public JsonFriendlyRecord withData(byte[] data) {
        this.setData(data);
        return this;
    }

    /**
     * @param partitionKey The partition key.
     * 
     * @return this
     */
    public JsonFriendlyRecord withPartitionKey(String partitionKey) {
        this.setPartitionKey(partitionKey);
        return this;
    }

    /**
     * @param sequenceNumber The sequence number.
     * 
     * @return this
     */
    public JsonFriendlyRecord withSequenceNumber(String sequenceNumber) {
        this.setSequenceNumber(sequenceNumber);
        return this;
    }
}
