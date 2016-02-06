/*
 * Copyright 2012-2013 Amazon.com, Inc. or its affiliates. All Rights Reserved.
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
package io.codemonastery.dropwizard.kinesis.leases.impl;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import io.codemonastery.dropwizard.kinesis.clientlibrary.types.ExtendedSequenceNumber;
import io.codemonastery.dropwizard.kinesis.leases.exceptions.DependencyException;
import io.codemonastery.dropwizard.kinesis.leases.exceptions.InvalidStateException;
import io.codemonastery.dropwizard.kinesis.leases.exceptions.ProvisionedThroughputException;
import io.codemonastery.dropwizard.kinesis.leases.interfaces.IKinesisClientLeaseManager;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * An implementation of LeaseManager for the KinesisClientLibrary - takeLease updates the ownerSwitchesSinceCheckpoint field.
 */
public class KinesisClientLeaseManager extends LeaseManager<KinesisClientLease> implements IKinesisClientLeaseManager {

    @SuppressWarnings("unused")
    private static final Log LOG = LogFactory.getLog(KinesisClientLeaseManager.class);

    /**
     * Constructor.
     * 
     * @param table Leases table
     * @param dynamoDBClient DynamoDB client to use
     */
    public KinesisClientLeaseManager(String table, AmazonDynamoDB dynamoDBClient) {
        this(table, dynamoDBClient, false);
    }

    /**
     * Constructor for integration tests - see comment on superclass for documentation on setting the consistentReads
     * flag.
     * 
     * @param table leases table
     * @param dynamoDBClient DynamoDB client to use
     * @param consistentReads true if we want consistent reads for testing purposes.
     */
    public KinesisClientLeaseManager(String table, AmazonDynamoDB dynamoDBClient, boolean consistentReads) {
        super(table, dynamoDBClient, new KinesisClientLeaseSerializer(), consistentReads);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean takeLease(KinesisClientLease lease, String newOwner)
        throws DependencyException, InvalidStateException, ProvisionedThroughputException {
        String oldOwner = lease.getLeaseOwner();

        boolean result = super.takeLease(lease, newOwner);

        if (oldOwner != null && !oldOwner.equals(newOwner)) {
            lease.setOwnerSwitchesSinceCheckpoint(lease.getOwnerSwitchesSinceCheckpoint() + 1);
        }

        return result;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public ExtendedSequenceNumber getCheckpoint(String shardId)
        throws ProvisionedThroughputException, InvalidStateException, DependencyException {
    	ExtendedSequenceNumber checkpoint = null;
        KinesisClientLease lease = getLease(shardId);
        if (lease != null) {
            checkpoint = lease.getCheckpoint();
        }
        return checkpoint;
    }
}
