package io.codemonastery.dropwizard.kinesis.rule;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.kinesis.AmazonKinesis;
import com.amazonaws.services.kinesis.AmazonKinesisClient;
import com.amazonaws.services.kinesis.model.ResourceNotFoundException;
import org.junit.rules.ExternalResource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.Set;
import java.util.UUID;
import java.util.function.Supplier;

public class KinesisClientRule extends ExternalResource implements Supplier<AmazonKinesis> {

    public static final Regions TEST_REGIONS = Regions.US_WEST_2;
    public static final Region TEST_REGION = Region.getRegion(TEST_REGIONS);

    private static final Logger LOG = LoggerFactory.getLogger(KinesisClientRule.class);

    private final Set<String> streamNames = new HashSet<>();
    private AmazonKinesis client;

    public KinesisClientRule() {
    }

    @Override
    protected void before() throws Throwable {
        ClientConfiguration clientConfiguration = new ClientConfiguration();
        client = new AmazonKinesisClient(new DefaultAWSCredentialsProviderChain(), clientConfiguration);
        client.setRegion(TEST_REGION);
    }

    @Override
    protected void after() {

        for (String streamName : streamNames) {
            try {
                client.deleteStream(streamName);
            } catch (ResourceNotFoundException e){
                //this is ok, means it was never created
            }catch (Exception e) {
                LOG.error("Could not delete test stream " + streamName, e);
            }
        }

        streamNames.clear();

        client.shutdown();
        client = null;
    }

    public AmazonKinesis getClient() {
        if(client == null){
            throw new IllegalStateException("CLIENT_RULE is not initialized, did you use the @ClassRule or @Rule annotations?");
        }
        return client;
    }

    @Override
    public AmazonKinesis get() {
        return getClient();
    }

    /**
     * Requests a new random stream name that will be deleted after rule has run
     */
    public String streamName() {
        String name = "test-" + UUID.randomUUID();
        streamNames.add(name);
        return name;
    }
}
