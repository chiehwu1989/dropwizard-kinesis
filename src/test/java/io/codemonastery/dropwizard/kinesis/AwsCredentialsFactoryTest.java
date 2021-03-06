package io.codemonastery.dropwizard.kinesis;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.util.StringInputStream;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.dropwizard.Configuration;
import io.dropwizard.configuration.ConfigurationFactory;
import org.junit.Test;

import javax.validation.Valid;
import javax.validation.constraints.NotNull;

import static org.assertj.core.api.Assertions.assertThat;


public class AwsCredentialsFactoryTest {

    public static final class FakeConfiguration extends Configuration {

        @JsonProperty
        @Valid
        @NotNull
        public AwsCredentialsFactory aws;

    }

    @Test
    public void builderMethods() throws Exception {
        AwsCredentialsFactory factory = new AwsCredentialsFactory()
                .accessKey("aaa")
                .secretAccessKey("bbb");

        assertThat(factory.getAccessKey()).isEqualTo("aaa");
        assertThat(factory.getSecretAccessKey()).isEqualTo("bbb");

        factory.refresh(); // does nothing hahaha
    }

    @Test
    public void canConfigure() throws Exception {
        ConfigurationFactory<FakeConfiguration> configurationFactory = ConfigurationFactories.make(FakeConfiguration.class);
        FakeConfiguration configuration = configurationFactory.build(s -> new StringInputStream("aws:\n  accessKey: aaa\n  secretAccessKey: bbb"), "");
        assertThat(configuration).isNotNull();
        assertThat(configuration.aws).isNotNull();
        assertThat(configuration.aws.getAccessKey()).isEqualTo("aaa");
        assertThat(configuration.aws.getSecretAccessKey()).isEqualTo("bbb");

        AWSCredentials credentials = configuration.aws.getCredentials();
        assertThat(credentials.getAWSAccessKeyId()).isEqualTo("aaa");
        assertThat(credentials.getAWSSecretKey()).isEqualTo("bbb");
    }
}
