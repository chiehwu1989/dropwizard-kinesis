Dropwizard Kinesis
===================
*Why doesn't this exist already...*

[![Build Status](https://travis-ci.org/code-monastery/dropwizard-kinesis.svg?branch=master)](https://travis-ci.org/code-monastery/dropwizard-kinesis)
[![Maven Central](https://maven-badges.herokuapp.com/maven-central/io.codemonastery/dropwizard-kinesis/badge.svg)](https://maven-badges.herokuapp.com/maven-central/io.codemonastery/dropwizard-kinesis)

Kinesis (+DynamoDB) configuration, metrics, health-checks and lifecycle management integrated with dropwizard, focused on common use cases. Inspired by [dropwizard-core](https://github.com/dropwizard/dropwizard/tree/master/dropwizard-core) and [dropwizard-extra](//github.com/datasift/dropwizard-extra), depends on [Amazon Kinesis Client Library](https://github.com/awslabs/amazon-kinesis-client).

Configuration
-----
Configuration follows the dropwizard pattern - configuration classes are also factories for the classes they configure.

There are a few classes to configure, and some you'll need to implement, but before you do that you need to determine how you provide aws credentials to the aws clients!
The client classes require an [AWSCredentialsProvider](https://github.com/aws/aws-sdk-java/blob/master/aws-java-sdk-core/src/main/java/com/amazonaws/auth/AWSCredentialsProvider.java), such as [DefaultAWSCredentialsProviderChain](https://github.com/aws/aws-sdk-java/blob/master/aws-java-sdk-core/src/main/java/com/amazonaws/auth/DefaultAWSCredentialsProviderChain.java).
If you are using container based virtualization you can easily mount credentials or use environment variables.
If you prefer to use dropwizard configuration, you can use [AwsCredentialsFactory](src/main/java/io/codemonastery/dropwizard/kinesis/AwsCredentialsFactory.java). 

After configuring your credentials you can then configure aws kinesis/dynamodb clients, and use the clients to configure consumers/producers. 
* To produce or consume to kinesis you'll need an [AmazonKinesis](https://github.com/aws/aws-sdk-java/blob/master/aws-java-sdk-kinesis/src/main/java/com/amazonaws/services/kinesis/AmazonKinesis.java) client. You can create one via [KinesisFactory](src/main/java/io/codemonastery/dropwizard/kinesis/KinesisFactory.java). 
* To consume from kinesis you'll need an [AmazonDynamoDB](https://github.com/aws/aws-sdk-java/blob/master/aws-java-sdk-dynamodb/src/main/java/com/amazonaws/services/dynamodbv2/AmazonDynamoDB.java) client which is used to coordinate between peered consumers and save offsets. You can create one via [DynamoDbFactory](src/main/java/io/codemonastery/dropwizard/kinesis/DynamoDbFactory.java). 
* Producers are easy to configure via the [ProducerFactory](src/main/java/io/codemonastery/dropwizard/kinesis/producer/ProducerFactory.java). There are two types of producers: simple and buffered. By default you'll get buffered. You'll also need to determine how to encode your events... see below.
* Consumers are more difficult to configure, as you'll need both kinesis and dynamodb clients and will need to implement event decoding and event processing. By default [ConsumerFactory](src/main/java/io/codemonastery/dropwizard/kinesis/consumer/ConsumerFactory.java) will make consumers that log records. In addition, if you do a special anonymous subclass trick in your configuration class it will use [ObjectMapper](https://github.com/FasterXML/jackson-databind/blob/master/src/main/java/com/fasterxml/jackson/databind/ObjectMapper.java) for default event decoding.
 
An example minimal configuration for an application with both a producer and consumer:
``` yaml
producer:
    streamName: test-stream

consumer:
    streamName: test-stream

```

For all configurations see [Complete-Configuration](/../../wiki/Complete-Configuration) or see class [ExampleConfiguration](src/test/java/io/codemonastery/dropwizard/kinesis/example/ExampleConfiguration.java). To see how the configuration could be used to create producers and consumers, look at [ExampleApplication](src/test/java/io/codemonastery/dropwizard/kinesis/example/ExampleApplication.java).

Event Consumer
-----
To meaningfully consume events you'll need to implement [EventConsumer](src/main/java/io/codemonastery/dropwizard/kinesis/consumer/EventConsumer.java) and a [Supplier](https://docs.oracle.com/javase/8/docs/api/java/util/function/Supplier.html) for that consumer.
Whenever a event is successfully consumed the EventConsumer should return true. If the event was not successfully consumed, return false. The event will be consumed later, and hopefully next time it will be successful. Important note: if the is never successfully consumed by the application it will halt processing of events from that shard, so in some cases you may want to return true regardless of outcome.

Event Encoder/Decoder
-----
By default the producer/consumer will try to encode/decode events to/from json using Jackson [ObjectMapper](https://github.com/FasterXML/jackson-databind/blob/master/src/main/java/com/fasterxml/jackson/databind/ObjectMapper.java), but only because that is a dependency of dropwizard-core. Naturally you'll want to use some other technique. For an example take a look at [EventObjectMapper](src/main/java/io/codemonastery/dropwizard/kinesis/EventObjectMapper.java).

There is one serious gotcha with the default EventDecoder: the EventDecoder cannot infer how to decode objects unless the ConsumerFactory field was a anonymous subclass with the correct type information. Moreover, you should make sure any new consumer factories inherit default decoding from the previous consumer factory. Example:
``` java
    // the anonymous subclass here allows us to infer json parsing for the Event class
    @Valid
    @NotNull
    private ConsumerFactory<Event> consumer = new ConsumerFactory<Event>(){}; // <= note the {}
    
    @JsonProperty
    public ConsumerFactory<Event> getConsumer() {
        return consumer;
    }

    @JsonProperty
    public void setConsumer(ConsumerFactory<Event> consumer) {
        // because dropwizard configuration will possibly replace our anonmymous consumer
        // we need to inform new consumer to inheritc decoder if necessary
        if(consumer != null){
            consumer.inheritDecoder(this.consumer);
        }
        this.consumer = consumer;
    }
```

Latest vs Trim Horizon
-----
There are many configurations to pay attention to, but perhaps the most important consumer configuration is initialPositionInStream, which needs to either be "TRIM_HORIZON" or "LATEST". When a consumer is created for the very first time, identified by the applicationName configuration, the consumer will decide where in the stream to start reading from based on initialPositionInStream. LATEST will cause a consumer to start immediately after the latest published record, while TRIM_HORIZON will cause a consumer to start reading from the oldest record still in kinesis. Example configuration: 
``` yaml
consumer:
    streamName: test-stream
    initialPositionInStream: "TRIM_HORIZON"
```
