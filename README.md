Dropwizard Kinesis
===================
*Why doesn't this exist already...*

[![Build Status](https://travis-ci.org/code-monastery/dropwizard-kinesis.svg?branch=master)](https://travis-ci.org/code-monastery/dropwizard-kinesis)
[![Maven Central](https://maven-badges.herokuapp.com/maven-central/io.codemonastery/dropwizard-kinesis/badge.svg)](https://maven-badges.herokuapp.com/maven-central/io.codemonastery/dropwizard-kinesis)

Kinesis (+DynamoDB) configuration, metrics, health-checks and lifecycle management integrated with dropwizard, focused on common use cases. Inspired by [dropwizard-core](https://github.com/dropwizard/dropwizard/tree/master/dropwizard-core) and [dropwizard-extra](//github.com/datasift/dropwizard-extra), depends on [Amazon Kinesis Client Library](https://github.com/awslabs/amazon-kinesis-client).

Configuration
-----
Configuration follows the dropwizard pattern that configuration classes are also factories for the classes they configure.

There are a few classes to configure, and some you'll need to implement, but before you do that you need to determine how you provide aws credentials to the aws clients!
The client classes require an [AWSCredentialsProvider](https://github.com/aws/aws-sdk-java/blob/master/aws-java-sdk-core/src/main/java/com/amazonaws/auth/AWSCredentialsProvider.java), such as [DefaultAWSCredentialsProviderChain](https://github.com/aws/aws-sdk-java/blob/master/aws-java-sdk-core/src/main/java/com/amazonaws/auth/DefaultAWSCredentialsProviderChain.java).
If you are using container based virtualization you can easily mount credentials or use environment variables.
If you prefer to use dropwizard configuration, you can use [AwsCredentialsFactory](src/main/java/io/codemonastery/dropwizard/kinesis/AwsCredentialsFactory.java). 

After configuring you credentials you can then configure aws kinesis/dynamodb clients, and use the clients to configure consumers/producers. 
* To produce or consume to kinesis you'll need an [AmazonKinesis](https://github.com/aws/aws-sdk-java/blob/master/aws-java-sdk-kinesis/src/main/java/com/amazonaws/services/kinesis/AmazonKinesis.java) client. You can create one via [KinesisFactory](src/main/java/io/codemonastery/dropwizard/kinesis/KinesisFactory.java). 
* To consume from kinesis you'll need an [AmazonDynamoDB](https://github.com/aws/aws-sdk-java/blob/master/aws-java-sdk-dynamodb/src/main/java/com/amazonaws/services/dynamodbv2/AmazonDynamoDB.java) client which is used to coordinate between peered consumers and save offsets. You can create one via [DynamoDbFactory](src/main/java/io/codemonastery/dropwizard/kinesis/DynamoDbFactory.java). 
* Producers are easy to configure via the [ProducerFactory](src/main/java/io/codemonastery/dropwizard/kinesis/producer/ProducerFactory.java). There are two types of producers: simple and buffered. By default you'll get buffered. You'll also need to determine how to encode your events... see below.
* Consumers are more difficult to configure, as you'll need both kinesis and dynamodb clients and will need to implement event decoding and event processing. By default [ConsumerFactory](src/main/java/io/codemonastery/dropwizard/kinesis/consumer/ConsumerFactory.java) will just print out records. Moreover, if you do special anonymous subclass trick, it will use [ObjectMapper](https://github.com/FasterXML/jackson-databind/blob/master/src/main/java/com/fasterxml/jackson/databind/ObjectMapper.java).
 
An example of minimal configuration for both producer and consumer:
``` yaml
producer:
    streamName: test-stream

consumer:
    streamName: test-stream

```

Corresponding application configuration class:
``` java
public class ReadmeConfiguration extends Configuration {

    @Valid
    @NotNull
    private KinesisFactory kinesis = new KinesisFactory();

    @Valid
    @NotNull
    private DynamoDbFactory dynamoDb = new DynamoDbFactory();

    //no need to initialize since we can choose between simple and buffered produces via configuration
    @Valid
    @NotNull
    private ProducerFactory<Event> producer;

    //the anonymous subclass here allows us to infer json parsing for the Event class
    @Valid
    @NotNull
    private ConsumerFactory<Event> consumer = new ConsumerFactory<Event>(){};

    @JsonProperty
    public KinesisFactory getKinesis() {
        return kinesis;
    }

    @JsonProperty
    public void setKinesis(KinesisFactory kinesis) {
        this.kinesis = kinesis;
    }

    @JsonProperty
    public DynamoDbFactory getDynamoDb() {
        return dynamoDb;
    }

    @JsonProperty
    public void setDynamoDb(DynamoDbFactory dynamoDb) {
        this.dynamoDb = dynamoDb;
    }

    @JsonProperty
    public ProducerFactory<Event> getProducer() {
        return producer;
    }

    @JsonProperty
    public void setProducer(ProducerFactory<Event> producer) {
        this.producer = producer;
    }

    @JsonProperty
    public ConsumerFactory<Event> getConsumer() {
        return consumer;
    }

    @JsonProperty
    public void setConsumer(ConsumerFactory<Event> consumer) {
        this.consumer = consumer;
    }
}
```

Complete Configuration example:
``` yaml
kinesis:
  region: "US_WEST_2"
  client:
    userAgent: "aws-sdk-java/1.10.52 Mac_OS_X/10.10.5 Java_HotSpot(TM)_64-Bit_Server_VM/25.45-b02/1.8.0_45"
    maxErrorRetry: -1
    localAddress: null
    protocol: "HTTPS"
    proxyHost: null
    proxyPort: -1
    proxyUsername: null
    proxyPassword: null
    proxyDomain: null
    proxyWorkstation: null
    preemptiveBasicProxyAuth: false
    maxConnections: 50
    socketTimeout: 50000
    connectionTimeout: 50000
    requestTimeout: 0
    clientExecutionTimeout: 0
    signerOverride: null
    connectionTTL: -1
    connectionMaxIdleMillis: 60000
    responseMetadataCacheSize: 50
    useExpectContinue: true
    apacheHttpClientConfig:
      sslSocketFactory: null
    socketBufferSizeHints:
    - 0
    
dynamoDb:
  region: "US_WEST_2"
  client:
    userAgent: "aws-sdk-java/1.10.52 Mac_OS_X/10.10.5 Java_HotSpot(TM)_64-Bit_Server_VM/25.45-b02/1.8.0_45"
    maxErrorRetry: -1
    localAddress: null
    protocol: "HTTPS"
    proxyHost: null
    proxyPort: -1
    proxyUsername: null
    proxyPassword: null
    proxyDomain: null
    proxyWorkstation: null
    preemptiveBasicProxyAuth: false
    maxConnections: 50
    socketTimeout: 50000
    connectionTimeout: 50000
    requestTimeout: 0
    clientExecutionTimeout: 0
    signerOverride: null
    connectionTTL: -1
    connectionMaxIdleMillis: 60000
    responseMetadataCacheSize: 50
    useExpectContinue: true
    apacheHttpClientConfig:
      sslSocketFactory: null
    
producer:
  type: buffered
  streamName: "test-stream"
  create: null
  maxBufferSize: 1000
  flushPeriod: "10 seconds"
  
consumer:
  streamName: "test-stream"
  create: null
  applicationName: null
  workerId: null
  initialPositionInStream: "LATEST"
  failOverTime: "10000 milliseconds"
  maxRecords: 10000
  idleTimeBetweenReads: "1000 milliseconds"
  callIfEmpty: false
  parentShardPollInterval: "10000 milliseconds"
  shardSyncInterval: "60000 milliseconds"
  cleanupLeasesOnShardCompletion: true
  taskBackoffTime: "500 milliseconds"
  validateSequenceNumberBeforeCheckpoint: true
```
