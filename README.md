Dropwizard Kinesis
===================
*Why doesn't this exist already...*

[![Build Status](https://travis-ci.org/code-monastery/dropwizard-kinesis.svg?branch=master)](https://travis-ci.org/code-monastery/dropwizard-kinesis)
[![Maven Central](https://maven-badges.herokuapp.com/maven-central/io.codemonastery/dropwizard-kinesis/badge.svg)](https://maven-badges.herokuapp.com/maven-central/io.codemonastery/dropwizard-kinesis)

Kinesis (+DynamoDB) configuration, metrics, health-checks and lifecycle management integrated with dropwizard, focused on common use cases. Inspired by dropwizard-core and [dropwizard-extra](//github.com/datasift/dropwizard-extra), depends on [Amazon Kinesis Client Library](https://github.com/awslabs/amazon-kinesis-client).

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
* Producers are easy to configure via the [ProducerFactory](src/main/java/io/codemonastery/dropwizard/kinesis/producer/ProducerFactory.java). There are two types of producers: simple and buffered. By default you'll get buffered.
* Consumers are more difficult to configure, as you'll need to implement 
