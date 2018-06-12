# CloudEvent Gateway

A simple POC for a Gateway that is routing CloudEvents from different sources to a Kafka topic.


## Input Options

The current version of the Gateway supports input via HTTP and Apache Kafka.

### HTTP input

The gateway supports an `/ce` endpoint that understands the `CloudEvent` format, and publishes it to the Vertx Event bus.

### Kafka input

The gateway listens to a global topic (`gw.global.input.cloudevents`) for incoming events. It than routes them to the 
Eventbus of vert.x

## EventBus filter

The EventBus Consumer receives messages published by the HTTP and Kafka `Verticle`. It than goes ahead and routes
the `CloudEvent` to a topic that matches the event type (e.g. `aws.s3.object.created` or `Microsoft.Storage.BlobCreated`)

## Consuming Clients

Currently the _outbound_ channel is Apache Kafka, hence any consumer can now connect to the GW and receive the JSON
playload for the CloudEvent, to trigger custom processing.
