# :rocket: CloudEvent Router and Gateway

[![Build Status](https://travis-ci.org/project-streamzi/cloudevents-gateway.png)](https://travis-ci.org/project-streamzi/cloudevents-gateway)
[![License](https://img.shields.io/:license-Apache2-blue.svg)](http://www.apache.org/licenses/LICENSE-2.0)

A simple POC for a Gateway that is routing CloudEvents from different sources to a Kafka topic.


## Input/Source Options

The current version of the Gateway supports input via HTTP and Apache Kafka.

### HTTP source

The gateway supports an `/ce` endpoint that understands the `CloudEvent` format, and publishes it to a Kafka topic, matching the 
`eventType` of the CloudEvent.

### Kafka source

The gateway listens to a global topic (`gw.global.input.cloudevents`) for incoming events. It than routes them to the 
Kafka topic, representing the given `eventType`.

## Sinks

The Gateway is based on Apache Kafka, and all incoming CloudEvents  are routed to a Kafka topic/sink,
representing the event type. 

### HTTP Sink

Publishing all events from a given/configurable `event-type` to a configurable HTTP endpoint.

### Kafka Sink

Any `eventType` topic can be accessed with normal Apache Kafka APIs, such as the Streams API, for processing CloudEvents.
