[![GoDoc](https://godoc.org/github.com/altairsix/eventsource?status.svg)](https://godoc.org/github.com/altairsix/eventsource) ![Travis CI](https://travis-ci.org/altairsix/eventsource.svg?branch=master) 

```eventsource``` is a Serverless event sourcing library for Go that attempts to 
leverage the capabilities of AWS to simplify the development and operational 
requirements for event sourcing projects.

This library is still under development and changes to the core api are likely.

Take advantage of the scalability, high availability, clustering, and strong 
security model you've come to know and love with AWS.
 
Serverless and accessible were significant design considerations in the creation of this 
library.  What AWS can handle, I'd rather have AWS handle.

## Installation

```
go get github.com/altairsix/eventsource/...
```

## Getting Started

The easiest way to get started is to check out the _examples directory.  gopherfest has the 
most complete example.  Before you run the gopherfest example, you'll need to (a) ensure you 
have AWS credentials loaded into your environment and (b) then run:

```
eventsource dynamodb create-table --name orders --region us-west-2
```

## Key Concepts

Event sourcing is the idea that rather than storing the current state of a domain
model into the database, you can instead store the sequence of events (or facts)
and then rebuild the domain model from those facts.  

git is a great analogy. each commit becomes an event and when you clone or pull
the repo, git uses that sequence of commits (events) to rebuild the project
file structure (the domain model).

Greg Young has an excellent primer on event sourcing that can found on the 
[EventStore docs page](http://docs.geteventstore.com/introduction/4.0.0/event-sourcing-basics/).

![Overview](https://s3.amazonaws.com/site-eventsource/Overview.png)

### Event

Events represent domain events and should be expressed in the past tense such as CustomerMoved,
OrderShipped, or EmailAddressChanged.  These are irrefutable facts that have completed in the 
past.  

Try to avoid sticking derived values into the events as (a) events are long lived and bugs in the
events will cause you great grief and (b) business rules change over time, sometimes retroactively.

### Aggregate

The Aggregate (often called Aggregate Root) represents the domain modeled by the bounded context
and represents the current state of our domain model.

### Repository

Provides the data access layer to store and retrieve events into a persistent store.

### Store

Represents the underlying data storage mechanism.  eventsource only supports dynamodb out of the
box, but there's no reason future versions could not support other database technologies like
MySQL, Postgres or Mongodb. 

### Serializer

Specifies how events should be serialized.  eventsource currently uses simple JSON serialization
although I have some thoughts to support avro in the future.

### CommandHandler

CommandHandlers are responsible for accepting (or rejecting) commands and emitting events.  By
convention, the struct that implements Aggregate should also implement CommandHandler.

### Command

An active verb that represents the mutation one wishes to perform on the aggregate.

### Dispatcher

Responsible for retrieving or instantiates the aggregate, executes the command, and saving the
the resulting event(s) back to the repository.

## Creating dynamodb tables

Eventsource comes with a utility to simplify creating / deleting the dynamodb tables.

```
eventsource dynamodb create-table --name {table-name}
eventsource dynamodb delete-table --name {table-name}
```

## Development

To run the tests locally, execute the following:

```
docker-compose up
export DYNAMODB_ENDPOINT=http://localhost:8080
go test ./...
```

## Testing

The ```scenario``` package simplifies testing.

```go
    scenario.New(t, &Order{}).  // &Order{} implements both Aggregate and CommandHandler
        Given().                // an initial set of events
        When(&CreateOrder{}).   // command is applied
        Then(&OrderCreated{})   // expect the following events to be emitted
```

### Todo 

- [ ] document singleton usage
- [ ] implement dynamodb to sns lambda function
- [ ] implement dynamodb to kinesis firehose lambda function
- [ ] document stream replay via s3
- [ ] add support for terraform in tooling

