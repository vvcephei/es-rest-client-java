es-client-java
==============

In case the rest of this readme doesn't make it clear, this is a pre-release version of this project. I'm seeking input from the Elasticsearch devs (and anyone else who cares to comment). Feel free to send me GH Issues or comment on the original issue: https://github.com/elastic/elasticsearch/issues/7743.

This project is part proof of concept and part initial implementation of a REST+JSON elasticsearch client for Java.

Most Java projects use either the Node or Transport client or the Jest client. Each of these has some major drawbacks:

* The "native" ES clients use Java serialization, which is sensitive to changes in the JVM, sometimes from build to build. If you're using these clients, you should try to use
  exactly the same JVM on the client and the server.
* The Jest client does not implement the ```org.elasticsearch.client.Client``` interface, so you can't switch easily among Http, Transport, and Node clients.

The idea of this project is to address both of those concerns by implementing the Client interface and sending requests over HTTP.

See this issue for some background: https://github.com/elastic/elasticsearch/issues/7743

State and Goals
-----

[![build status](https://travis-ci.org/vvcephei/es-client-java.svg?branch=master)](https://travis-ci.org/vvcephei/es-client-java)

It will take a while to implement the whole ES api, so I'm publicising the project with just a few methods implemented. I'm hoping to accomplish two things:

1. Get some feedback on the approach and implementation. I've just hacked several things out to just get something working. Please give me your feedback on how to do things idiomatically (or even correctly, if I've really hosed it up).
2. Elicit some help in implementing the methods you care about. I feel that Get, Index, Deletes, and Search will cover the majority of use cases, so we should start with these, but if you need something else done, I'd love to help you do it.

I want to make sure that if a method is implemented, it is correct. So the stuff in org.elasticsearch.action should not make any awkward compromises.
Even if the whole client doesn't make it into the main ES repo, I think this stuff may belong there (alongside the actual REST endpoints).

Implemented:

* get
* index
* delete
* search (some minor parts of search objects are not serialized in the API and cannot be inferred, so they are not implemented. They are clearly marked in the code with FIXMEs, and I'll fix them later with PRs to ES. They really are minor, so I don't think you'll be bitten by them.)


|Version|Notes|
|-------|-----|
|0.1PRE1|Just an initial peek at the project. Get, Index, Delete, and Search (with the exception of aggregations) are implemented. The client is created just for a single node.|
|0.1PRE2|Search is fully implemented (including aggregations). The client is still a single-node client: See [Issue #4](https://github.com/vvcephei/es-client-java/issues/4).|

### RestExecutor?
As far as the actual client goes, I feel that the ES client should not configure its own Http client. There are a lot of choices and configurations for Java HTTP clients,
and I don't want to make those choices for you. The core client defines an SPI that covers everything it needs to do over HTTP.

* ```com.bazaarvoice.elasticsearch.client.core.spi.RestExecutor``` is an abstraction of something that makes asynch REST requests.
* ```com.bazaarvoice.elasticsearch.client.core.spi.RestResponse``` is an abstraction of... REST responses.

Other modules can implement these however they please. I'm providing a Jersey implementation right now, just because it's easy to do a basic configuration.

### Thickness of the client

I'm aware that it's typical for the ES client to handle state management tasks like sniffing and round-robining. I didn't get into that because just
being able to make requests and get responses is priority #1.

This client is a thin client. You give it an executor, and it uses it to make requests and get responses. If you want to talk to two nodes, I'd guess you would make two clients
and round-robin yourself for now. That said, if you really want sniffing and whatnot done, feel free to take a crack at it. The only thing I care about is that it remain agnostic
to the actual transport-level http client.

### Dependencies

The core module depends only on elasticsearch. This is really important to maintain the broad usability of this project. This is another reason it's a good idea
to pull in the http client in a seperate module.

We don't want to put users in the position of trying to exclude undesired dependencies or versions. With this setup, anyone can quickly provide a custom implementation using
the http client that's already in their dependencies.

### Exports for ES

I've attached the Apache 2.0 licence, the same license that ES uses, and I've pulled the deserializations for InternalSomeObject out into InternalSomeObjectHelper classes, with the intention that ES can copy and paste the InternalSomeObjectHelper.fromXContent() method to live right next to the InternalSomeObject.toXContent() method.

Approach
--------

The approach I've been following is to consult the ES API endpoint corresponding to the method I'm implementing and essentially writing the inverse function for what it does.

So ```org.elasticsearch.action.get.GetRest#act``` is written by essentially doing the opposite of what ```org.elasticsearch.rest.action.get.RestGetAction.handleRequest``` does,
and ```org.elasticsearch.action.get.GetRest.getResponseFunction``` is the inverse of ```org.elasticsearch.index.get.GetResult.toXContent```.

The result is hopefully an idiomatic, performant, and correct implementation of the desired functions.


