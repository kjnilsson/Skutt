Skutt
=====

A high-level messaging API for education and experimentation. 

There are a few general aims for the project:

# Cross platform compatible - Avoid leakign .NET internals into the rabbit mq.
# Aid in the application of Enterprise Integration Patterns (Hophe et. al). Concepts such as Commands, Events and documents and how they should be distributed should be part of the library.
# Fun and play.

Initially it will primarily wrap RabbitMQ but other messaging frameworks may feature. Who knows? :)

Skutt is not meant for production use at this stage, perhaps never. For production use I would recommend EasyNetQ or MassTransit.
