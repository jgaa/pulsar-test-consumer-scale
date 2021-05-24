# pulsar-test-consumer-scale

Trivial test for Apache Pulsar to test how many subscribers it can handle on one stream.

Initial tests on a workstation with 32 cores and 256 GB RAM, using the standard Pulsar docker image in standalone mode:

```sh
docker run --rm -it --name pulsar \
  -p 6650:6650 \
  -p 8080:8080 \
  -v `pwd`/pulsardata:/pulsar/data \
  apachepulsar/pulsar:2.7.2 \
  bin/pulsar standalone 
```

Trying to push 1000 messages/sec from one producer.

Limiting the number of consumers to 50 per Pulsar Client instance, I could successfully subscribe 700 unique subscribers (in exclusive mode) to my stream. 

When I tried 800 subscribers, Pulsar failed. It could not keep up with the producer, and the test application used virtually no CPU, indicating that not much was going on. Pulsar on the other hand used lots of CPU and allocated lot's of memory, until it failed with a `OutOfDirectMemoryError: failed to allocate 16777216 byte(s) of direct memory (used: 4289190757, max: 4294967296)` message. 

Using just one pulsar client, Pulsar failed at 700 subscribers. 

One thing I notice is that as the number of subscribers rise, Pulsar fails to keep up with the sending of messages when I use 600 subscribers over one Pulsar client instance, the average receive speed drops to ~500 messages/sec. When I limit the connections over one Pulsar Client to 50, the receivers seems to keep up with the producer, and interestingly, the producer is also faster. (The producer always use it's own Pulsar client).

