# QueueManager

the `QueueManager` class defines logic for managing the queue that
Ziggy uses to track its job workload. this is responsible for 

- safely moving files within/between `waiting` queues
- deleting configs within the `waiting` queue
- (future) accessing the output logs of `inprocess`/`failed` jobs
- (future) editing configs within the `waiting` queues

along with the python class, we need a Flask API to expose the logic within the office network so anyone can manage the queues