# Study Zookeeper
Below are the demos that I create to study zookeeper.

## TaskAssign
1. There are three daemons
    + Client
        + a. Will create task under /create, like /create/task-00000.
        + b. Will monitor task status under /status.
        + c. For finished task, will delete node under /status.
    + Worker
        + a. Will register itself under /workers.
        + b. Will Monitor /assign/{worker} tasks assigned to this worker
        + c. Will process tasks assigned to this workers, and set task status under /status.
    + Master
        + a. Will compete to become the active master, otherwise monitor the active master status.
        + b. Get/monitor tasks list under /create.
        + c. Get/monitor worker list under /workers
        + d. Do assignment.
        
### Lifecycle 
1. Task
```
create ->               assign ->                     process ->              result.
(/create/task-00000)   (/assign/worker/task-00000)    (/status/task-00000)    (no node)
```
