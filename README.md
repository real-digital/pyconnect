# TBD

## Known issues
* Kafkacat and the Producer will try to reach the test cluster via "localhost" but will get the 
response that they should connect to "broker", which isn't resolvable outside of the docker-compose environment.
A simple fix is to just add "broker" as an alias for "localhost" in `/etc/hosts`.