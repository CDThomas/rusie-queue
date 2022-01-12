# PG Queue

## Running
```
$ docker-compose up -d # Start PG
$ cargo run # Run the example
```

## TODO
* Add tests
* Add example app that demos usage (?)
* Add UI for viewing current state of jobs
    * Similar to Oban's UI
    * Can start with a simple table that requires manual refresh to get new state
    * Something realtime would be fun
        * Could start with a polling impl
        * Yew looks interesting. Plain HTML or React would be easy to get started with.
        * Don't want anything super heavy if I want to release it as a package. That rules
          pretty much rules React out.
