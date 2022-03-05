# Rusie Queue

![Rupaul "you better work" meme](./you_better_work.jpeg)


## Examples

First, make sure that Postgres is running:
```
$ docker-compose up -d
```

Next, start the worker:
```
$ cargo run --example hello worker
```

Finally, you can push messages to the worker:
```
$ cargo run --example hello push # Push and use default name to print
$ cargo run --example hello push Rust Ferris # Push with list of names to print
```

## Development

### Setup

Install the SQLx CLI:
```
$ cargo install sqlx-cli
```

Create the DB and run migrations:
```
$ docker-compose up -d # Make sure that PG is running
$ sqlx database create
$ sqlx migrate run
```


### Testing

```
$ docker-compose up -d # Tests require PG
$ cargo test -- --test-threads=1 # Tests currently have to run in a single thread
```
