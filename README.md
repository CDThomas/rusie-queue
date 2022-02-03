# Rusie Queue

![Rupaul "you better work" meme](./you_better_work.jpeg)

## Running
```
$ docker-compose up -d # Start PG
$ cargo run # Run the example
```

## Testing

```
$ docker-compose up -d # Tests require PG
$ cargo test -- --test-threads=1 # Tests currently have to run in a single thread
```
