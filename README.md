# Lobox Assignment

## How to use

1. Configure the rocksdb ``dataDir`` and ``datasets`` paths in application.yml
2. Navigate to the root of the project and run the application with the command: ``./gradlew :bootRun``
3. Wait for importing and indexing to be finished(it will take around 10 minutes on a personal laptop with 8G RAM and SSD drive)
4. Check API documentation on http://localhost:8080/swagger-ui/index.html

## Summary
For performance reasons and also memory limitations, RocksDb has been used in the project.
### Works I would have done if had more time and options:
1. Write integration tests
2. Validation and error handling
2. Combine a relational database with a key value database to have both query flexibility and performance OR make use of something like ElasticSearch