# Lobox Assignment

## How to use

1. Configure the datasets paths in application.yml
2. Navigate to the root of the project and run the application with the command: ``./gradlew :bootRun``

## Endpoints

- Get titles in which both director and writer are the same person and he/she is still alive:
    - Get first page with default page size: ``curl 'http://localhost:8080/titles/written-directed-same-alive-person'``
    - Get subsequent pages: ``curl http://localhost:8080/titles/written-directed-same-alive-person?pageSize=5&lastKey=tt0000607``
    - > Note: The ``lastKey`` query param is the previous page last item id 