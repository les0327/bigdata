# Labs for BigData
## How to run
In project dir:
```./gradlew clean build run --args='lab1'```

## Local hadoop
```docker run --name hadoop -p 50070:50070 -p 8088:8088 -p 19888:19888 sequenceiq/hadoop-docker:2.7.1```

## Compile lab2
```./gradlew clean jar```