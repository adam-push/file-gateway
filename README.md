# file-gateway

This is a Gateway Framework application used for self-learning.
 
It periodically polls for files in a configured directory, and pushes their contents to Diffusion, one topic per file.

## Running the application

``` shell
java -Dgateway.config.use-local-services=true -Dgateway.config.file=src/main/resources/configuration.json -jar target/file-gateway-1.0-SNAPSHOT-jar-with-dependencies.jar 
```
