# file-gateway

This is a Gateway Framework application used for self-learning.
 
It has two services available for use:

## POLLING_JSON_SOURCE
Periodically poll for files in a named directory, and pushes their contents to Diffusion, one topic per file.

### Application parameters

| Parameter | Mandatory | Default | Description |
|-|:-:|:-:|-|
|`directory` | N | data | Name of the directory to poll files from. |
|`topicRoot` | N | | Path segment prepended to the destination topic name. |
|`stopAfterInitialLoad` | N | false | Stop polling after all files have be read once. |
|`deleteFiles`| N | false | Delete files after they have been read. |
|`recordPerLine` | N | false | If the input file(s) contain multiple lines, treat each line as a separate record. |


## FILE_STRING_SINK
Subscribes to topics and writes the data to one or more files.

| Parameter   | Mandatory | Default | Description                                                                                                                                                                           |
|-------------|:-:|:-:|---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `directory` |Y|| Directory into which files will be written                                                                                                                                            |
| `filename`  |N|Topic name| The name of the file to write to, or if left empty, generated from the topic path.                                                                                                    |
| `overwrite` |N|false| Overwrite or append to the file on each topic update                                                                                                                                  |
| `flush`     |N|true| Flush the file contents to disk on each topic update                                                                                                                                  |
| `newline`   |N|See description| Append a newline to the file on each topic update. If not specified, it will default to `true` if updates are appended to the file, and `false` if the file is overwritten each time. |
| `cache`     |N|true|If true, internally cache and use file writers. Otherwise, open a new file writer for each topic update and close after writing.|

## Running the application

``` shell
java -Dgateway.config.use-local-services=true -Dgateway.config.file=src/main/resources/configuration.json -jar target/file-gateway-1.0-SNAPSHOT-jar-with-dependencies.jar 
```
