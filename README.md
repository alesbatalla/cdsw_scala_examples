# Getting Started with Spark

This baseline project shows how to interact with Spark on Yarn in Scala
on Cloudera Data Science Workbench. To begin, start a new session with a Scala-Spark engine.

## Accessing Spark
An active spark context will automatically be initiated at session startup and saved to the
variable `sc`.

## Resources and Jars

This workbench utilizes the Apache Toree (incubating) kernel for spark, which
includes outside dependencies or jars with the following magics:

* AddDeps

 * Adds the specified dependencies from Maven Central to the Spark Kernel and Spark cluster

 * Requires the company name, artifact id, and version of the dependency

 * Examples:
    * Adding a single library with all of its dependencies
    ```
    %AddDeps org.scalaj scalaj-http_2.11 2.3.0
    ```
    * Using the programmatic API
    ```scala
    kernel.magics.addDeps("org.scalaj scalaj-http_2.11 2.3.0")
    ```
* AddJar [-f]

  * Adds the specified jars to the Spark Kernel and Spark cluster

  * Requires the path to the jar, which can either be a local path or remote jar hosted via HTTP

  * Including -f will ignore any cached jars and redownload remote jars

  * Examples:
    * Adding a single jar from HTTP and forcing redownload if cached
    ```
    %AddJar http://example.com/some_lib.jar -f
    ```
    * Adding a single jar from the file system relative to the kernel
    ```
    %AddJar file:/path/to/some/lib.jar
    ```
    * Using the programmatic API
    ```scala
    kernel.magics.addJar("http://example.com/some_lib.jar -f")
    ```

## Files

Modify the default files to get started with your own project.

* `README.md` -- This project's readme in Markdown format.
* `auction-analysis.scala` -- A simple analysis of ebay auctions in scala
* `pi.scala` -- Calculate pi using Monte Carlo estimation
* `/examples` -- A set of example scripts based on [Apache Spark Examples](https://github.com/apache/spark/tree/master/examples/src/main/scala/org/apache/spark/examples)
* `/data` -- datasets used for the above examples
```
