
# Description

Spark pipeline processing NYC taxi events to identify best zones for taxi riders depending on day and hour. The job takes 4 arguments:
* The yellow NYC taxi rides source (can be the official NYC taxi bucket)
* The green NYC taxi rides source (can be the official NYC taxi bucket)
* The taxi zone referential data that will be joined with taxi rides
* The target bucket that will contain geo-localized taxi rides and statistics for identifying the most profitable zones for drivers

## Data format

The job follows the data format provided by 2017 and 2018 NYC taxi rides

## Pre-requisite

* Compatibility: Spark 2.4.0, Scala 2.11.12, Sbt 1.2.8


# Building


`sbt assembly`


# Deployment



