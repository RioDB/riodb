#!/bin/sh
mvn clean compile assembly:single
cp target/riodb.jar .

