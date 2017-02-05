#!/bin/bash
spark-submit \
	--class ca.redsofa.jobs.HelloWorld \
	--master local[*] \
  	./target/spark-helloworld-1.0-SNAPSHOT.jar
