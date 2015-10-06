#!/bin/bash
###################################################################################################################################
# Usage: sh run.sh <graphFile> <indexFile> 				  							  #
###################################################################################################################################
ORIGINAL_DIR=$PWD
BASE_DIR="$( cd -P "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
cd $BASE_DIR

#source /etc/profile

VERSION="0.0.1"
VERSION_DEPS="0.0.1"
BASE_NAME=pagerank
GRAPH_FILE=$1
INDEX_FILE=$2
LIB_DIR=$BASE_DIR/target/scala-2.10

JARs="$LIB_DIR/$BASE_NAME-assembly-$VERSION_DEPS-deps.jar $LIB_DIR/$BASE_NAME-assembly-$VERSION.jar"

CLASS="PageRank"

spark-submit \
	--master yarn-cluster \
	--num-executors 1 \
	--driver-memory 250m \
	--executor-memory 250m \
 	--executor-cores 3 \
 	--name $BASE_NAME \
	--class $CLASS \
	--jars $JARs $GRAPH_FILE $INDEX_FILE

cd $ORIGINAL_DIR
