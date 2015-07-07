#!/bin/bash


LOCAL_STORM_TOPOLOGY_PATH='/home/vignesh/Desktop/Projects/CS-525-Project/apache-storm-0.9.2-incubating/examples/storm-starter/target/storm-starter-0.9.2-incubating-jar-with-dependencies.jar'
DST_TOPOLOGY_PATH='/users/babu3/downloads/scripts'

REMOTE_IP=$1

echo "Transferring topology files"
sudo scp $LOCAL_STORM_TOPOLOGY_PATH babu3@$REMOTE_IP:$DST_TOPOLOGY_PATH

echo "Transfer complete"
