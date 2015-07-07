#!/bin/bash

# Must specify IP address as first argument
LOCAL_STORM_RELEASE_PATH="/home/vignesh/Desktop/Projects/CS-525-Project/apache-storm-0.9.2-incubating/storm-dist/binary/target/apache-storm-0.9.2-incubating.zip"
DST_STORM_PATH='/users/babu3/downloads'
REMOTE_IP=$1

echo "Transferring Storm files"
sudo scp $LOCAL_STORM_RELEASE_PATH babu3@$REMOTE_IP:$DST_STORM_PATH


echo "Transfer complete"
