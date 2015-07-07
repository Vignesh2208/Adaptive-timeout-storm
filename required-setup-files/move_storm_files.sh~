#!/bin/bash

#STORM_RELEASE_PATH='/home/vignesh/Desktop/CS-525/apache-storm-0.9.2-incubating/storm-dist/binary/target/apache-storm-0.9.2-incubating.zip'
STORM_RELEASE_PATH='/home/vignesh/Documents/apache-storm-0.9.2-incubating/storm-dist/binary/target/apache-storm-0.9.2-incubating.zip'
STORM_YAML_PATH='/home/vignesh/.storm/storm.yaml'
STORM_CONFIG_PATH='/home/vignesh/Desktop/CS-525/config_storm.sh'
#STORM_STARTER_PATH='/home/vignesh/Desktop/CS-525/apache-storm-0.9.2-incubating/examples/storm-starter/target/storm-starter-0.9.2-incubating-jar-with-dependencies.jar'
STORM_STARTER_PATH='/home/vignesh/Documents/apache-storm-0.9.2-incubating/examples/storm-starter/target/storm-starter-0.9.2-incubating-jar-with-dependencies.jar'
TIMEOUT_COMPUTE_PATH='/app/home/storm/timeout_compute.py'
ITERATE_FAULT_INJECTOR_PATH='/home/vignesh/Desktop/Fault_injector_iterate.py'
{
gcloud compute copy-files $STORM_RELEASE_PATH nimbus:/tmp/apache-storm-incubating.zip --zone us-central1-a &&
echo "Transferred to Nimbus"
} || {
echo "Nimbus instance is down"
}

{
gcloud compute copy-files $STORM_RELEASE_PATH slave-1:/tmp/apache-storm-incubating.zip --zone us-central1-a &&
echo "Transferred to Slave-1"
} || {
echo "Slave-1 instance is down"
}

{
gcloud compute copy-files $STORM_RELEASE_PATH slave-2:/tmp/apache-storm-incubating.zip --zone us-central1-a &&
echo "Transferred to Slave-2"
} || {

echo "Slave-2 instance is down"
}


{
gcloud compute copy-files $STORM_RELEASE_PATH slave-3:/tmp/apache-storm-incubating.zip --zone us-central1-a &&
echo "Transferred to Slave-3"
} || {
echo "Slave-3 instance is down"
}

{
gcloud compute copy-files $STORM_YAML_PATH nimbus:/tmp/storm.yaml --zone us-central1-a &&
echo "Transferred to storm.yaml to Nimbus"
} || {
echo "Nimbus instance is down could not copy storm.yaml"
}

{
gcloud compute copy-files $STORM_YAML_PATH slave-1:/tmp/storm.yaml --zone us-central1-a &&
echo "Transferred to storm.yaml to Slave-1"
} || {
echo "Slave-1 instance is down could not copy storm.yaml"
}

{
gcloud compute copy-files $STORM_YAML_PATH slave-2:/tmp/storm.yaml --zone us-central1-a &&
echo "Transferred to storm.yaml to Slave-2"
} || {
echo "Slave-2 instance is down could not copy storm.yaml"
}

{
gcloud compute copy-files $STORM_YAML_PATH slave-3:/tmp/storm.yaml --zone us-central1-a &&
echo "Transferred to storm.yaml to Slave-3"
} || {
echo "Slave-3 instance is down could not copy storm.yaml"
}



{
gcloud compute copy-files $STORM_CONFIG_PATH nimbus:/tmp/config_storm.sh --zone us-central1-a &&
echo "Transferred to config_storm.sh to Nimbus"
} || {
echo "Nimbus instance is down could not copy config_storm.sh"
}

{
gcloud compute copy-files $STORM_CONFIG_PATH slave-1:/tmp/config_storm.sh --zone us-central1-a &&
echo "Transferred to config_storm.sh to Slave-1"
} || {
echo "Slave-1 instance is down could not copy config_storm.sh"
}

{
gcloud compute copy-files $STORM_CONFIG_PATH slave-2:/tmp/config_storm.sh --zone us-central1-a &&
echo "Transferred to config_storm.sh to Slave-2"
} || {
echo "Slave-2 instance is down could not copy config_storm.sh"
}

{
gcloud compute copy-files $STORM_CONFIG_PATH slave-3:/tmp/config_storm.sh --zone us-central1-a &&
echo "Transferred to config_storm.sh to Slave-3"
} || {
echo "Slave-3 instance is down could not copy config_storm.sh"
}

{
gcloud compute copy-files $STORM_STARTER_PATH nimbus:/home/vignesh/storm-starter-0.9.2-incubating-jar-with-dependencies.jar --zone us-central1-a &&
echo "Transferred storm starter to Nimbus"
} || {
echo "Nimbus instance is down"
}

{
gcloud compute copy-files $TIMEOUT_COMPUTE_PATH nimbus:/tmp/timeout_compute.py --zone us-central1-a &&
echo "Transferred timeout_compute.py to Nimbus"
} || {
echo "Nimbus instance is down"
}

{
gcloud compute copy-files $TIMEOUT_COMPUTE_PATH slave-1:/tmp/timeout_compute.py --zone us-central1-a &&
echo "Transferred timeout_compute.py to Slave-1"
} || {
echo "Slave-1 instance is down"
}

{
gcloud compute copy-files $TIMEOUT_COMPUTE_PATH slave-2:/tmp/timeout_compute.py --zone us-central1-a &&
echo "Transferred timeout_compute.py to Slave-2"
} || {

echo "Slave-2 instance is down"
}

{
gcloud compute copy-files $TIMEOUT_COMPUTE_PATH slave-3:/tmp/timeout_compute.py --zone us-central1-a &&
echo "Transferred timeout_compute.py to Slave-3"
} || {

echo "Slave-3 instance is down"
}

{
gcloud compute copy-files $ITERATE_FAULT_INJECTOR_PATH nimbus:/home/vignesh --zone us-central1-a &&
echo "Transferred Fault injector iterate to Nimbus"
} || {
echo "Nimbus instance is down"
}





