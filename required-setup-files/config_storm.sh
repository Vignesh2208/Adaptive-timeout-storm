#!/bin/bash


sudo unzip /tmp/apache-storm-incubating.zip -d /usr/local
#sudo chown -R storm:storm /usr/local/apache-storm-0.9.2-incubating
sudo rm /usr/local/storm
sudo ln -s /usr/local/apache-storm-0.9.2-incubating /usr/local/storm
sudo cp -f /tmp/storm.yaml /usr/local/storm/conf
sudo ln -s /usr/local/apache-storm-0.9.2-incubating/bin/storm /home/vignesh/storm
#sudo cp /tmp/timeout_compute.py /app/home/storm


