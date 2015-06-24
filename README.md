# Adaptive-timeout-storm

Few things to note :

In "storm-core/src/jvm/backtype/storm/Constants.java"

* Replace  TIMEOUT_FILE_BASE_DIR (original value "/home/vignesh/Clojure_projects/") to whatever path you want. The timeout files (which are read by the timeout-timer) will be created in this path and automatically deleted after the topology is shut-down.

* Compile the source code using 

   mvn clean install -DskipTests=true


Folder descriptions :
---------------------

* apache-storm-0.9.2-incubating :

	The source code containing the dynamic timeout adaption of Storm

* required setup files :

	Files required to setup a storm cluster

* util-scripts : 

	Some utility scripts

* Storm-GCE-setup.txt : 

	Instructions on how to create a new VM instance and all the required software that has to be installed

* Storm-setup-instructions.pdf :

	Instructions on how to setup a storm cluster along with descriptions of the setup files and utility scripts


