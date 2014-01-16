Message Logging System

This file contains instruction for launching RoQ with Message Logging System.

The source code of RoQ with Message Logging System can be fond on the CD-ROM provided with this thesis. The classes of MLS arelocated in :

RoQ-master/roq-core/src/main/java/org/roqmessaging/{log, log/reliability, log/storage}


Requirements for running RoQ with MLS:
Java 1.7
ZMQ 3.2.3
JZMQ 2.2.0

To lunch the RoQ with MLS follow the next steps:

RoQ GCM launcher.
go to /roq/bin and launch:
./startGCM.sh

RoQ Host launcher.
go to /roq/bin and launch:
./startHost.sh

RoQ Log Manager launcher.
go to /roq/bin and launch:
./startLogMgmt.sh

For testing:

RoQ Queue launcher.
java -Djava.library.path=/usr/local/lib -Dlog4j.configuration="file:roq/config/log4j.properties" -cp roq/lib/roq-management-1.0-SNAPSHOT-jar-with-dependencies.jar org.roqmessaging.management.launcher.QueueManagementLauncher <addess of the GCM> add myqueue


RoQ Test launcher.
java -Djava.library.path=/usr/local/lib -Dlog4j.configuration="file:roq/config/log4j.properties" -cp roq/lib/roq-simulation-1.0-SNAPSHOT-jar-with-dependencies.jar org.roqmessaging.loaders.launcher.TestLoaderLauncher test.txt myqueue
