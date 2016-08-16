.. Licensed to the Apache Software Foundation (ASF) under one
.. or more contributor license agreements.  See the NOTICE file
.. distributed with this work for additional information
.. regarding copyright ownership.  The ASF licenses this file
.. to you under the Apache License, Version 2.0 (the
.. "License"); you may not use this file except in compliance
.. with the License.  You may obtain a copy of the License at
..
..     http://www.apache.org/licenses/LICENSE-2.0
..
.. Unless required by applicable law or agreed to in writing, software
.. distributed under the License is distributed on an "AS IS" BASIS,
.. WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
.. See the License for the specific language governing permissions and
.. limitations under the License.

.. highlight:: none

Setting up Cassandra in Eclipse
===============================

Eclipse is a popular open source IDE that can be used for Cassandra development. Various Eclipse environments are available from the `download page <https://www.eclipse.org/downloads/eclipse-packages/>`_. The following guide was created with "Eclipse IDE for Java Developers".

These instructions were tested on Ubuntu 16.04 with Eclipse Neon (4.6) using Cassandra 2.1, 2.2 and 3.x.

Checkout and setup Cassandra
----------------------------

Requirements: Java 8 (C* 2.x must be compatible with Java 7), `Ant <http://ant.apache.org/>`_, Eclipse, Git.

Cassandra is using Git for version control. In this tutorial we will checkout Cassandra from it.

From the console, checkout the code using Git. Here we assume you are checking out the latest trunk, but browse http://git-wip-us.apache.org/repos/asf/cassandra.git for all available versions...

::

   git clone http://git-wip-us.apache.org/repos/asf/cassandra.git cassandra-trunk

   cd cassandra-trunk

   ant build

   ant generate-eclipse-files

The ant build step may take some time as various libraries are downloaded.

**It is important that you generate the Eclipse files with Ant before trying to set up the Eclipse project.**

Setup Eclipse
-------------

 * Start Eclipse.
 * Select ``File->Import->Existing Projects into Workspace->Select git directory``
 * Make sure "cassandra-trunk" is recognized and selected as a project (assuming you checked the code out into the folder cassandra-trunk as described above)
 * Confirm "Finish" to have your project imported

You should now be able to find the project as part of the "Package Explorer" or "Project Explorer" without having Eclipse complain about any errors after building the project automatically.

Unit Tests
----------

Unit tests can be run from Eclipse by simply right-clicking the class file or method and selecting ``Run As->JUnit Test``. Tests can be debugged this way as well by defining breakpoints (double-click line number) and selecting ``Debug As->JUnit Test``.

Alternatively all unit tests can be run from the command line using the ``ant test`` command, ``ant test -Dtest.name=<simple_classname>`` to execute a test suite or ``test testsome -Dtest.name=<FQCN> -Dtest.methods=<testmethod1>[,testmethod2]`` for individual tests.

Debugging Cassandra Using Eclipse
---------------------------------

There are two ways how to start and debug a local Cassandra instance with Eclipse. You can either start Cassandra just as you normally would by using the ``./bin/cassandra`` script and connect to the JVM through `remotely <https://docs.oracle.com/javase/8/docs/technotes/guides/troubleshoot/introclientissues005.html>`_ from Eclipse or start Cassandra from Eclipse right away.

Starting Cassandra From Command Line
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

 * Set environment variable to define remote debugging options for the JVM:
   ``export JVM_EXTRA_OPTS="-agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=1414"``
 * Start Cassandra by executing the ``./bin/cassandra``

Afterwards you should be able to connect to the running Cassandra process through the following steps:

From the menu, select ``Run->Debug Configurations..``

.. image:: images/eclipse_debug0.png

Create new remote application

.. image:: images/eclipse_debug1.png

Configure connection settings by specifying a name and port 1414

.. image:: images/eclipse_debug2.png

Afterwards confirm "Debug" to connect to the JVM and start debugging Cassandra!

Starting Cassandra From Eclipse
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Cassandra can also be started directly from Eclipse if you don't want to use the command line.

From the menu, select ``Run->Run Configurations..``

.. image:: images/eclipse_debug3.png

Create new application

.. image:: images/eclipse_debug4.png

Specify name, project and main class ``org.apache.cassandra.service.CassandraDaemon``

.. image:: images/eclipse_debug5.png

Configure additional JVM specific parameters that will start Cassandra with some of the settings created by the regular startup script. Change heap related values as needed.

::

   -Xms1024M -Xmx1024M -Xmn220M -Xss256k -ea -XX:+UseThreadPriorities -XX:ThreadPriorityPolicy=42 -XX:+UseParNewGC -XX:+UseConcMarkSweepGC -XX:+CMSParallelRemarkEnabled -XX:+UseCondCardMark -javaagent:./lib/jamm-0.3.0.jar -Djava.net.preferIPv4Stack=true

.. image:: images/eclipse_debug6.png

Now just confirm "Debug" and you should see the output of Cassandra starting up in the Eclipse console and should be able to set breakpoints and start debugging!

