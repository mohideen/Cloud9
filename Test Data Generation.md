**Test Data Generation**

The Apache Pig Data Generator is used to generate the test data for the application. 
The source files for the data generator class is available for download at the Duke University’s Starfish project page. The Starfish project has various Hadoop cluster performance profiling and tuning related resources. The pigmix archive available at the [MR-apps page](http://www.cs.duke.edu/starfish/mr-apps.html) of the project has all the necessary files for running the data generator. 

The pigmix source must be built using the ant build file in the root directory to get the jar file. The pigperf.jar built from the pigmix will have the runnable data generator class. Also, the lib folder in the pigmix will have the sdsuLibJKD12.jar and pig-0.9.0-core.jar files that are essential for running the data generator.  The data generator generates output in text file. The [DataGeneratorHadoop](http://wiki.apache.org/pig/DataGeneratorHadoop) pig wiki page has the full documentation for running the data generator. 

NOTE 1: The pig-0.9.0-core.jar file in the pimix lib folder should be used made available for the program at the runtime. Using different versions of pig may cause runtime exceptions.

NOTE 2: While generating large data set (>1000000000) the map tasks might encounter outofmemory error. The mapred.child.java.opts property should be set to a higher value (eg: -Xmx2000m) to overcome this problem. A custom configuration file can be created for this purpose, and loaded at run time using the -conf option. (See [DataGeneratorHadoop](http://wiki.apache.org/pig/DataGeneratorHadoop))

