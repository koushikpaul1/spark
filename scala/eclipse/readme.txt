eclipse doesnt have sbt plugin
although sbt has an eclipse plugin
which is a workaround but not proper solution.

which means eclipse doesnt understand build.sbt as it does for pom.xml or build.gradle
so if we change a dependency in our eclipse project in build.sbt file, eclipse will not understand it, and will not change the dependency in the project dependencies.

The workaround is as follows 

Step1:download and install sbt from https://www.scala-sbt.org/release/docs/Setup.html
		for windows it is pretty straight forward  https://piccolo.link/sbt-1.2.8.msi pretty straight forward
Step2:Create a folder say D:\sbt\edge
		now create a build.sbt file here
		You can use the following as the content, change it accordingly
		
	name := "edge"
	version := "0.1"
	scalaVersion := "2.11.8"
	val sparkVersion="2.4.0"
	libraryDependencies ++= Seq(
	"org.apache.spark" %% "spark-core" % sparkVersion,
	"org.apache.spark" %% "spark-sql" % sparkVersion,
	"org.apache.spark" %% "spark-mllib" % sparkVersion % "runtime",
	"org.apache.spark" %% "spark-streaming" % sparkVersion % "provided",
	"org.apache.spark" %% "spark-hive" % sparkVersion % "provided",
	"org.apache.spark" %% "spark-catalyst" % sparkVersion % Test,
	"org.apache.spark" %% "spark-graphx" % sparkVersion,
	"org.apache.spark" %% "spark-repl" % sparkVersion % "provided",
	"org.apache.spark" %% "spark-yarn" % sparkVersion,
	"org.apache.spark" %% "spark-mllib-local" % sparkVersion,
	//"org.apache.spark" %% "spark-streaming-kafka" % "1.6.3",
	//"org.apache.spark" %% "spark-streaming-twitter" % "1.6.3",
	"ch.qos.logback" % "logback-classic" % "1.1.3"
	)

		now create these nested folders src and main like -> D:\sbt\spark\src\main

Step3:open CMD/PowerShell go to D:\sbt\edge 
			run "sbt package"
			
Step4:go inside .sbt in ur home directory and in plugins folder of the correct version in my case C:\Users\xxxx\.sbt\1.0\plugins
			if u have a plugins.sbt file already, add the following line "addSbtPlugin("com.typesafe.sbteclipse" % "sbteclipse-plugin" % "5.2.4")"
			if not, create and add the line.
Step5:Go back to CMD/PowerShell , run 	sbt eclipse. It will create the necessary files for eclipse project.
Step6:Open an eclipse workspace, and import this project as an existing project. 		

		
