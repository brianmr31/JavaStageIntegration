# JavaStageIntegration
simple java integration stage at IBM datastage

If you want to complie, you must have library "ccjava-api.jar", you can find this library in IBM datatage project on your server
mvn clean package

create new project 
mvn archetype:generate -DgroupId=<Package> -DartifactId=<ProjectName> -DarchetypeArtifactId=maven-archetype-quickstart -DarchetypeVersion=1.4 -DinteractiveMode=false
  
Check pom.xml you need to use plugin maven-assembly-plugin to compile all dependencies jar on one jar
