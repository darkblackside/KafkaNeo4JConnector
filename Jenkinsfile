pipeline {
  agent any
  stages {
    stage('InstallDependency') {
      steps { 
        sh 'mvn clean install'
      }
    }
  }
}
