pipeline {
    agent any
    environment {
        PSQL_LHOST_ADMIN_CRED = credentials('PSQL_LHOST_ADMIN_CRED')
    }
    stages {
        stage ('update database ddl'){
            steps {
                bat 'echo update table ddl'
                bat 'psql -f ./create_tables.sql "postgres://%PSQL_LHOST_ADMIN_CRED_USR%:%PSQL_LHOST_ADMIN_CRED_PSW%@localhost:5432/any"'
                bat 'echo update routines ddl'
                bat 'psql -f ./create_routines.sql "postgres://%PSQL_LHOST_ADMIN_CRED_USR%:%PSQL_LHOST_ADMIN_CRED_PSW%@localhost:5432/any"'
            }
        }
    }
}