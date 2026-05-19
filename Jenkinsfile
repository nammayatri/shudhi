pipeline {
    
    agent {
        kubernetes {
	    	label 'dind-agent'
	    }
    }

    environment {
        PROD_ECR_REGISTRY    = '147728078333.dkr.ecr.ap-south-1.amazonaws.com'
        PROD_REGION          = 'ap-south-1'
        SANDBOX_ECR_REGISTRY = '463356420488.dkr.ecr.ap-south-1.amazonaws.com'
        SANDBOX_REGION       = 'ap-south-1'
        REPO_NAME            = 'shudhi'
        TAG                  = "${env.GIT_COMMIT?.take(7) ?: 'latest'}"
    }

    stages {
        stage('Checkout') {
            steps {
                checkout scm
            }
        }

        stage('Build Images') {
            parallel {
                stage('Sidecar') {
                    steps {
                        sh "docker build -t ${REPO_NAME}:${TAG} ."
                    }
                }
                // stage('Dashboard') {
                //     steps {
                //         sh "docker build -t ${REPO_NAME}-dashboard:${TAG} ./dashboard"
                //     }
                // }
            }
        }

        stage('Push to Prod ECR') {
            when {
                anyOf {
                    branch 'main'
                    buildingTag()
                }
            }
            steps {
                sh "aws ecr get-login-password --region ${PROD_REGION} | docker login --username AWS --password-stdin ${PROD_ECR_REGISTRY}"

                // Sidecar
                sh "docker tag ${REPO_NAME}:${TAG} ${PROD_ECR_REGISTRY}/${REPO_NAME}:${TAG}"
                sh "docker tag ${REPO_NAME}:${TAG} ${PROD_ECR_REGISTRY}/${REPO_NAME}:latest"
                sh "docker push ${PROD_ECR_REGISTRY}/${REPO_NAME}:${TAG}"
                sh "docker push ${PROD_ECR_REGISTRY}/${REPO_NAME}:latest"

                // Dashboard
                // sh "docker tag ${REPO_NAME}-dashboard:${TAG} ${PROD_ECR_REGISTRY}/${REPO_NAME}-dashboard:${TAG}"
                // sh "docker tag ${REPO_NAME}-dashboard:${TAG} ${PROD_ECR_REGISTRY}/${REPO_NAME}-dashboard:latest"
                // sh "docker push ${PROD_ECR_REGISTRY}/${REPO_NAME}-dashboard:${TAG}"
                // sh "docker push ${PROD_ECR_REGISTRY}/${REPO_NAME}-dashboard:latest"
            }
        }

        stage('Push to Sandbox ECR') {
            when {
                anyOf {
                    branch 'main'
                    buildingTag()
                }
            }
            steps {
                sh "aws ecr get-login-password --region ${SANDBOX_REGION} | docker login --username AWS --password-stdin ${SANDBOX_ECR_REGISTRY}"

                // Sidecar
                sh "docker tag ${REPO_NAME}:${TAG} ${SANDBOX_ECR_REGISTRY}/${REPO_NAME}:${TAG}"
                sh "docker tag ${REPO_NAME}:${TAG} ${SANDBOX_ECR_REGISTRY}/${REPO_NAME}:latest"
                sh "docker push ${SANDBOX_ECR_REGISTRY}/${REPO_NAME}:${TAG}"
                sh "docker push ${SANDBOX_ECR_REGISTRY}/${REPO_NAME}:latest"

                // Dashboard
                // sh "docker tag ${REPO_NAME}-dashboard:${TAG} ${SANDBOX_ECR_REGISTRY}/${REPO_NAME}-dashboard:${TAG}"
                // sh "docker tag ${REPO_NAME}-dashboard:${TAG} ${SANDBOX_ECR_REGISTRY}/${REPO_NAME}-dashboard:latest"
                // sh "docker push ${SANDBOX_ECR_REGISTRY}/${REPO_NAME}-dashboard:${TAG}"
                // sh "docker push ${SANDBOX_ECR_REGISTRY}/${REPO_NAME}-dashboard:latest"
            }
        }
    }

    post {
        always {
            sh "docker logout ${PROD_ECR_REGISTRY} || true"
            sh "docker logout ${SANDBOX_ECR_REGISTRY} || true"
        }
        success {
            echo "Images pushed to both ECRs with tag: ${TAG}"
        }
        failure {
            echo 'Build failed.'
        }
    }
}
