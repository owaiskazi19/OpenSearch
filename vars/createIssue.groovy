void call(Map args = [:]) {
    String ISSUE_TITLE = "Jenkins Randomized Gradle Check Failed on"
    withCredentials([usernamePassword(credentialsId: "${GITHUB_BOT_TOKEN_NAME}")]) {
        sh([
            'curl',
            '-XPOST',
            '--header "Content-Type: text/x-markdown; charset=utf-8"',
            "--data '{\"title\":\"'${ISSUE_TITLE}'\",\"body\":\"@opensearch-project/opensearch-coreteam,pleasetakealookatBuild'${BUILD_NUMBER}'ingradle_checklogformoreinformation.\",\"labels\":[\"cicd\",\"bug\"]}'",
            "https://api.github.com/repos/owaiskazi19/S3Repo/issues?state=all"
        ])
    }
}
