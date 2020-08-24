def slackcolor = 'good'
def githash
def failpointPath = "github.com/pingcap/gofail"
def failpoint = "gofail"
def goimportsPath = "golang.org/x/tools/cmd/goimports"
def goimports = "goimports"

if (params.containsKey("release_test")) {
    ghprbTargetBranch = params.getOrDefault("release_test__ghpr_target_branch", params.release_test__release_branch)
    ghprbCommentBody = params.getOrDefault("release_test__ghpr_comment_body", "")
    ghprbActualCommit = params.getOrDefault("release_test__ghpr_actual_commit", params.release_test__tidb_commit)
    ghprbPullId = params.getOrDefault("release_test__ghpr_pull_id", "")
    ghprbPullTitle = params.getOrDefault("release_test__ghpr_pull_title", "")
    ghprbPullLink = params.getOrDefault("release_test__ghpr_pull_link", "")
    ghprbPullDescription = params.getOrDefault("release_test__ghpr_pull_description", "")
}

def pd_url = "${FILE_SERVER_URL}/download/builds/pingcap/pd/pr/${ghprbActualCommit}/centos7/pd-server.tar.gz"

try {
    stage('Prepare') {failpointPath
        node ("${GO_BUILD_SLAVE}") {
            def ws = pwd()
            deleteDir()
            dir("go/src/github.com/pingcap/pd") {
                container("golang") {
                    timeout(30) {
                        sh """
                        pwd
                        while ! curl --output /dev/null --silent --head --fail ${pd_url}; do sleep 15; done
                        sleep 5
                        curl ${pd_url} | tar xz
                        rm -rf ./bin
                        export GOPATH=${ws}/go
                        # export GOPROXY=http://goproxy.pingcap.net
                        go list ./...
                        go list ./... > packages.list
                        cat packages.list
                        split packages.list -n r/5 packages_unit_ -a 1 --numeric-suffixes=1
                        echo 1
                        cat packages_unit_1
                        """
                        if (ghprbTargetBranch == "release-2.1") {
                            sh """
                            GO111MODULE=off GOPATH=${ws}/go go get -v ${failpointPath}
                            find . -type d | grep -vE "(\\.git|\\.retools)" | GOPATH=${ws}/go xargs ${ws}/go/bin/${failpoint} enable
                            """
                        }
                        if (ghprbTargetBranch == "release-3.0" || ghprbTargetBranch == "release-3.1") {
                            sh """
                            make retool-setup
                            make failpoint-enable
                            """
                        }
                        if (ghprbTargetBranch == "release-4.0" || ghprbTargetBranch == "master") {
                            sh """
                            make failpoint-enable
                            make deadlock-enable
    						"""
                        }
                    }
                }
            }

             stash includes: "go/src/github.com/pingcap/pd/**", name: "pd"
        }
    }

    stage('Unit Test') {
        def run_unit_test = { chunk_suffix ->
           node("${GO_TEST_SLAVE}") {
                def ws = pwd()
                deleteDir()
                unstash 'pd'

                 dir("go/src/github.com/pingcap/pd") {
                    container("golang") {
                        timeout(30) {
                            sh """
                               set +e
                               killall -9 -r tidb-server
                               killall -9 -r tikv-server
                               killall -9 -r pd-server
                               rm -rf /tmp/pd
                               set -e
                               cat packages_unit_${chunk_suffix}
                            """
                            if (fileExists("go.mod")) {
                               sh """
                               mkdir -p \$GOPATH/pkg/mod && mkdir -p ${ws}/go/pkg && ln -sfT \$GOPATH/pkg/mod ${ws}/go/pkg/mod || true
                               GOPATH=${ws}/go CGO_ENABLED=1 GO111MODULE=on go test -race -cover \$(cat packages_unit_${chunk_suffix})
                               """
                            } else {
                                sh """
                               GOPATH=${ws}/go CGO_ENABLED=1 GO111MODULE=off go test -race -cover \$(cat packages_unit_${chunk_suffix})
                               """
                            }
                        }
                    }
                }
            }
        }
        def tests = [:]

        tests["Unit Test Chunk #1"] = {
            run_unit_test(1)
        }

        tests["Unit Test Chunk #2"] = {
            run_unit_test(2)
        }

        tests["Unit Test Chunk #3"] = {
            run_unit_test(3)
        }

        tests["Unit Test Chunk #4"] = {
            run_unit_test(4)
        }

        tests["Unit Test Chunk #5"] = {
            run_unit_test(5)
        }

        parallel tests
    }

     currentBuild.result = "SUCCESS"
} catch (Exception e) {
    currentBuild.result = "FAILURE"
    slackcolor = 'danger'
    echo "${e}"
}

stage('Summary') {
   echo "Send slack here ..."
   def duration = ((System.currentTimeMillis() - currentBuild.startTimeInMillis) / 1000 / 60).setScale(2, BigDecimal.ROUND_HALF_UP)
   def slackmsg = "[#${ghprbPullId}: ${ghprbPullTitle}]" + "\n" +
   "${ghprbPullLink}" + "\n" +
   "${ghprbPullDescription}" + "\n" +
   "Unit Test Result: `${currentBuild.result}`" + "\n" +
   "Elapsed Time: `${duration} mins` " + "\n" +
   "${env.RUN_DISPLAY_URL}"

    if (currentBuild.result != "SUCCESS") {
       slackSend channel: '#jenkins-ci', color: 'danger', teamDomain: 'pingcap', tokenCredentialId: 'slack-pingcap-token', message: "${slackmsg}"
   }
}