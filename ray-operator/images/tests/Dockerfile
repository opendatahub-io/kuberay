FROM golang:1.24

ENV KUBECONFIG=/kuberay/tests/.kube/config

# Set the working directory inside the container
WORKDIR /kuberay/tests

# Download the latest OpenShift CLI binary
RUN wget -q https://mirror.openshift.com/pub/openshift-v4/clients/ocp/stable-4.18/openshift-client-linux.tar.gz -P oc-client && \
    tar -xf oc-client/openshift-client-linux.tar.gz -C oc-client && \
    cp oc-client/oc /usr/local/bin && \
    rm -rf oc-client/

COPY ray-operator/go.mod ray-operator/go.sum ./

# Download all dependencies
RUN go mod download && \
    go install gotest.tools/gotestsum@latest

COPY ray-operator .

COPY ray-operator/images/tests/.env-odh .env-odh

COPY ray-operator/images/tests/run-tests.sh .

# Jenkins shift-left runs the container as a non-root uid
# (--userns=keep-id --user 1001:1001). JUnit is written under
# /kuberay/tests/results (see kuberay main.yaml resultsDir).
# Go cache/tmp live under HOME in run-tests.sh (/tmp/kuberay-test-home).
RUN mkdir -p /kuberay/tests/results && \
    chmod -R a+rwx /kuberay/tests/results && \
    chmod +x /kuberay/tests/run-tests.sh

# Command to run the tests
ENTRYPOINT ["bash", "run-tests.sh"]
