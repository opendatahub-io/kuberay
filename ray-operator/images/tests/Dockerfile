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

# Command to run the tests
ENTRYPOINT ["bash", "run-tests.sh"]
