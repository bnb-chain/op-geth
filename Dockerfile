# Support setting various labels on the final image
ARG COMMIT=""
ARG VERSION=""
ARG BUILDNUM=""

# Build Geth in a stock Go builder container
FROM golang:1.21-alpine as builder

RUN apk add --no-cache build-base libc-dev
RUN apk add --no-cache gcc musl-dev linux-headers git

# Get dependencies - will also be cached if we won't change go.mod/go.sum
COPY go.mod /go-ethereum/
COPY go.sum /go-ethereum/
RUN cd /go-ethereum && go mod download

ADD . /go-ethereum
ENV CGO_CFLAGS="-O -D__BLST_PORTABLE__"
ENV CGO_CFLAGS_ALLOW="-O -D__BLST_PORTABLE__"
RUN cd /go-ethereum && go run build/ci.go install -static ./cmd/geth

# Pull Geth into a second stage deploy alpine container
FROM alpine:latest

# Set user environment variables
ENV GETH_USER=geth
ENV GETH_USER_UID=1001
ENV GETH_USER_GID=1001

# Install required packages and create user
RUN apk add --no-cache ca-certificates bash curl \
    && rm -rf /var/cache/apk/* \
    && addgroup -g ${GETH_USER_GID} ${GETH_USER} \
    && adduser -u ${GETH_USER_UID} -G ${GETH_USER} --no-create-home -D ${GETH_USER} \
    && addgroup ${GETH_USER} tty \
    && sed -i -e "s/bin\/sh/bin\/bash/" /etc/passwd

RUN echo "[ ! -z \"\$TERM\" -a -r /etc/motd ] && cat /etc/motd" >> /etc/bash/bashrc


# Copy the binary from builder stage
COPY --from=builder /go-ethereum/build/bin/geth /usr/local/bin/

# Create necessary directories and set permissions
RUN mkdir -p /db && \
    chown -R ${GETH_USER}:${GETH_USER} /db && \
    chmod 755 /db


# Switch to non-root user
USER ${GETH_USER}

# Set working directory
WORKDIR /db

EXPOSE 8545 8546 30303 30303/udp
ENTRYPOINT ["geth"]

# Add some metadata labels to help programatic image consumption
ARG COMMIT=""
ARG VERSION=""
ARG BUILDNUM=""

LABEL commit="$COMMIT" version="$VERSION" buildnum="$BUILDNUM"
