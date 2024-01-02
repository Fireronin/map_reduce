FROM alpine:latest

RUN apk add --no-cache curl && \
    curl -LO "https://storage.googleapis.com/kubernetes-release/release/$(curl -s https://storage.googleapis.com/kubernetes-release/release/stable.txt)/bin/linux/amd64/kubectl" && \
    chmod +x ./kubectl && \
    mv ./kubectl /usr/local/bin/kubectl


# Install protobuf and cargo
RUN apk add --no-cache protobuf cargo

# Copy map_reduce folder
COPY ./map_reduce /home/map/map_reduce

# Run cargo build
RUN cd /home/map/map_reduce && cargo build

# Set the default command to run when starting a container from this image
# CMD ["/home/map/map_reduce/target/debug/map_reduce"]



