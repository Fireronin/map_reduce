FROM ubuntu:latest

# Install curl, protobuf, cargo, apt-transport-https, ca-certificates, and gnupg
RUN apt-get update && \
    apt-get install -y curl protobuf-compiler cargo apt-transport-https ca-certificates gnupg

# Download the public signing key for the Kubernetes package repositories
RUN if [ ! -d /etc/apt/keyrings ]; then mkdir -p -m 755 /etc/apt/keyrings; fi && \
    curl -fsSL https://pkgs.k8s.io/core:/stable:/v1.29/deb/Release.key | gpg --dearmor -o /etc/apt/keyrings/kubernetes-apt-keyring.gpg

# Add the appropriate Kubernetes apt repository
RUN echo 'deb [signed-by=/etc/apt/keyrings/kubernetes-apt-keyring.gpg] https://pkgs.k8s.io/core:/stable:/v1.29/deb/ /' > /etc/apt/sources.list.d/kubernetes.list

# Update apt package index, then install kubectl
RUN apt-get update && \
    apt-get install -y kubectl

# This stuff is not needed as executable will be copied from host

# # Copy map_reduce folder
# COPY ./map_reduce /home/map/map_reduce

# # Run cargo build
# RUN cd /home/map/map_reduce && cargo build

# Set the default command to run when starting a container from this image
# CMD ["/home/map/map_reduce/target/debug/map_reduce"]