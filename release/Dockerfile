FROM ubuntu:16.04

# Make the "en_US.UTF-8" locale so postgres will be utf-8 enabled by default.
RUN apt-get update && apt-get install -y apt-utils locales && rm -rf /var/lib/apt/lists/* && \
    localedef -i en_US -c -f UTF-8 -A /usr/share/locale/locale.alias en_US.UTF-8
ENV LANG=en_US.UTF-8 \
    LANGUAGE=en_US:en \
    LC_ALL=en_US.UTF-8

ENV LLVM_VERSION=3.8
RUN apt-get update && \
    apt-get -y install \
        autoconf \
        bison \
        build-essential \
        clang++-${LLVM_VERSION} \
        clang-${LLVM_VERSION} \
        cmake \
        curl \
        flex \
        git-core \
        libtool \
        software-properties-common

RUN update-alternatives --install /usr/bin/clang clang /usr/bin/clang-${LLVM_VERSION} 100
RUN update-alternatives --install /usr/bin/clang++ clang++ /usr/bin/clang++-${LLVM_VERSION} 100

ENV MAKEFLAGS='-j$(nproc)'

# Install protobuf 3 and grpc.
RUN git clone -b $(curl -L https://grpc.io/release) https://github.com/grpc/grpc && \
    cd grpc && git submodule update --init && make && make install && \
    cd third_party/protobuf && make install
RUN rm -rf grpc

RUN git clone https://github.com/apache/incubator-quickstep.git quickstep
RUN cd quickstep/third_party && ./download_and_patch_prerequisites.sh
RUN cd quickstep/build && cmake -DCMAKE_BUILD_TYPE=Release -DENABLE_NETWORK_CLI=On \
        -DCMAKE_C_COMPILER=clang -DCMAKE_CXX_COMPILER=clang++ .. && \
    make quickstep_cli_shell quickstep_client && \
    cp   quickstep_cli_shell quickstep_client /usr/local/bin
RUN rm -rf quickstep

# Add non-root user for container.
ENV CONTAINER_USER="quickstep"
RUN useradd -m ${CONTAINER_USER}
# Make bash the default shell (useful for when using tmux in the container).
RUN chsh --shell /bin/bash ${CONTAINER_USER}
USER ${CONTAINER_USER}
