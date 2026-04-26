FROM rust:1.85-bookworm

RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    pkg-config \
    libssl-dev \
    exiftool \
    git \
    libclang-dev \
    clang \
    cmake \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /workspace

# Standard Cargo environment variables
ENV CARGO_HOME=/cargo-home
ENV PATH=$CARGO_HOME/bin:$PATH

CMD ["/bin/bash"]
