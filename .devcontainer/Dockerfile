FROM mcr.microsoft.com/devcontainers/cpp:dev-ubuntu24.04


ENV LD_LIBRARY_PATH=/usr/local/lib:$LD_LIBRARY_PATH

ENV PYTHONFAULTHANDLER=1 \
  PYTHONUNBUFFERED=1

ENV PIP_DISABLE_PIP_VERSION_CHECK=1 \
  PIP_NO_CACHE_DIR=1 \
  PIP_ROOT_USER_ACTION=ignore

ENV DEBIAN_FRONTEND=noninteractive 

ARG POETRY_VERSION=1.2.2
ARG CMAKE_MAJOR=3.24
ARG CMAKE_VERSION=3.24.2
ARG HYPERFINE=1.15.0

# avoid a torch.distributed error
# TODO: figure out optimal value
ENV OMP_NUM_THREADS=1

# C++/build dependencies
RUN apt update \
  && apt install -y \
  build-essential \
  clang-format \
  curl \
  gcc \
  git \
  libbz2-dev \
  libcurl4-openssl-dev \
  libbz2-dev \
  nasm \
  software-properties-common \
  vim \
  wget \
  zip \
  zlib1g-dev \
  && rm -rf /var/lib/apt/lists/* \
  && apt clean

# google cloud
RUN echo "deb [signed-by=/usr/share/keyrings/cloud.google.gpg] https://packages.cloud.google.com/apt cloud-sdk main" | \
  tee -a /etc/apt/sources.list.d/google-cloud-sdk.list
RUN curl https://packages.cloud.google.com/apt/doc/apt-key.gpg | \
  apt-key --keyring /usr/share/keyrings/cloud.google.gpg add -
RUN apt-get update && apt-get install -y  google-cloud-cli

RUN apt update -y \
  && apt install software-properties-common -y \
  && add-apt-repository ppa:deadsnakes/ppa

# Install apt dependencies
RUN apt update -y \
  && apt install -y --no-install-recommends \
  python3.10-full \
  && rm -rf /var/lib/apt/lists/* \
  && apt clean

RUN mkdir -p /venv
RUN python3.10 -m venv /venv
ENV PATH="/venv/bin:$PATH"

# FIXME - pip is out of date, find a better way
RUN pip install --upgrade pip

RUN pip install wheel yapf cpplint

RUN pip install \
  "poetry==$POETRY_VERSION" \
  && poetry config virtualenvs.create false
# if poetry doesn't pick up a deps this for some reason pip install here

# if we want the user to be able to do pip install etc.
RUN chmod -R 777 /venv

# Allow users to update venv
RUN mkdir /workspaces
RUN chmod -R 777 /workspaces

# you can use this for benchmarking cli tools
RUN wget https://github.com/sharkdp/hyperfine/releases/download/v${HYPERFINE}/hyperfine_${HYPERFINE}_amd64.deb \
  && dpkg -i hyperfine_${HYPERFINE}_amd64.deb

# this can be slow if you don't have a large number of cpus
RUN mkdir -p /apps/cmake \
  && cd /apps/cmake \
  && wget https://cmake.org/files/v${CMAKE_MAJOR}/cmake-${CMAKE_VERSION}.tar.gz \
  && tar -xzvf cmake-${CMAKE_VERSION}.tar.gz \
  && rm cmake-${CMAKE_VERSION}.tar.gz \
  && cd  cmake-${CMAKE_VERSION} \
  && ./bootstrap --system-curl \
  && make -j$(nproc) \
  && make install \
  && cd / \
  && rm -rf /apps/cmake \
  && ldconfig


ENV PYTHONPATH "${PYTHONPATH}:/workspaces/multidimio-cpp/multidimio-cpp"
ENV PYTHONPATH "${PYTHONPATH}:/workspaces/multidimio-cpp"
