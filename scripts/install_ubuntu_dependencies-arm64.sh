#!/usr/bin/env sh
#
# Install Ubuntu aarch64/arm64 deb dev/tool packages on x86_64
#
apt-get -y install $* \
  gcc-aarch64-linux-gnu \
  g++-aarch64-linux-gnu

#  pkg-config-aarch64-linux-gnu \

# Big hack to get libssl-dev:arm64 on ubuntu 22.04 - https://askubuntu.com/questions/1255707/apt-cant-find-packages-on-ubuntu-20-04-arm64-raspberry-pi-4
cat << EOD | tee -a /etc/apt/sources.list.d/ubuntu-ports-arm64.list
deb [arch=arm64] http://ports.ubuntu.com/ jammy main multiverse universe
deb [arch=arm64] http://ports.ubuntu.com/ jammy-security main multiverse universe
deb [arch=arm64] http://ports.ubuntu.com/ jammy-backports main multiverse universe
deb [arch=arm64] http://ports.ubuntu.com/ jammy-updates main multiverse universe
EOD
dpkg --print-foreign-architectures
dpkg --add-architecture arm64
dpkg --print-foreign-architectures
# Not all repos have arm64 components, force true return
apt-get update || true
apt-get install -y libssl-dev:arm64
