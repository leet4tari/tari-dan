# Docker Build Notes
Create a folder ```sources``` and build a docker image.
```bash
mkdir sources
cd sources
git clone https://github.com/tari-project/dan-testing
git clone https://github.com/tari-project/tari.git
git clone https://github.com/tari-project/tari-dan.git
git clone https://github.com/tari-project/tari-connector.git
cp -v dan-testing/docker_rig/cross-compile-aarch64.sh .
docker build -f dan-testing/docker_rig/dan-testing.Dockerfile \
  -t local/dan-testing .
```

# Docker Testing Notes
Launching the docker image with local ports redirected to docker container ports 18000 to 19000 
```bash
docker run --rm -it -p 18000-19000:18000-19000 \
  quay.io/tarilabs/dan-testing
```

Using the folder ```sources```, builds can be done with
the docker image.
```bash
docker run --rm -it -p 18000-19000:18000-19000 \
  -v $PWD/sources/:/home/tari/sources-build \
  quay.io/tarilabs/dan-testing:development_20230704_790dbea \
  /bin/bash
```
