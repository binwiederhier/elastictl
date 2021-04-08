FROM ubuntu
MAINTAINER Philipp C. Heckel <philipp.heckel@gmail.com>

COPY elastictl /usr/bin
RUN \
	   apt-get update \
	&& apt-get install -y ca-certificates --no-install-recommends \
	&& rm -rf /var/lib/apt/lists/*ubuntu.{org,net}* \
	&& apt-get purge -y --auto-remove \
	&& useradd -m -d/home/elastictl -s /bin/bash elastictl \
	&& echo 'elastictl ALL=(ALL) NOPASSWD:ALL' >> /etc/sudoers

ENTRYPOINT ["elastictl"]
