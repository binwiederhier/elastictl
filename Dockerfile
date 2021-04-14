FROM ubuntu
MAINTAINER Philipp C. Heckel <philipp.heckel@gmail.com>

COPY elastictl /usr/bin
CMD ["elastictl"]
