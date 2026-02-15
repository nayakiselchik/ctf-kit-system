FROM zeek/zeek:latest

RUN apt-get update && apt-get install -y --no-install-recommends inotify-tools \
  && rm -rf /var/lib/apt/lists/*

COPY zeek-watch.sh /usr/local/bin/zeek-watch.sh
RUN chmod +x /usr/local/bin/zeek-watch.sh

ENTRYPOINT ["/usr/local/bin/zeek-watch.sh"]
