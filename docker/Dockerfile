FROM tiagopeixoto/graph-tool
COPY . .
RUN apt-get update && apt-get install -y r-base
RUN git clone https://github.com/shaypal5/twikwak17
WORKDIR /twikwak17
RUN pip install -e ".[test]"
