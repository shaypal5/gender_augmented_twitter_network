FROM tiagopeixoto/graph-tool
USER root
COPY ./cfg.json /root/.config/twikwak17/cfg.json
# RUN apt-get update && apt-get install -y r-base
RUN curl https://bootstrap.pypa.io/get-pip.py -o get-pip.py && \
    python get-pip.py && \
    python --version
RUN python -m pip install ipython ptpython jupyter
RUN git clone https://github.com/shaypal5/twikwak17 && \
    ls -a && echo $PWD
WORKDIR /root/twikwak17
RUN python -m pip install -e ".[test]"
WORKDIR /root/
RUN mkdir ".jupyter" && \
    echo "c.NotebookApp.ip = '0.0.0.0'" > /root/.jupyter/jupyter_notebook_config.py
