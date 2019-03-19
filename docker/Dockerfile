FROM tiagopeixoto/graph-tool
USER user
ENV PATH="/home/user/.local/bin:${PATH}"
WORKDIR /home/user/
COPY ./cfg.json /home/user/.config/twikwak17/cfg.json
COPY jupyter_notebook_config.py /home/user/.jupyter/jupyter_notebook_config.py
USER root
RUN chown -R user /home/user/.config /home/user/.jupyter
USER user
RUN chmod -R u+rwx /home/user/.config /home/user/.jupyter && \
    ls -l /home/user/.config/ && \
    ls -l /home/user/.jupyter
RUN curl https://bootstrap.pypa.io/get-pip.py -o get-pip.py && \
    python get-pip.py --user && \
    python --version
# RUN python -m pip install ptpython --user
RUN git clone https://github.com/shaypal5/twikwak17 && \
    ls -a && echo $PWD
WORKDIR /home/user/twikwak17
RUN python -m pip install -e ".[test]" --user
# RUN git remote set-url origin git@github.com:shaypal5/twikwak17.git
WORKDIR /home/user/
