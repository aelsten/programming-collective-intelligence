FROM python:3
WORKDIR /home/
RUN pip install Pillow
RUN pip install h5py
RUN pip install scipy
RUN pip install beautifulsoup4
VOLUME /home
CMD bash
