FROM continuumio/miniconda3

ARG CACHEBUST=1

WORKDIR /home/jovyan

# Create the environment:
COPY . /home/jovyan
RUN conda env create -f environment.yml

# Make RUN commands use the new environment:
SHELL ["conda", "run", "-n", "myenv", "/bin/bash", "-c"]
