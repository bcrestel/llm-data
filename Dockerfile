FROM piptools:latest

ADD requirements.txt ./
RUN pip install -r requirements.txt

# Add jupyterlab extensions
RUN pip install jupyterlab_execute_time

# Git is needed for MLFlow tracking
RUN apt-get update -y && apt-get install -y git

ENV PYTHONPATH="/home"
