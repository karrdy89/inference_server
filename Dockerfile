FROM python:3.11.5-slim
ENV TZ=Asia/Seoul
RUN ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && echo $TZ > /etc/timezone
RUN apt-get update
RUN apt-get install nano
ENV PIP_ROOT_USER_ACTION=ignore
WORKDIR /app/
COPY ./ .
RUN mkdir cert
RUN mkdir logs
RUN mkdir config
RUN mkdir tmp
RUN pip install --upgrade pip
RUN pip install -r requirements.txt
CMD ["python3", "start_server.py"]