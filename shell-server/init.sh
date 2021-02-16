#!/bin/sh
apk update && 
apk upgrade && 
apk add git \
  openldap-clients \
  openssh &&
mkdir $HOME/.ssh &&
if [ -f /home/airflow/ssh-airflow.pub ] ; then
 mkdir /home/airflow/.ssh
 cat /home/airflow/ssh-airflow.pub >> /home/airflow/.ssh/authorized_keys
fi
addgroup  -g 50000 airflow
adduser -D -G airflow -u 50000 -h /home/airflow airflow
ssh-keygen -A
passwd -d root
passwd -u airflow
/usr/sbin/sshd -De

