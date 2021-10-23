#!/bin/bash

docker run --name mysqllatest -e MYSQL_ROOT_PASSWORD="Temp123!" -p 3306:3306 -d mysql:latest

