#!/usr/bin/env bash

mvn -DskipTests=false -DskipDocAndSrc=false -e clean package deploy
