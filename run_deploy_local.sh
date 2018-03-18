#!/usr/bin/env bash

mvn -DskipTests=false -DskipDocAndSrc=true -e clean license:format package install -U
