#!/bin/bash

mvn clean

pushd data/
find . -name *.data | xargs -I {} rm {}
find . -name *.archive | xargs -I {} rm {}
popd
