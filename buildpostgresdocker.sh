#!/bin/bash

# Copyright 2016 Google Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

function show_help(){
    echo "Usage is $0 [-p version to push]"
}

version=""

#Get the optoinal -d
while getopts "p:" opt; do
    case "$opt" in
        h)
            show_help
            exit 0
            ;;
        p)  version=$OPTARG
            ;;
        '?')
            show_help >&2
            exit 1
            ;;
    esac
done


docker build -f pgoutput/Dockerfile ./pgoutput/ -t thirtyx/transicator

if [ "$version" != "" ]; then 
    docker tag thirtyx/transicator thirtyx/transicator:$version
    docker push thirtyx/transicator:$version
fi
