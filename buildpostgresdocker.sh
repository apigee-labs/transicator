#!/bin/bash



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