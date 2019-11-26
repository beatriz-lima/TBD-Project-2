#! /bin/bash

extra_options=""

while test $# -ge 1; do
    case $1 in
        --rm)
            extra_options+=" $1"
            shift
        ;;
        -d)
            extra_options+=" $1"
            shift
        ;;
        -h|--help)
            echo "Usage: $0" >&2
            echo "" >&2
            echo "Options:" >&2
            echo "  --rm            Automatically remove the container when it exits" >&2
            echo "  -d              Run container in the background, print new container id" >&2
            echo "" >&2
            exit 0
        ;;
        -*)
            echo "Unexpected option $1" >&2
            exit 2
        ;;
        *)
            break
        ;;
    esac
done

if ! test -d volumes/neo4j/data; then
    mkdir -p volumes/neo4j/data
fi

docker run \
    --publish=7474:7474 \
    --publish=7687:7687 \
    --volume=$PWD/volumes/neo4j/data:/data \
    neo4j:3.5
