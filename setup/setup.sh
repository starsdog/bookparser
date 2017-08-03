#!/bin/bash
sudo ansible-playbook -i "localhost," -c local package.yml

if [ ! -d '../html' ]; then
    mkdir ../html
fi

if [ ! -d '../json' ]; then
    mkdir ../json
fi

if [ ! -d '../media' ]; then
    mkdir ../media
    mkdir ../media/large
    mkdir ../media/small
fi

cp ../image.csv.org ../image.csv
cp ../download.csv.org ../download.csv
cp ../parse.csv.org ../parse.csv
