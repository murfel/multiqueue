#!/usr/bin/env bash

function download {
    wget -O USA-road-d.$1.gr.gz http://users.diag.uniroma1.it/challenge9/data/USA-road-d/USA-road-d.$1.gr.gz
    gunzip USA-road-d.$1.gr.gz
    cat USA-road-d.$1.gr | sed -e '/^c/d' | cut -c 3- | tail -c +4 > $1.in
    rm USA-road-d.$1.gr
}

#wget -O rome99.gr http://users.diag.uniroma1.it/challenge9/data/rome/rome99.gr
#cat rome99.gr | sed -e '/^c/d' | cut -c 3- | tail -c +4 > rome.in
#rm rome99.gr

for NAME in NY BAY COL FLA NW NE CAL LKS E W CTR USA
do
  download $NAME
done