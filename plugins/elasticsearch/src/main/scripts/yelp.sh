#!/usr/bin/env bash
#
# Copyright (C) 2017 Dremio Corporation
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#


#
# Simple script for loading Yelp data into Elasticsearch.
# Download the Yelp dataset and place it under a directory named 'data'.
# Each dataset is loaded into a separate document type in the 'yelp' index.
#

usage() {
    echo "usage: yelp.sh [business|review|user|checkin|tip]"
}

starting() {
    echo "----------------------------"
    echo "loading data file: $1"
    echo "----------------------------"
}

finished() {
    echo "----------------------------"
    echo "complete"
    echo "----------------------------"
}

command=$1

case $command in

    business)
        FILE=yelp_academic_dataset_business.json
        INPUT=`pwd`/data/$FILE
        starting $INPUT
        cat $INPUT | logstash -e "
            input { stdin { } }
            filter { json { source => "message" } }
            output {
              elasticsearch {
                     host => 'localhost' cluster => 'dremio-test-cluster' index => 'yelp' document_type => 'business'
                }
            }
        "
        finished
    ;;

    review)
        FILE=yelp_academic_dataset_review.json
        INPUT=`pwd`/data/$FILE
        starting $INPUT
        cat $INPUT | logstash -e "
            input { stdin { } }
            filter { json { source => "message" } }
            output {
              elasticsearch {
                     host => 'localhost' cluster => 'dremio-test-cluster' index => 'yelp' document_type => 'review'
                }
            }
        "
        finished
    ;;

    user)
        FILE=yelp_academic_dataset_user.json
        INPUT=`pwd`/data/$FILE
        starting $INPUT
        cat $INPUT | logstash -e "
            input { stdin { } }
            filter { json { source => "message" } }
            output {
              elasticsearch {
                     host => 'localhost' cluster => 'dremio-test-cluster' index => 'yelp' document_type => 'user'
                }
            }
        "
        finished
    ;;

    checkin)
        FILE=yelp_academic_dataset_checkin.json
        INPUT=`pwd`/data/$FILE
        starting $INPUT
        cat $INPUT | logstash -e "
            input { stdin { } }
            filter { json { source => "message" } }
            output {
              elasticsearch {
                     host => 'localhost' cluster => 'dremio-test-cluster' index => 'yelp' document_type => 'checkin'
                }
            }
        "
        finished
    ;;

    tip)
        FILE=yelp_academic_dataset_tip.json
        INPUT=`pwd`/data/$FILE
        starting $INPUT
        cat $INPUT | logstash -e "
            input { stdin { } }
            filter { json { source => "message" } }
            output {
              elasticsearch {
                     host => 'localhost' cluster => 'dremio-test-cluster' index => 'yelp' document_type => 'tip'
                }
            }
        "
        finished
    ;;

    *)
        usage
        exit 1
    ;;

esac
exit 0
