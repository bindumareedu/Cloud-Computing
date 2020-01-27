#!/bin/bash

# Copyright 2015 Google Inc. All Rights Reserved.
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

# [START startup_script]
apt-get update
apt-get -y install default-jdk
sudo apt-get install python-pip
sudo pip install gsutil
sudo mkdir -p /cmn/str
sudo chmod 777 -R /cmn/
gsutil cp gs://mapreduce-storage/server.jar /cmn/str/
cd /cmn/str
java -jar server.jar
# [END startup_script]