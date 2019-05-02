#!/bin/bash

python gateway_server.py config.json &

python controller.py config.json &


