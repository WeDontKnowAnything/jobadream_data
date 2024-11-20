#!/bin/sh

Xvfb :99 -screen 0 1920x1080x24 > /dev/null 2>&1 &
python crawler.py