#!/bin/sh

pip install virtualenv

virtualenv env

. env/bin/activate

pip install -r requirements.txt

