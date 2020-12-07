#!/bin/bash
pip3 install -e .;
python setup.py test;
#python -m pytest tests/ --cov;
