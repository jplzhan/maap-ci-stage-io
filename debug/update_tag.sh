#!/bin/bash
git tag -f $1
git push -f origin $1
