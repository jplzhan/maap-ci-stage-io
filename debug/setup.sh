#!/bin/bash

basedir=$(dirname "$(readlink -f "$0")")

# Install VIM
apt-get update
apt-get install vim
curl -fLo ~/.vim/autoload/plug.vim --create-dirs https://raw.githubusercontent.com/junegunn/vim-plug/master/plug.vim
cp $basedir/.vimrc ~/

# Install AWS S3 CLI
curl "https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip" -o "awscliv2.zip"
unzip awscliv2.zip
./aws/install
