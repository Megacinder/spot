#!/usr/bin/env bash


SRC_DIR="src"
SRC_SPARK_DIR="src/spark"

if [ -x "$(which pipenv)"]; then
  # install packages to a temporary directory and zip it
  touch $SRC_SPARK_DIR/requirements.txt # safeguard in case there are no packages
  pip3 install -r $SRC_SPARK_DIR/requirements.txt --target $SRC_SPARK_DIR/packages

  # check to see if there are any external dependencies
  # if not then create an empty file to seed zip with
  if [ -z "$(ls -A $SRC_SPARK_DIR/packages)" ]; then
    touch $SRC_SPARK_DIR/packages/empty.txt
  fi

  # zip dependencies
  if [ ! -d $SRC_SPARK_DIR/packages ]; then
    echo 'ERROR - pip failed to import dependencies'
    exit 1
  fi

  cd $SRC_SPARK_DIR/packages
  zip -9mrv packages.zip .
  mv packages.zip ..
  cd -

  # remove temporary directory and requirements.txt
  rm -rf $SRC_SPARK_DIR/packages
  rm $SRC_SPARK_DIR/requirements.txt

  # add local modules
  echo '... adding all modules from local utils package'
  zip -ru9 $SRC_SPARK_DIR/packages.zip $SRC_SPARK_DIR -x $SRC_SPARK_DIR/__pycache__/\*
  zip -ru9 $SRC_SPARK_DIR/packages.zip $SRC_SPARK_DIR/__init__.py
  zip -ru9 $SRC_SPARK_DIR/packages.zip $SRC_DIR/__init__.py

  exit 0
else
  echo 'ERROR - pipenv is not installed --> run `pip3 install pipenv` to load pipenv into global site packages or install via a system package manager.'
  exit 1
fi
