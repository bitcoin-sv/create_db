#!/bin/bash

read -r VERSIONPREFIX<version_createdb.txt

git remote update
git pull
git status -uno

COMMITID=$(git rev-parse --short HEAD)

APPVERSIONCREATEDB="$VERSIONPREFIX-$COMMITID"

mkdir -p Build

echo "* Building NuGet nChain.CreateDB package"

dotnet restore -p:Configuration=Release ..
VERSION=${VERSIONPREFIX} dotnet build -c Release -o Build ../nChain.CreateDB.sln
VERSION=${VERSIONPREFIX} dotnet pack -c Release -o Build ../nChain.CreateDB.sln
