#!/usr/bin/env bash
set -euo pipefail

#echo "Executant go test ./... amb cobertura bàsica"
#go test ./... -cover -count=1
#echo "Tests superats correctament"


echo "Executant go test ./... amb cobertura ampliada"
go test ./... -covermode=count -coverpkg=./... -count=1
echo "Tests superats correctament"