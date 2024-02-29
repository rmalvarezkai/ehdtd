#!/bin/bash

su postgres -c "psql -f db-create.sql"

#psql -h 127.0.0.1 -p 5432 -U ehdtd -w -d ehdtd -f ehdtd.sql








