#!/bin/bash

curl -LsfS https://get.airbyte.com | bash -

abctl local install --disable-auth
