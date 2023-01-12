#!/bin/ash
/usr/sbin/rmb-relay --domain $RMB_DOMAIN -r $REDIS_URL -s $SUBSTRATE_URL -u $USER_PER_WORKER -w $WORKERS -l 80
