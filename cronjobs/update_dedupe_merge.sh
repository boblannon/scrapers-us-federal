#! /bin/bash

set -e

### update_dedupe_merge.sh
# This script is responsible for running update commands, followed by the loop
# of deduplication and merging until there are no new merge operations to
# perform. It must not run if another instance of it is already running. This
# constraint is enforced by pgrep

PYTHON=$HOME/virt/bin/python
PUPA=$HOME/virt/bin/pupa

logger -t "update_dedupe_merge.sh" "======= starting update_dedupe_merge.sh  $(date --rfc-3339=seconds) ======="

if [ "$(pgrep -f "/bin/bash $HOME/src/scrapers-us-federal/cronjobs/update_dedupe_merge.sh")" != "$$" ];
then
    logger -t "update_dedupe_merge" "update_dedupe_merge already running"
    exit 1
else
    if pgrep -f "$PYTHON $HOME/src/scrapers-us-federal/scripts/dedupe_and_merge.py";
    then
        logger -t "update_dedupe_merge" "dedupe_and_merge somehow already running. something's wrong."
        exit 1
    else
        logger -t "update_dedupe_merge" "pupa update"
        $PUPA update unitedstates lobbying_registrations &>> $HOME/logs/update.log
        logger -t "update_dedupe_merge" "deduping and merging"
        $PYTHON $HOME/src/scrapers-us-federal/scripts/dedupe_and_merge.py &> $HOME/logs/dedupe_and_merge.log
        if [ $? -eq 0 ];
        then
            logger -t "update_dedupe_merge" "======= finished update_dedupe_merge.sh  $(date --rfc-3339=seconds) ======="
        else
            logger -t "update_dedupe_merge" "======= failed update_dedupe_merge.sh  $(date --rfc-3339=seconds) ======="
        fi
    fi
fi
