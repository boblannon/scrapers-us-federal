### post_employment_update.sh
# This script is responsible for running update commands, followed by the loop
# of deduplication and merging until there are no new merge operations to
# perform. It must not run if another instance of it is already running. This
# constraint is enforced by pgrep

PYTHON=$HOME/virt/bin/python
PUPA=$HOME/virt/bin/pupa

logger -t "post_employment_update.sh" "======= starting post_employment_update.sh  $(date --rfc-3339=seconds) ======="

if [ "$(pgrep -f "/bin/bash $HOME/src/scrapers-us-federal/cronjobs/post_employment_update.sh")" != "$$" ];
then
    logger -t "post_employment_update" "post_employment_update already running"
    exit 1
else
    if pgrep -f "$PYTHON $HOME/src/scrapers-us-federal/scripts/dedupe_and_merge.py";
    then
        logger -t "post_employment_update" "dedupe_and_merge somehow already running. something's wrong."
        exit 1
    else
        logger -t "post_employment_update" "pupa update"
        $PUPA update unitedstates house_post_employment &>> $HOME/logs/update.log
        $PUPA update unitedstates senate_post_employment &>> $HOME/logs/update.log
        logger -t "post_employment_update" "deduping and merging"
        $PYTHON $HOME/src/scrapers-us-federal/scripts/dedupe_and_merge.py &> $HOME/logs/dedupe_and_merge.log
        if [ $? -eq 0 ];
        then
          cache_busted=$(curl http://lobbying.influenceexplorer.com/vashistha_cache_clear)
          page_ok=$(curl --write-out %{http_code} --silent --output /dev/null http://lobbying.influenceexplorer.com/vashistha_cache_clear)
          if [ "$page_ok" == "200" ] &&  [ "$cache_busted" == "OK" ]
            then
                logger -t "post_employment_update" "======= finished post_employment_update.sh  $(date --rfc-3339=seconds) ======="
            else
                logger -t "post_employment_update" "======= WARNING: post_employment_update worked, but cache busting didn't  $(date --rfc-3339=seconds) ======="
          fi
        else
            logger -t "post_employment_update" "======= failed post_employment_update.sh  $(date --rfc-3339=seconds) ======="
        fi
    fi
fi
