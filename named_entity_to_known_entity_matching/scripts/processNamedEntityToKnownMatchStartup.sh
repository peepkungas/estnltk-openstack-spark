#!/bin/bash
for n in "$@"
do
case $n in
    -p=*|--processes=*)
    processes="${n#*=}"
    ;;
esac
done

if [ ! -n "$processes" ]; then
    processes=1
fi

if [ $(ps aux | grep 'processNamedEntityToKnownMatch.sh' | grep -v 'grep' | wc -l) -lt $processes ]; then
    answer=`/opt/estnltk-openstack-spark/named_entity_to_known_entity_matching/scripts/processNamedEntityToKnownMatch.sh | tee -a /opt/logs/known_match_processed.log`
    echo "Process ended. Exiting" >> /opt/logs/known_match_processed.log
else
    echo "Already running. Exiting" >> /opt/logs/known_match_processed.log
fi