#!/usr/bin/env bash
# Script for feeding a larger number of files to NutchWAX one at a time to get more constant indexing. Otherwise if many files are given to NutchWAX, initialising indexers takes a very long time.
starttime_all=$(date +%s)
SCRIPTDIR=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )
cd $SCRIPTDIR
cd ..
FILENAME="$1"
while read LINE ; do
echo $LINE
 # create temp manifest file
 touch manifests/temp_manifest
 echo $LINE > manifests/temp_manifest
 # import segment
 starttime=$(date +%s%N)
 echo " Importing..."
 $SCRIPTDIR/nutchwax import manifests/temp_manifest
 endtime=$(date +%s%N)
 spenttime=$(($endtime - $starttime))
 echo " Importing time in nanoseconds: ${spenttime}"
 # index segment 
 # select newest segment
 SEGMENT=`ls -t segments | tail -1`
 if [ ! -d "segments/$SEGMENT" ]
 then
  rm $SCRIPTDIR/../manifests/temp_manifest
  echo " WARN: no segment found."
  continue
 fi
 mv segments/$SEGMENT segments/temp_seg
 # delete temp files
 rm $SCRIPTDIR/../manifests/temp_manifest
 rm -rf $SCRIPTDIR/../indexes/temp_index
 rm -rf $SCRIPTDIR/../segments/temp_seg
done < $PWD/$FILENAME
echo "Finished feeding. End of input file reached."
endtime_all=$(date +%s)
spenttime_all=$(($endtime_all - $starttime_all))
echo " Indexing time in seconds: ${spenttime_all}"
