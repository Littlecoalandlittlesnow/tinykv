#!/bin/bash
for ((i=$1;i<=$2;i++))
do
#  go test -test.v -test.run="TestBasic2B" >> TestBasic2B.log
#  go test -test.v -test.run="TestConcurrent2B" >> TestConcurrent2B.log
#  go test -test.v -test.run="TestUnreliable2B" >> TestUnreliable2B.log
#  go test -test.v -test.run="TestOnePartition2B" >> TestOnePartition2B.log
#  go test -test.v -test.run="TestManyPartitionsOneClient2B" >> TestManyPartitionsOneClient2B.log
#  go test -test.v -test.run="TestManyPartitionsManyClients2B" >> TestManyPartitionsManyClients2B.log
#  go test -test.v -test.run="TestPersistOneClient2B" >> TestPersistOneClient2B.log
#  go test -test.v -test.run="TestPersistConcurrent2B" >> TestPersistConcurrent2B.log
#  go test -test.v -test.run="TestPersistConcurrentUnreliable2B" >> TestPersistConcurrentUnreliable2B.log
#  go test -test.v -test.run="TestPersistPartition2B" >> TestPersistPartition2B.log
#  go test -test.v -test.run="TestPersistPartitionUnreliable2B" > 2B"$i".log
#  go test -test.v -test.run="TestOneSnapshot2C" > 2C"$i".log
#  go test -test.v -test.run="TestSnapshotUnreliableRecoverConcurrentPartition2C" > 2C"$i".log
   #go test -test.v -test.run="TestTransferLeader3B" >> 3Ba.log
  #  go test -test.v -test.run="TestBasicConfChange3B" >> 3Bb.log
  #  go test -test.v -test.run="TestConfChangeRecover3B" >> 3Bc.log
  #  go test -test.v -test.run="TestConfChangeRecoverManyClients3B" >> 3Bd.log
  #  go test -test.v -test.run="TestConfChangeUnreliable3B" >> 3Be.log
  #  go test -test.v -test.run="TestConfChangeUnreliableRecover3B" >> 3Bf.log
  #  go test -test.v -test.run="TestConfChangeSnapshotUnreliableRecover3B" >> 3Bg.log
  #  go test -test.v -test.run="TestConfChangeSnapshotUnreliableRecoverConcurrentPartition3B" >> 3Bh.log
  # go test -test.v -test.run="TestOneSplit3B" > /home/tinykvs/tinykv/output/3B"$i".log
  #  go test -test.v -test.run="TestSplitRecover3B" >> /home/tinykvs/tinykv/output/3Bk.log
  # go test -test.v -test.run="TestSplitRecoverManyClients3B" > /home/tinykvs/tinykv/output/3B"$i".log
  #go test -test.v -test.run="TestSplitUnreliable3B" > /home/tinykvs/tinykv/output/3B"$i".log
  # go test -test.v -test.run="TestSplitUnreliableRecover3B" >> /home/tinykvs/tinykv/output/3B"$i".log
 go test -test.v -test.run="TestSplitConfChangeSnapshotUnreliableRecover3B" > 3Ba"$i".log
 go test -test.v -test.run="TestSplitConfChangeSnapshotUnreliableRecoverConcurrentPartition3B" > 3Bb"$i".log
  echo "$i"完成
done