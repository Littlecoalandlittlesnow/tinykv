#!/bin/bash
for ((i=1;i<=$1;i++))
do
  go test -test.v -test.run="TestBasic2B" >> TestBasic2B.log
  go test -test.v -test.run="TestConcurrent2B" >> TestConcurrent2B.log
  go test -test.v -test.run="TestUnreliable2B" >> TestUnreliable2B.log
  go test -test.v -test.run="TestOnePartition2B" >> TestOnePartition2B.log
  go test -test.v -test.run="TestManyPartitionsOneClient2B" >> TestManyPartitionsOneClient2B.log
  go test -test.v -test.run="TestManyPartitionsManyClients2B" >> TestManyPartitionsManyClients2B.log
  go test -test.v -test.run="TestPersistOneClient2B" >> TestPersistOneClient2B.log
  go test -test.v -test.run="TestPersistConcurrent2B" >> TestPersistConcurrent2B.log
  go test -test.v -test.run="TestPersistConcurrentUnreliable2B" >> TestPersistConcurrentUnreliable2B.log
  go test -test.v -test.run="TestPersistPartition2B" >> TestPersistPartition2B.log
  go test -test.v -test.run="TestPersistPartitionUnreliable2B" >> TestPersistPartitionUnreliable2B.log
done