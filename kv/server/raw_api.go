package server

import (
	"context"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
)

// The functions below are Server's Raw API. (implements TinyKvServer).
// Some helper methods can be found in sever.go in the current directory

// RawGet return the corresponding Get response based on RawGetRequest's CF and Key fields
func (server *Server) RawGet(_ context.Context, req *kvrpcpb.RawGetRequest) (*kvrpcpb.RawGetResponse, error) {
	// Your Code Here (1).
	mr, _ := server.storage.Reader(nil)
	defer mr.Close()
	res, _ := mr.GetCF(req.GetCf(), req.GetKey())
	return &kvrpcpb.RawGetResponse{
		Value:    res,
		NotFound: res == nil,
	}, nil
}

// RawPut puts the target data into storage and returns the corresponding response
func (server *Server) RawPut(_ context.Context, req *kvrpcpb.RawPutRequest) (*kvrpcpb.RawPutResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using Storage.Modify to store data to be modified
	server.storage.Write(nil, []storage.Modify{
		{
			Data: storage.Put{
				Value: req.GetValue(),
				Key:   req.GetKey(),
				Cf:    req.GetCf(),
			},
		},
	})
	return nil, nil
}

// RawDelete delete the target data from storage and returns the corresponding response
func (server *Server) RawDelete(_ context.Context, req *kvrpcpb.RawDeleteRequest) (*kvrpcpb.RawDeleteResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using Storage.Modify to store data to be deleted
	server.storage.Write(nil, []storage.Modify{
		{
			Data: storage.Delete{
				Key: req.GetKey(),
				Cf:  req.GetCf(),
			},
		},
	})
	return nil, nil
}

// RawScan scan the data starting from the start key up to limit. and return the corresponding result
func (server *Server) RawScan(_ context.Context, req *kvrpcpb.RawScanRequest) (*kvrpcpb.RawScanResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using reader.IterCF
	mr, _ := server.storage.Reader(nil)
	defer mr.Close()
	mrIter := mr.IterCF(req.GetCf())
	defer mrIter.Close()
	mrIter.Seek(req.GetStartKey())
	var i uint32
	i = 0
	var res []*kvrpcpb.KvPair
	for ; i < req.GetLimit() && mrIter.Valid(); i++ {
		NewItem := mrIter.Item()
		val, _ := NewItem.Value()
		key := NewItem.Key()
		res = append(res, &kvrpcpb.KvPair{
			Key:   key,
			Value: val,
		})
		mrIter.Next()
	}
	return &kvrpcpb.RawScanResponse{
		Kvs: res,
	}, nil
}
