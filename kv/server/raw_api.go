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
	reader, err := server.storage.Reader(&kvrpcpb.Context{})
	if err != nil {
		return nil, err
	}
	var value []byte
	value, err = reader.GetCF(req.Cf, req.Key)
	if err != nil {
		return nil, err
	} else if value == nil {
		return &kvrpcpb.RawGetResponse{NotFound: true}, nil
	}
	return &kvrpcpb.RawGetResponse{Value: value}, nil
}

// RawPut puts the target data into storage and returns the corresponding response
func (server *Server) RawPut(_ context.Context, req *kvrpcpb.RawPutRequest) (*kvrpcpb.RawPutResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using Storage.Modify to store data to be modified
	data := storage.Put{Cf: req.Cf, Key: req.Key, Value: req.Value}
	err := server.storage.Write(&kvrpcpb.Context{}, []storage.Modify{{Data: data}})
	resp := &kvrpcpb.RawPutResponse{}
	if err != nil {
		resp.Error = err.Error()
	}
	return resp, nil
}

// RawDelete delete the target data from storage and returns the corresponding response
func (server *Server) RawDelete(_ context.Context, req *kvrpcpb.RawDeleteRequest) (*kvrpcpb.RawDeleteResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using Storage.Modify to store data to be deleted
	data := storage.Delete{Cf: req.Cf, Key: req.Key}
	err := server.storage.Write(&kvrpcpb.Context{}, []storage.Modify{{Data: data}})
	resp := &kvrpcpb.RawDeleteResponse{}
	if err != nil {
		resp.Error = err.Error()
	}
	return resp, nil
}

// RawScan scan the data starting from the start key up to limit. and return the corresponding result
func (server *Server) RawScan(_ context.Context, req *kvrpcpb.RawScanRequest) (*kvrpcpb.RawScanResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using reader.IterCF
	reader, err := server.storage.Reader(&kvrpcpb.Context{})
	resp := &kvrpcpb.RawScanResponse{}
	if err != nil {
		resp.Error = err.Error()
		return resp, nil
	}
	iter := reader.IterCF(req.Cf)
	for iter.Seek(req.StartKey); iter.Valid(); iter.Next() {
		item := iter.Item()
		key := item.Key()
		value, _ := item.Value()
		resp.Kvs = append(resp.Kvs, &kvrpcpb.KvPair{Key: key, Value: value})
		if uint32(len(resp.Kvs)) >= req.Limit {
			break
		}
	}
	return resp, nil
}
