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
	response := &kvrpcpb.RawGetResponse{}
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		response.Error = err.Error()
		return response, nil
	}
	value, err := reader.GetCF(req.Cf, req.Key)
	if err != nil {
		response.Error = err.Error()
		return response, nil
	}
	response.Value = value
	if value == nil {
		response.NotFound = true
	}
	return response, nil
}

// RawPut puts the target data into storage and returns the corresponding response
func (server *Server) RawPut(_ context.Context, req *kvrpcpb.RawPutRequest) (*kvrpcpb.RawPutResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using Storage.Modify to store data to be modified
	response := &kvrpcpb.RawPutResponse{}
	bt := []storage.Modify{
		{
			Data: storage.Put{
				Key:   req.Key,
				Value: req.Value,
				Cf:    req.Cf,
			},
		},
	}
	if err := server.storage.Write(req.Context, bt); err != nil {
		response.Error = err.Error()
		return response, nil
	}
	return response, nil
}

// RawDelete delete the target data from storage and returns the corresponding response
func (server *Server) RawDelete(_ context.Context, req *kvrpcpb.RawDeleteRequest) (*kvrpcpb.RawDeleteResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using Storage.Modify to store data to be deleted
	response := &kvrpcpb.RawDeleteResponse{}
	bt := []storage.Modify{
		{
			Data: storage.Delete{
				Key: req.Key,
				Cf:  req.Cf,
			},
		},
	}
	if err := server.storage.Write(req.Context, bt); err != nil {
		response.Error = err.Error()
		return response, nil
	}
	return response, nil
}

// RawScan scan the data starting from the start key up to limit. and return the corresponding result
func (server *Server) RawScan(_ context.Context, req *kvrpcpb.RawScanRequest) (*kvrpcpb.RawScanResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using reader.IterCF
	response := &kvrpcpb.RawScanResponse{}
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		response.Error = err.Error()
		return response, nil
	}
	ite := reader.IterCF(req.Cf)
	ite.Seek(req.StartKey)
	limit := req.Limit
	response.Kvs = make([]*kvrpcpb.KvPair, 0)
	for i := uint32(0); i < limit; i++ {
		if ite.Valid() {
			pair := &kvrpcpb.KvPair{
				Key: ite.Item().Key(),
			}
			if val, err := ite.Item().Value(); err != nil {
				pair.Error = &kvrpcpb.KeyError{
					Retryable: err.Error(),
				}
			} else {
				pair.Value = val
			}
			response.Kvs = append(response.Kvs, pair)
			ite.Next()
		} else {
			break
		}
	}
	ite.Close()
	return response, nil
}
