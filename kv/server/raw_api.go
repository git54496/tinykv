package server

import (
	"context"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/log"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
)

// The functions below are Server's Raw API. (implements TinyKvServer).
// Some helper methods can be found in sever.go in the current directory

// RawGet return the corresponding Get response based on RawGetRequest's CF and Key fields
func (server *Server) RawGet(context context.Context, req *kvrpcpb.RawGetRequest) (*kvrpcpb.RawGetResponse, error) {
	// Your Code Here (1).
	response := &kvrpcpb.RawGetResponse{}

	reader, err := server.storage.Reader(req.Context)
	defer reader.Close()
	if err != nil {
		log.Fatal(err)
		return nil, err
	}
	value, err := reader.GetCF(req.GetCf(), req.GetKey())
	if value == nil {
		response.NotFound = true
		return response, nil
	}
	if err != nil {
		log.Fatal(err)
		return nil, err
	}

	response.Value = value
	return response, nil
}

// RawPut puts the target data into storage and returns the corresponding response
func (server *Server) RawPut(_ context.Context, req *kvrpcpb.RawPutRequest) (*kvrpcpb.RawPutResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using Storage.Modify to store data to be modified
	res := &kvrpcpb.RawPutResponse{}

	// 转为modify，再用storage的Write方法，以复用storage接口
	mod := storage.Put{
		Key: req.Key,
		Value: req.Value,
		Cf: req.Cf,
	}
	var s  = make([]storage.Modify, 1)
	s[0] = storage.Modify{
		Data: mod,
	}

	err := modify(server, req.Context, s)
	if err != nil {
		res.Error = err.Error()
		return res, err
	}

	return res, nil
}

// RawDelete delete the target data from storage and returns the corresponding response
func (server *Server) RawDelete(_ context.Context, req *kvrpcpb.RawDeleteRequest) (*kvrpcpb.RawDeleteResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using Storage.Modify to store data to be deleted

	res := &kvrpcpb.RawDeleteResponse{}

	mod := storage.Delete{
		Key: req.Key,
		Cf: req.Cf,
	}

	var s  = make([]storage.Modify, 1)
	s[0] = storage.Modify{
		Data: mod,
	}

	err := modify(server, req.Context, s)
	if err != nil {
		res.Error = err.Error()
		return res, err
	}

	return res, nil
}

// RawScan scan the data starting from the start key up to limit. and return the corresponding result
func (server *Server) RawScan(_ context.Context, req *kvrpcpb.RawScanRequest) (*kvrpcpb.RawScanResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using reader.IterCF
	resp := &kvrpcpb.RawScanResponse{
		Kvs: make([]*kvrpcpb.KvPair,0),
	}


	reader, err := server.storage.Reader(req.Context)
	defer reader.Close()
	if err != nil {
		log.Fatal(err)
	}

	iter := reader.IterCF(req.Cf)
	defer iter.Close()

	for iter.Seek(req.StartKey); iter.Valid() && req.Limit > 0; req.Limit--  {
		v, err := iter.Item().Value()
		if err != nil {
			log.Fatal(err.Error())
			return nil, err
		}
		resp.Kvs = append(resp.Kvs, &kvrpcpb.KvPair{
			Key: iter.Item().Key(),
			Value: v,
		})
		iter.Next()
	}

	return resp, nil
}

func modify(server *Server, context *kvrpcpb.Context, mods []storage.Modify) error  {
	err := server.storage.Write(context, mods)
	if err != nil {
		return err
	}
	return nil
}
