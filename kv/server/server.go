package server

import (
	"context"
	"fmt"

	"github.com/pingcap-incubator/tinykv/kv/coprocessor"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/kv/storage/raft_storage"
	"github.com/pingcap-incubator/tinykv/kv/transaction/latches"
	"github.com/pingcap-incubator/tinykv/kv/transaction/mvcc"
	coppb "github.com/pingcap-incubator/tinykv/proto/pkg/coprocessor"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
	"github.com/pingcap-incubator/tinykv/proto/pkg/tinykvpb"
	"github.com/pingcap/tidb/kv"
)

var _ tinykvpb.TinyKvServer = new(Server)

// Server is a TinyKV server, it 'faces outwards', sending and receiving messages from clients such as TinySQL.
type Server struct {
	storage storage.Storage

	// (Used in 4B)
	Latches *latches.Latches

	// coprocessor API handler, out of course scope
	copHandler *coprocessor.CopHandler
}

func NewServer(storage storage.Storage) *Server {
	return &Server{
		storage: storage,
		Latches: latches.NewLatches(),
	}
}

// The below functions are Server's gRPC API (implements TinyKvServer).

// Raft commands (tinykv <-> tinykv)
// Only used for RaftStorage, so trivially forward it.
func (server *Server) Raft(stream tinykvpb.TinyKv_RaftServer) error {
	return server.storage.(*raft_storage.RaftStorage).Raft(stream)
}

// Snapshot stream (tinykv <-> tinykv)
// Only used for RaftStorage, so trivially forward it.
func (server *Server) Snapshot(stream tinykvpb.TinyKv_SnapshotServer) error {
	return server.storage.(*raft_storage.RaftStorage).Snapshot(stream)
}

// Transactional API.
func (server *Server) KvGet(_ context.Context, req *kvrpcpb.GetRequest) (*kvrpcpb.GetResponse, error) {
	// Your Code Here (4B).
	resp := &kvrpcpb.GetResponse{}
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		resp.Error = &kvrpcpb.KeyError{
			Retryable: err.Error(),
		}
		return resp, nil
	}
	txn := mvcc.NewMvccTxn(reader, req.Version)
	lock, err := txn.GetLock(req.Key)
	if err != nil {
		resp.Error = &kvrpcpb.KeyError{
			Retryable: err.Error(),
		}
		return resp, nil
	}
	if lock != nil && lock.IsLockedFor(req.Key, req.Version, resp) {
		return resp, nil
	}
	value, err := txn.GetValue(req.Key)
	if err != nil {
		resp.Error = &kvrpcpb.KeyError{
			Retryable: err.Error(),
		}
		return resp, nil
	}
	if value == nil {
		resp.NotFound = true
	} else {
		resp.Value = value
	}
	return resp, nil
}

func (server *Server) KvPrewrite(_ context.Context, req *kvrpcpb.PrewriteRequest) (*kvrpcpb.PrewriteResponse, error) {
	// Your Code Here (4B).

	keys := make([][]byte, 0)
	for _, mutation := range req.Mutations {
		keys = append(keys, mutation.Key)
	}
	if len(keys) == 0 {
		return &kvrpcpb.PrewriteResponse{}, nil
	}
	// lookup latches
	server.Latches.WaitForLatches(keys)
	defer server.Latches.ReleaseLatches(keys)

	resp := &kvrpcpb.PrewriteResponse{}
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		resp.Errors = append(resp.Errors, &kvrpcpb.KeyError{
			Retryable: err.Error(),
		})
		return resp, nil
	}
	primaryLock := &mvcc.Lock{
		Primary: req.PrimaryLock,
		Ts:      req.StartVersion,
		Ttl:     req.LockTtl,
		Kind:    mvcc.WriteKindPut,
	}
	txn := mvcc.NewMvccTxn(reader, req.StartVersion)
	for i, key := range keys {
		lock, err := txn.GetLock(key)
		if err != nil {
			resp.Errors = append(resp.Errors, &kvrpcpb.KeyError{
				Retryable: err.Error(),
			})
			return resp, nil
		}
		if lock != nil {
			resp.Errors = append(resp.Errors, &kvrpcpb.KeyError{
				Conflict: &kvrpcpb.WriteConflict{
					Key:        key,
					Primary:    req.PrimaryLock,
					StartTs:    req.StartVersion,
					ConflictTs: lock.Ts,
				},
			})
			return resp, nil
		}
		txn.PutLock(key, primaryLock)
		recentWrite, commitTs, err := txn.MostRecentWrite(key)
		if err != nil {
			resp.Errors = append(resp.Errors, &kvrpcpb.KeyError{
				Retryable: err.Error(),
			})
			return resp, nil
		}
		if recentWrite != nil && commitTs > req.StartVersion {
			resp.Errors = append(resp.Errors, &kvrpcpb.KeyError{
				Conflict: &kvrpcpb.WriteConflict{
					Key:        key,
					Primary:    req.PrimaryLock,
					StartTs:    req.StartVersion,
					ConflictTs: commitTs,
				},
			})
			return resp, nil
		}
		if req.Mutations[i].Op == kvrpcpb.Op_Del {
			txn.DeleteValue(key)
		} else if req.Mutations[i].Op == kvrpcpb.Op_Put {
			txn.PutValue(key, req.Mutations[i].Value)
		}
	}
	if err = server.storage.Write(req.Context, txn.Writes()); err != nil {
		resp.Errors = append(resp.Errors, &kvrpcpb.KeyError{
			Retryable: err.Error(),
		})
		return resp, nil
	}
	return resp, nil
}

func (server *Server) KvCommit(_ context.Context, req *kvrpcpb.CommitRequest) (*kvrpcpb.CommitResponse, error) {
	// Your Code Here (4B).
	if len(req.Keys) > 0 {
		server.Latches.WaitForLatches(req.Keys)
		defer server.Latches.ReleaseLatches(req.Keys)
	}
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		return &kvrpcpb.CommitResponse{
			Error: &kvrpcpb.KeyError{
				Retryable: err.Error(),
			},
		}, nil
	}
	txn := mvcc.NewMvccTxn(reader, req.StartVersion)
	for _, key := range req.Keys {
		lock, err := txn.GetLock(key)
		if err != nil {
			return &kvrpcpb.CommitResponse{
				Error: &kvrpcpb.KeyError{
					Retryable: err.Error(),
				},
			}, nil
		}
		if lock == nil {
			recentWrite, commitTs, err := txn.CurrentWrite(key)
			if err != nil {
				return &kvrpcpb.CommitResponse{
					Error: &kvrpcpb.KeyError{
						Retryable: err.Error(),
					},
				}, nil
			}
			if recentWrite != nil {
				if recentWrite.Kind == mvcc.WriteKindPut && commitTs == req.CommitVersion {
					return &kvrpcpb.CommitResponse{}, nil
				}
				return &kvrpcpb.CommitResponse{
					Error: &kvrpcpb.KeyError{
						Conflict: &kvrpcpb.WriteConflict{
							Key:        key,
							StartTs:    req.StartVersion,
							ConflictTs: commitTs,
						},
					},
				}, nil
			}
		}
		if lock != nil {
			if lock.Ts != req.StartVersion {
				return &kvrpcpb.CommitResponse{
					Error: &kvrpcpb.KeyError{
						Retryable: fmt.Errorf("prewrite by another transaction %d", lock.Ts).Error(),
					},
				}, nil
			}
			txn.DeleteLock(key)
			txn.PutWrite(key, req.CommitVersion, &mvcc.Write{
				StartTS: req.StartVersion,
				Kind:    mvcc.WriteKindPut,
			})
		}
	}
	if len(txn.Writes()) > 0 {
		if err = server.storage.Write(req.Context, txn.Writes()); err != nil {
			return &kvrpcpb.CommitResponse{
				Error: &kvrpcpb.KeyError{
					Retryable: err.Error(),
				},
			}, nil
		}
	}
	return &kvrpcpb.CommitResponse{}, nil
}

func (server *Server) KvScan(_ context.Context, req *kvrpcpb.ScanRequest) (*kvrpcpb.ScanResponse, error) {
	// Your Code Here (4C).
	return nil, nil
}

func (server *Server) KvCheckTxnStatus(_ context.Context, req *kvrpcpb.CheckTxnStatusRequest) (*kvrpcpb.CheckTxnStatusResponse, error) {
	// Your Code Here (4C).
	return nil, nil
}

func (server *Server) KvBatchRollback(_ context.Context, req *kvrpcpb.BatchRollbackRequest) (*kvrpcpb.BatchRollbackResponse, error) {
	// Your Code Here (4C).
	return nil, nil
}

func (server *Server) KvResolveLock(_ context.Context, req *kvrpcpb.ResolveLockRequest) (*kvrpcpb.ResolveLockResponse, error) {
	// Your Code Here (4C).
	return nil, nil
}

// SQL push down commands.
func (server *Server) Coprocessor(_ context.Context, req *coppb.Request) (*coppb.Response, error) {
	resp := new(coppb.Response)
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		if regionErr, ok := err.(*raft_storage.RegionError); ok {
			resp.RegionError = regionErr.RequestErr
			return resp, nil
		}
		return nil, err
	}
	switch req.Tp {
	case kv.ReqTypeDAG:
		return server.copHandler.HandleCopDAGRequest(reader, req), nil
	case kv.ReqTypeAnalyze:
		return server.copHandler.HandleCopAnalyzeRequest(reader, req), nil
	}
	return nil, nil
}
