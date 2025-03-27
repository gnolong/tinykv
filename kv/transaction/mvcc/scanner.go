package mvcc

import (
	"bytes"

	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
)

// Scanner is used for reading multiple sequential key/value pairs from the storage layer. It is aware of the implementation
// of the storage layer and returns results suitable for users.
// Invariant: either the scanner is finished and cannot be used, or it is ready to return a value immediately.
type Scanner struct {
	// Your Data Here (4C).
	Txn    *MvccTxn
	curKey []byte
	iter   engine_util.DBIterator
}

// NewScanner creates a new scanner ready to read from the snapshot in txn.
func NewScanner(startKey []byte, txn *MvccTxn) *Scanner {
	// Your Code Here (4C).
	scanner := &Scanner{
		Txn:    txn,
		iter:   txn.Reader.IterCF(engine_util.CfWrite),
		curKey: []byte{},
	}
	scanner.iter.Seek(EncodeKey(startKey, txn.StartTS))
	return scanner
}

func (scan *Scanner) Close() {
	// Your Code Here (4C).
	if scan.iter != nil {
		scan.iter.Close()
	}
}

// Next returns the next key/value pair from the scanner. If the scanner is exhausted, then it will return `nil, nil, nil`.
func (scan *Scanner) Next() ([]byte, []byte, error) {
	// Your Code Here (4C).
	if !scan.iter.Valid() {
		return nil, nil, nil
	}
	item := scan.iter.Item()
	seekKey := item.Key()
	seekTs := decodeTimestamp(seekKey)
	userKey := DecodeUserKey(seekKey)
	if bytes.Equal(userKey, scan.curKey) {
		scan.iter.Seek(EncodeKey(scan.curKey, 0))
		return scan.Next()
	}
	if seekTs >= scan.Txn.StartTS {
		scan.iter.Next()
		return scan.Next()
	}
	val, err := item.Value()
	if err != nil {
		return nil, nil, &KeyError{
			KeyError: kvrpcpb.KeyError{
				Retryable: err.Error(),
			},
		}
	}
	write, err := ParseWrite(val)
	if err != nil {
		return nil, nil, &KeyError{
			KeyError: kvrpcpb.KeyError{
				Retryable: err.Error(),
			},
		}
	}
	if write.Kind == WriteKindDelete {
		scan.iter.Seek(EncodeKey(userKey, 0))
		return scan.Next()
	}
	value, err := scan.Txn.Reader.GetCF(engine_util.CfDefault, EncodeKey(userKey, write.StartTS))
	if err != nil {
		return nil, nil, &KeyError{
			KeyError: kvrpcpb.KeyError{
				Retryable: err.Error(),
			},
		}
	}
	scan.curKey = userKey
	scan.iter.Seek(EncodeKey(userKey, 0))
	return userKey, value, nil
}
