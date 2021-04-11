package blockstore

import (
	"context"

	blocks "github.com/ipfs/go-block-format"
	cid "github.com/ipfs/go-cid"
	blockstore "github.com/ipfs/go-ipfs-blockstore"
	"github.com/tikv/client-go/config"
	"github.com/tikv/client-go/rawkv"
)

// KeyScanSize defines how many keys fetch from TiKV cluster every round
const KeyScanSize = 1024

// TiKVStore wraps TiKV client and response for interacting with TiKV cluster
type TiKVStore struct {
	blockstore.Blockstore
	client *rawkv.Client
}

// NewTiKVStore returns a TiKVStore which conneted to a TiKV cluster
// if connect failed, an error will be returned
func NewTiKVStore(pdAddrs []string) (blockstore.Blockstore, error) {
	cli, err := rawkv.NewClient(context.TODO(), pdAddrs, config.Default())
	if err != nil {
		return nil, err
	}
	return &TiKVStore{nil, cli}, nil
}

// DeleteBlock implements blockstore.Blockstore
func (store *TiKVStore) DeleteBlock(cid cid.Cid) error {
	return store.client.Delete(context.TODO(), cid.Bytes())
}

// Has implements blockstore.Blockstore
func (store *TiKVStore) Has(cid cid.Cid) (bool, error) {
	ttl, err := store.client.GetKeyTTL(context.TODO(), cid.Bytes())
	if err != nil {
		return false, err
	}
	return ttl != nil, nil
}

// Get implements blockstore.Blockstore
func (store *TiKVStore) Get(cid cid.Cid) (blocks.Block, error) {
	data, err := store.client.Get(context.TODO(), cid.Bytes())
	if err != nil {
		return nil, err
	}
	if data == nil {
		return nil, blockstore.ErrNotFound
	}
	return blocks.NewBlockWithCid(data, cid)
}

// GetSize implements blockstore.Blockstore
func (store *TiKVStore) GetSize(cid cid.Cid) (int, error) {
	data, err := store.client.Get(context.TODO(), cid.Bytes())
	if err != nil {
		return -1, err
	}
	if data == nil {
		return -1, blockstore.ErrNotFound
	}
	return len(data), nil
}

// Put implements blockstore.Blockstore
func (store *TiKVStore) Put(block blocks.Block) error {
	return store.client.Put(context.TODO(), block.Cid().Bytes(), block.RawData())
}

// PutMany implements blockstore.Blockstore
func (store *TiKVStore) PutMany(blocks []blocks.Block) error {
	var keys, datas [][]byte
	for _, block := range blocks {
		keys = append(keys, block.Cid().Bytes())
		datas = append(datas, block.RawData())
	}
	return store.client.BatchPut(context.TODO(), keys, datas)
}

// AllKeysChan implements blockstore.Blockstore
func (store *TiKVStore) AllKeysChan(ctx context.Context) (<-chan cid.Cid, error) {
	ch := make(chan cid.Cid)

	go func() {
		startKey := []byte{}
		for {
			keys, _, err := store.client.Scan(ctx, append(startKey, byte(0)), nil, KeyScanSize, rawkv.ScanOption{KeyOnly: true})
			if err != nil || ctx.Err() != nil {
				return
			}
			for _, key := range keys {
				_, cid, err := cid.CidFromBytes(key)
				if err != nil {
					log.Errorf("covert cid from bytes failed, bytes: %x", key)
					continue
				}
				select {
				case ch <- cid:
				case <-ctx.Done():
					return
				}
			}
			if len(keys) < KeyScanSize {
				return
			}
			startKey = keys[KeyScanSize-1]
		}
	}()

	return ch, nil
}

// HashOnRead implements Blockstore.HashOnRead.
// It is not supported by this blockstore.
func (store *TiKVStore) HashOnRead(_ bool) {
	log.Warnf("called HashOnRead on TiKV blockstore; function not supported; ignoring")
}
