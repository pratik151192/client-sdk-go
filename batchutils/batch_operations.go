package batchutils

import (
	"context"
	"sync"

	"github.com/momentohq/client-sdk-go/momento"
	"github.com/momentohq/client-sdk-go/responses"
)

const maxConcurrentDeletes = 5
const maxConcurrentGets = 5

func keyDistributor(ctx context.Context, keys []momento.Key, keyChan chan momento.Key) {
	for _, k := range keys {
		keyChan <- k
	}

	for {
		select {
		case <-ctx.Done():
			return
		default:
			keyChan <- nil
		}
	}
}

type errKeyVal struct {
	key   momento.Value
	error error
}

type getKeyResp struct {
	key  momento.Value
	resp responses.GetResponse
}

// BatchDelete

func deleteWorker(
	ctx context.Context,
	client momento.CacheClient,
	cacheName string,
	keyChan chan momento.Key,
	errChan chan *errKeyVal,
) {
	for {
		myKey := <-keyChan
		if myKey == nil {
			return
		}
		_, err := client.Delete(ctx, &momento.DeleteRequest{
			CacheName: cacheName,
			Key:       myKey,
		})
		if err != nil {
			errChan <- &errKeyVal{
				key:   myKey,
				error: err,
			}
		} else {
			errChan <- nil
		}
	}
}

type BatchDeleteRequest struct {
	Client               momento.CacheClient
	CacheName            string
	Keys                 []momento.Key
	MaxConcurrentDeletes int
}

// BatchDeleteError contains a map associating failing cache keys with their specific errors.
// It may be necessary to use a type assertion to access the errors:
//
// errors := (err.(*BatchDeleteError)).Errors()
type BatchDeleteError struct {
	errors map[momento.Value]error
}

func (e *BatchDeleteError) Error() string {
	return "errors occurred during batch delete"
}

func (e *BatchDeleteError) Errors() map[momento.Value]error {
	return e.errors
}

// BatchDelete deletes a slice of keys from the cache, returning a map from failing cache keys to their specific errors.
func BatchDelete(ctx context.Context, props *BatchDeleteRequest) *BatchDeleteError {
	// initialize return value
	cancelCtx, cancelFunc := context.WithCancel(ctx)
	// stop the key distributor when we return
	defer cancelFunc()
	var wg sync.WaitGroup

	if props.MaxConcurrentDeletes == 0 {
		props.MaxConcurrentDeletes = maxConcurrentDeletes
	}
	if len(props.Keys) < props.MaxConcurrentDeletes {
		props.MaxConcurrentDeletes = len(props.Keys)
	}
	keyChan := make(chan momento.Key, props.MaxConcurrentDeletes)
	errChan := make(chan *errKeyVal, len(props.Keys))

	for i := 0; i < props.MaxConcurrentDeletes; i++ {
		wg.Add(1)

		go func() {
			defer wg.Done()
			deleteWorker(ctx, props.Client, props.CacheName, keyChan, errChan)
		}()
	}

	go keyDistributor(cancelCtx, props.Keys, keyChan)

	// wait for the workers to return
	wg.Wait()

	var errors = make(map[momento.Value]error, 0)
	for i := 0; i < len(props.Keys); i++ {
		msg := <-errChan
		if msg != nil {
			errors[msg.key] = msg.error
		}
	}

	if len(errors) == 0 {
		return nil
	}
	return &BatchDeleteError{errors: errors}
}

// BatchGet

func getWorker(
	ctx context.Context,
	client momento.CacheClient,
	cacheName string,
	keyChan chan momento.Key,
	errChan chan *errKeyVal,
	getChan chan *getKeyResp,
) {
	for {
		myKey := <-keyChan
		if myKey == nil {
			return
		}
		getResponse, err := client.Get(ctx, &momento.GetRequest{
			CacheName: cacheName,
			Key:       myKey,
		})
		if err != nil {
			getChan <- nil
			errChan <- &errKeyVal{
				key:   myKey,
				error: err,
			}
		} else {
			errChan <- nil
			getChan <- &getKeyResp{
				key:  myKey,
				resp: getResponse,
			}
		}
	}
}

type BatchGetRequest struct {
	Client            momento.CacheClient
	CacheName         string
	Keys              []momento.Key
	MaxConcurrentGets int
}

// BatchGetError contains a map associating failing cache keys with their specific errors.
// It may be necessary to use a type assertion to access the errors:
//
// errors := (err.(*BatchGetError)).Errors()
type BatchGetError struct {
	errors map[momento.Value]error
}

func (e *BatchGetError) Error() string {
	return "errors occurred during batch delete"
}

func (e *BatchGetError) Errors() map[momento.Value]error {
	return e.errors
}

// BatchGetResponse contains a map associating successful get responses with their cache keys.
type BatchGetResponse struct {
	responses map[momento.Value]responses.GetResponse
}

func (e *BatchGetResponse) Responses() map[momento.Value]responses.GetResponse {
	return e.responses
}

// BatchGet deletes a slice of keys from the cache, returning a map from failing cache keys to their specific errors.
func BatchGet(ctx context.Context, props *BatchGetRequest) (*BatchGetResponse, *BatchGetError) {
	// initialize return value
	cancelCtx, cancelFunc := context.WithCancel(ctx)
	// stop the key distributor when we return
	defer cancelFunc()
	var wg sync.WaitGroup

	if props.MaxConcurrentGets == 0 {
		props.MaxConcurrentGets = maxConcurrentGets
	}
	if len(props.Keys) < props.MaxConcurrentGets {
		props.MaxConcurrentGets = len(props.Keys)
	}
	keyChan := make(chan momento.Key, props.MaxConcurrentGets)
	errChan := make(chan *errKeyVal, len(props.Keys))
	getChan := make(chan *getKeyResp, len(props.Keys))

	for i := 0; i < props.MaxConcurrentGets; i++ {
		wg.Add(1)

		go func() {
			defer wg.Done()
			getWorker(ctx, props.Client, props.CacheName, keyChan, errChan, getChan)
		}()
	}

	go keyDistributor(cancelCtx, props.Keys, keyChan)

	// wait for the workers to return
	wg.Wait()

	var errors = make(map[momento.Value]error, 0)
	var results = make(map[momento.Value]responses.GetResponse, 0)
	for i := 0; i < len(props.Keys); i++ {
		res := <-getChan
		err := <-errChan
		if res != nil {
			results[res.key] = res.resp
		} else if err != nil {
			errors[err.key] = err.error
		}
	}

	var batchGetResponses *BatchGetResponse
	var batchGetErrors *BatchGetError

	if len(results) == 0 {
		batchGetResponses = nil
	} else {
		batchGetResponses = &BatchGetResponse{responses: results}
	}

	if len(errors) == 0 {
		batchGetErrors = nil
	} else {
		batchGetErrors = &BatchGetError{errors: errors}
	}
	return batchGetResponses, batchGetErrors
}
