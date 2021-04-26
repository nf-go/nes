// Copyright 2020 The nfgo Authors. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package nes

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"nfgo.ga/nfgo/nlog"
	"nfgo.ga/nfgo/nutil/ntemplate"
)

// ESOper -
type ESOper interface {
	ESClient() *Client

	Get(ctx context.Context, model interface{}, index string, docID string, opts ...func(*GetRequest)) (interface{}, error)

	MultiGet(ctx context.Context, model interface{}, index string, docIDs []string, opts ...func(*MgetRequest)) (interface{}, error)

	// Bulk allows to perform multiple index/update/delete operations in a single request.
	//
	// See full documentation at https://www.elastic.co/guide/en/elasticsearch/reference/master/docs-bulk.html.
	Bulk(ctx context.Context, index string, writeReqBody func(ctx context.Context, buf *bytes.Buffer) error, opts ...func(*BulkRequest)) error

	Index(ctx context.Context, index string, docID string, obj interface{}, opts ...func(*IndexRequest)) error

	Delete(ctx context.Context, docID string, index string, opts ...func(*DeleteRequest)) error
	DeleteByQuery(ctx context.Context, query string, indexes []string, opts ...func(*DeleteByQueryRequest)) error
	DeleteByQueryTemplate(ctx context.Context, t *TemplateParam, indexes []string, opts ...func(*DeleteByQueryRequest)) error

	UpdateByQuery(ctx context.Context, query string, indexes []string, opts ...func(*UpdateByQueryRequest)) error
	UpdateByQueryTemplate(ctx context.Context, t *TemplateParam, indexes []string, opts ...func(*UpdateByQueryRequest)) error

	Count(ctx context.Context, query string, indexes []string, opts ...func(*CountRequest)) (int64, error)
	CountTemplate(ctx context.Context, t *TemplateParam, indexes []string, opts ...func(*CountRequest)) (int64, error)
	Search(ctx context.Context, model interface{}, query string, indexes []string, opts ...func(*SearchRequest)) (interface{}, error)
	SearchTemplate(ctx context.Context, model interface{}, t *TemplateParam, indexes []string, opts ...func(*SearchRequest)) (interface{}, error)
	// Scroll allows to retrieve a large numbers of results from a single search request.
	//
	// We no longer recommend using the scroll API for deep pagination.
	// If you need to preserve the index state while paging through more than 10,000 hits, use the search_after parameter with a point in time (PIT).
	// See documentation at https://www.elastic.co/guide/en/elasticsearch/reference/master/paginate-search-results.html#scroll-search-results
	SearchByScrollID(ctx context.Context, model interface{}, scrollID string, opts ...func(*ScrollRequest)) (interface{}, error)
}

// NewESOper -
func NewESOper(client *Client) ESOper {
	return &esOperImpl{
		client: client,
	}
}

// TemplateParam -
type TemplateParam struct {
	Template *ntemplate.TextTemplate
	Data     interface{}
	Name     string
}

func (t *TemplateParam) execute() (string, error) {
	if t.Name == "" {
		return t.Template.Execute(t.Data)
	}
	return t.Template.ExecuteTemplate(t.Name, t.Data)
}

type mgetRequestBody struct {
	IDs []string `json:"ids"`
}

type esOperImpl struct {
	client *Client
}

func (e *esOperImpl) ESClient() *Client {
	return e.client
}

func (e *esOperImpl) Get(ctx context.Context, model interface{}, index string, docID string, opts ...func(*GetRequest)) (interface{}, error) {
	api := e.client
	o := append([]func(*GetRequest){api.Get.WithContext(ctx)}, opts...)
	resp, err := api.Get(index, docID, o...)
	if err != nil {
		return nil, err
	}
	if err := unmarshallResponse(resp, model); err != nil {
		return nil, err
	}
	return model, nil
}

func (e *esOperImpl) MultiGet(ctx context.Context, model interface{}, index string, docIDs []string, opts ...func(*MgetRequest)) (interface{}, error) {
	api := e.client
	o := append([]func(*MgetRequest){api.Mget.WithContext(ctx), api.Mget.WithIndex(index)}, opts...)
	buf := &bytes.Buffer{}
	err := json.NewEncoder(buf).Encode(&mgetRequestBody{IDs: docIDs})
	if err != nil {
		return nil, err
	}
	resp, err := api.Mget(buf, o...)
	if err != nil {
		return nil, err
	}
	if err := unmarshallResponse(resp, model); err != nil {
		return nil, err
	}
	return model, nil
}

func (e *esOperImpl) Bulk(ctx context.Context, index string, writeReqBody func(ctx context.Context, buf *bytes.Buffer) error, opts ...func(*BulkRequest)) error {
	api := e.client
	var buf bytes.Buffer
	if err := writeReqBody(ctx, &buf); err != nil {
		return err
	}
	o := append([]func(*BulkRequest){api.Bulk.WithIndex(index), api.Bulk.WithContext(ctx)}, opts...)
	resp, err := api.Bulk(&buf, o...)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.IsError() {
		return newRespErr(resp)
	}
	return nil
}

func (e *esOperImpl) Index(ctx context.Context, index string, docID string, obj interface{}, opts ...func(*IndexRequest)) error {
	body := &bytes.Buffer{}
	if err := json.NewEncoder(body).Encode(obj); err != nil {
		return err
	}

	api := e.client
	o := append([]func(*IndexRequest){api.API.Index.WithContext(ctx), api.API.Index.WithDocumentID(docID)}, opts...)
	resp, err := api.Index(index, body, o...)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.IsError() {
		return newRespErr(resp)
	}

	return nil
}

func (e *esOperImpl) Delete(ctx context.Context, docID string, index string, opts ...func(*DeleteRequest)) error {
	api := e.client
	o := append([]func(*DeleteRequest){api.Delete.WithContext(ctx)}, opts...)
	resp, err := api.Delete(index, docID, o...)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.IsError() {
		return newRespErr(resp)
	}
	return nil
}

func (e *esOperImpl) DeleteByQuery(ctx context.Context, query string, indexes []string, opts ...func(*DeleteByQueryRequest)) error {
	if nlog.IsLevelEnabled(nlog.DebugLevel) {
		nlog.Logger(ctx).Debugf("nes es oper DeleteByQuery: the delete query is %s", query)
	}
	api := e.client
	o := append([]func(*DeleteByQueryRequest){api.DeleteByQuery.WithContext(ctx)}, opts...)
	resp, err := api.DeleteByQuery(indexes, strings.NewReader(query), o...)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.IsError() {
		return newRespErr(resp)
	}
	return nil
}

func (e *esOperImpl) DeleteByQueryTemplate(ctx context.Context, t *TemplateParam, indexes []string, opts ...func(*DeleteByQueryRequest)) error {
	query, err := t.execute()
	if err != nil {
		return err
	}
	return e.DeleteByQuery(ctx, query, indexes, opts...)
}

func (e *esOperImpl) UpdateByQuery(ctx context.Context, query string, indexes []string, opts ...func(*UpdateByQueryRequest)) error {
	if nlog.IsLevelEnabled(nlog.DebugLevel) {
		nlog.Logger(ctx).Debugf("nes es oper UpdateByQuery: the update query is %s", query)
	}
	api := e.client
	o := append([]func(*UpdateByQueryRequest){api.UpdateByQuery.WithBody(strings.NewReader(query)), api.UpdateByQuery.WithContext(ctx)}, opts...)
	resp, err := api.UpdateByQuery(indexes, o...)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.IsError() {
		return newRespErr(resp)
	}
	return nil
}

func (e *esOperImpl) UpdateByQueryTemplate(ctx context.Context, t *TemplateParam, indexes []string, opts ...func(*UpdateByQueryRequest)) error {
	query, err := t.execute()
	if err != nil {
		return err
	}
	return e.UpdateByQuery(ctx, query, indexes, opts...)
}

func (e *esOperImpl) Count(ctx context.Context, query string, indexes []string, opts ...func(*CountRequest)) (int64, error) {
	if nlog.IsLevelEnabled(nlog.DebugLevel) {
		nlog.Logger(ctx).Debugf("nes es oper Count: the count query is %s", query)
	}
	api := e.client
	o := append([]func(*CountRequest){api.Count.WithContext(ctx), api.Count.WithIndex(indexes...), api.Count.WithBody(strings.NewReader(query))}, opts...)
	resp, err := api.Count(o...)
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()
	if resp.IsError() {
		return 0, newRespErr(resp)
	}

	var m map[string]interface{}
	if err := json.NewDecoder(resp.Body).Decode(&m); err != nil {
		return 0, err
	}
	return int64(m["count"].(float64)), nil
}

func (e *esOperImpl) CountTemplate(ctx context.Context, t *TemplateParam, indexes []string, opts ...func(*CountRequest)) (int64, error) {
	query, err := t.execute()
	if err != nil {
		return 0, err
	}
	return e.Count(ctx, query, indexes, opts...)
}

func (e *esOperImpl) Search(ctx context.Context, model interface{}, query string, indexes []string, opts ...func(*SearchRequest)) (interface{}, error) {
	if nlog.IsLevelEnabled(nlog.DebugLevel) {
		nlog.Logger(ctx).Debugf("nes es oper Search: the search query is %s", query)
	}
	api := e.client
	o := append([]func(*SearchRequest){api.Search.WithContext(ctx), api.Search.WithIndex(indexes...), api.Search.WithBody(strings.NewReader(query))}, opts...)
	resp, err := api.Search(o...)
	if err != nil {
		return nil, err
	}

	if err := unmarshallResponse(resp, model); err != nil {
		return nil, err
	}
	return model, nil
}

func (e *esOperImpl) SearchTemplate(ctx context.Context, model interface{}, t *TemplateParam, indexes []string, opts ...func(*SearchRequest)) (interface{}, error) {
	query, err := t.execute()
	if err != nil {
		return 0, err
	}
	return e.Search(ctx, model, query, indexes, opts...)
}

func (e *esOperImpl) SearchByScrollID(ctx context.Context, model interface{}, scrollID string, opts ...func(*ScrollRequest)) (interface{}, error) {
	api := e.client
	o := append([]func(*ScrollRequest){api.Scroll.WithContext(ctx), api.Scroll.WithScrollID(scrollID), api.Scroll.WithScroll(5 * time.Minute)}, opts...)
	resp, err := api.Scroll(o...)
	if err != nil {
		return nil, err
	}

	if err := unmarshallResponse(resp, model); err != nil {
		return nil, err
	}
	return model, nil
}

func unmarshallResponse(resp *Response, dest interface{}) error {
	defer resp.Body.Close()
	if resp.IsError() {
		return newRespErr(resp)
	}
	return json.NewDecoder(resp.Body).Decode(dest)
}

func newRespErr(resp *Response) error {
	return fmt.Errorf("esapi's response status indicates failure: %s, %s", resp.Status(), resp.String())
}
