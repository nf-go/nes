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

	"nfgo.ga/nfgo/nutil/ntemplate"
)

// ESOper -
type ESOper interface {
	ESClient() *Client

	Index(ctx context.Context, index string, documentID string, obj interface{}, opts ...func(*IndexRequest)) error

	Delete(ctx context.Context, documentID string, index string, opts ...func(*DeleteRequest)) error
	DeleteByQuery(ctx context.Context, query string, indexes []string, opts ...func(*DeleteByQueryRequest)) error
	DeleteByQueryTemplate(ctx context.Context, t *TemplateParam, indexes []string, opts ...func(*DeleteByQueryRequest)) error

	UpdateByQuery(ctx context.Context, query string, indexes []string, opts ...func(*UpdateByQueryRequest)) error
	UpdateByQueryTemplate(ctx context.Context, t *TemplateParam, indexes []string, opts ...func(*UpdateByQueryRequest)) error

	Count(ctx context.Context, query string, indexes []string, opts ...func(*CountRequest)) (int64, error)
	CountTemplate(ctx context.Context, t *TemplateParam, indexes []string, opts ...func(*CountRequest)) (int64, error)
	Search(ctx context.Context, model interface{}, query string, indexes []string, opts ...func(*SearchRequest)) (interface{}, error)
	SearchTemplate(ctx context.Context, model interface{}, t *TemplateParam, indexes []string, opts ...func(*SearchRequest)) (interface{}, error)
	SearchByScrollID(ctx context.Context, model interface{}, scrollID string, index string, opts ...func(*ScrollRequest)) (interface{}, error)
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

type esOperImpl struct {
	client *Client
}

func (e *esOperImpl) ESClient() *Client {
	return e.client
}

func (e *esOperImpl) Index(ctx context.Context, index string, documentID string, obj interface{}, opts ...func(*IndexRequest)) error {
	body := &bytes.Buffer{}
	if err := json.NewEncoder(body).Encode(obj); err != nil {
		return err
	}

	api := e.client
	o := append([]func(*IndexRequest){api.API.Index.WithContext(ctx), api.API.Index.WithDocumentID(documentID)}, opts...)
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

func (e *esOperImpl) Delete(ctx context.Context, documentID string, index string, opts ...func(*DeleteRequest)) error {
	api := e.client
	o := append([]func(*DeleteRequest){api.Delete.WithContext(ctx)}, opts...)
	resp, err := api.Delete(index, documentID, o...)
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

func (e *esOperImpl) SearchByScrollID(ctx context.Context, model interface{}, scrollID string, index string, opts ...func(*ScrollRequest)) (interface{}, error) {
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
