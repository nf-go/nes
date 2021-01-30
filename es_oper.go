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

	Index(ctx context.Context, index string, documentID string, obj interface{}) error

	Delete(ctx context.Context, documentID string, index string) error
	DeleteByQuery(ctx context.Context, query string, index ...string) error
	DeleteByQueryTemplate(ctx context.Context, t *TemplateParam, index ...string) error

	UpdateByQuery(ctx context.Context, query string, index ...string) error
	UpdateByQueryTemplate(ctx context.Context, t *TemplateParam, index ...string) error

	Count(ctx context.Context, query string, index ...string) (int64, error)
	CountTemplate(ctx context.Context, t *TemplateParam, index ...string) (int64, error)
	Search(ctx context.Context, model interface{}, query string, index string, opts ...func(*SearchRequest)) (interface{}, error)
	SearchTemplate(ctx context.Context, model interface{}, t *TemplateParam, index string, opts ...func(*SearchRequest)) (interface{}, error)
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

func (e *esOperImpl) Index(ctx context.Context, index string, documentID string, obj interface{}) error {
	body := &bytes.Buffer{}
	if err := json.NewEncoder(body).Encode(obj); err != nil {
		return err
	}

	req := IndexRequest{
		Index:      index,
		DocumentID: documentID,
		Body:       body,
	}

	resp, err := req.Do(ctx, e.client)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.IsError() {
		return newRespErr(resp)
	}

	return nil
}

func (e *esOperImpl) Delete(ctx context.Context, documentID string, index string) error {
	api := e.client
	resp, err := api.Delete(index, documentID, api.Delete.WithContext(ctx))
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.IsError() {
		return newRespErr(resp)
	}
	return nil
}

func (e *esOperImpl) DeleteByQuery(ctx context.Context, query string, index ...string) error {
	api := e.client
	resp, err := api.DeleteByQuery(index, strings.NewReader(query), api.DeleteByQuery.WithContext(ctx))
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.IsError() {
		return newRespErr(resp)
	}
	return nil
}

func (e *esOperImpl) DeleteByQueryTemplate(ctx context.Context, t *TemplateParam, index ...string) error {
	query, err := t.execute()
	if err != nil {
		return err
	}
	return e.DeleteByQuery(ctx, query, index...)
}

func (e *esOperImpl) UpdateByQuery(ctx context.Context, query string, index ...string) error {
	api := e.client
	resp, err := api.UpdateByQuery(index,
		api.UpdateByQuery.WithBody(strings.NewReader(query)),
		api.UpdateByQuery.WithContext(ctx),
	)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.IsError() {
		return newRespErr(resp)
	}
	return nil
}

func (e *esOperImpl) UpdateByQueryTemplate(ctx context.Context, t *TemplateParam, index ...string) error {
	query, err := t.execute()
	if err != nil {
		return err
	}
	return e.UpdateByQuery(ctx, query, index...)
}

func (e *esOperImpl) Count(ctx context.Context, query string, index ...string) (int64, error) {
	api := e.client
	resp, err := api.Count(
		api.Count.WithContext(ctx),
		api.Count.WithIndex(index...),
		api.Count.WithBody(strings.NewReader(query)),
	)
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

func (e *esOperImpl) CountTemplate(ctx context.Context, t *TemplateParam, index ...string) (int64, error) {
	query, err := t.execute()
	if err != nil {
		return 0, err
	}
	return e.Count(ctx, query, index...)
}

func (e *esOperImpl) Search(ctx context.Context, model interface{}, query string, index string, opts ...func(*SearchRequest)) (interface{}, error) {
	api := e.client
	o := append([]func(*SearchRequest){api.Search.WithContext(ctx),
		api.Search.WithIndex(index),
		api.Search.WithBody(strings.NewReader(query))}, opts...)
	resp, err := api.Search(o...)
	if err != nil {
		return nil, err
	}

	if err := unmarshallResponse(resp, model); err != nil {
		return nil, err
	}
	return model, nil
}

func (e *esOperImpl) SearchTemplate(ctx context.Context, model interface{}, t *TemplateParam, index string, opts ...func(*SearchRequest)) (interface{}, error) {
	query, err := t.execute()
	if err != nil {
		return 0, err
	}
	return e.Search(ctx, model, query, index, opts...)
}

func (e *esOperImpl) SearchByScrollID(ctx context.Context, model interface{}, scrollID string, index string, opts ...func(*ScrollRequest)) (interface{}, error) {
	api := e.client
	o := append([]func(*ScrollRequest){api.Scroll.WithContext(ctx),
		api.Scroll.WithScrollID(scrollID),
		api.Scroll.WithScroll(5 * time.Minute)}, opts...)
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
