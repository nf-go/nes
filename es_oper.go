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
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/elastic/go-elasticsearch/v7/esapi"
	"nfgo.ga/nfgo/nutil/ntemplate"
)

// ESOper -
type ESOper interface {
	ESAPI() esapi.API
	Count(ctx context.Context, query string, index ...string) (int64, error)
	CountTemplate(ctx context.Context, t *TemplateParam, index ...string) (int64, error)
	Search(ctx context.Context, model interface{}, query string, index ...string) (interface{}, error)
	SearchTemplate(ctx context.Context, model interface{}, t *TemplateParam, index ...string) (interface{}, error)
}

// NewESOper -
func NewESOper(api esapi.API) ESOper {
	return &esOperImpl{
		api: api,
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
	api esapi.API
}

func (e *esOperImpl) ESAPI() esapi.API {
	return e.api
}

func newRespErr(resp *esapi.Response) error {
	return fmt.Errorf("esapi's response status indicates failure: %s, %s", resp.Status(), resp.String())
}

func (e *esOperImpl) Count(ctx context.Context, query string, index ...string) (int64, error) {
	api := e.api
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

func (e *esOperImpl) Search(ctx context.Context, model interface{}, query string, index ...string) (interface{}, error) {
	api := e.api
	resp, err := api.Search(
		api.Search.WithContext(ctx),
		api.Search.WithIndex(index...),
		api.Search.WithBody(strings.NewReader(query)),
		api.Search.WithTrackTotalHits(true),
		api.Search.WithScroll(time.Minute*5),
	)
	if err != nil {
		return nil, err
	}

	defer resp.Body.Close()
	if resp.IsError() {
		return nil, newRespErr(resp)
	}

	if err := json.NewDecoder(resp.Body).Decode(model); err != nil {
		return nil, err
	}

	return model, nil
}

func (e *esOperImpl) SearchTemplate(ctx context.Context, model interface{}, t *TemplateParam, index ...string) (interface{}, error) {
	query, err := t.execute()
	if err != nil {
		return 0, err
	}
	return e.Search(ctx, model, query, index...)
}
