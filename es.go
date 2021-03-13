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
	es "github.com/elastic/go-elasticsearch/v7"
	"github.com/elastic/go-elasticsearch/v7/esapi"
	"nfgo.ga/nfgo/nlog"
)

// Client represents the Elasticsearch client.
type Client = es.Client

type BulkRequest = esapi.BulkRequest

// IndexRequest -
type IndexRequest = esapi.IndexRequest

// DeleteRequest -
type DeleteRequest = esapi.DeleteRequest

// SearchRequest -
type SearchRequest = esapi.SearchRequest

// ScrollRequest -
type ScrollRequest = esapi.ScrollRequest

// CountRequest -
type CountRequest = esapi.CountRequest

// UpdateByQueryRequest -
type UpdateByQueryRequest = esapi.UpdateByQueryRequest

// DeleteByQueryRequest -
type DeleteByQueryRequest = esapi.DeleteByQueryRequest

// Response -
type Response = esapi.Response

// ESConfig -
type ESConfig struct {
	Addrs    []string `yaml:"addrs"`
	Username string   `yaml:"username"`
	Password string   `yaml:"password"`
}

// NewESClient -
func NewESClient(config *ESConfig) (*Client, error) {
	c := es.Config{
		Addresses: config.Addrs,
		Username:  config.Username,
		Password:  config.Password,
	}
	return es.NewClient(c)
}

// MustNewESClient -
func MustNewESClient(config *ESConfig) *Client {
	api, err := NewESClient(config)
	if err != nil {
		nlog.Fatal("fail to create esapi: ", err)
	}
	return api
}
